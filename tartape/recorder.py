import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Iterable, Optional, cast

from tartape.cache import HashCacheManager
from tartape.constants import (
    DEFAULT_EXCLUDES,
    SUPPORTED_CHECKSUM_ALGORITHMS,
    TAPE_EXTENSION,
    TAR_FOOTER_SIZE,
)
from tartape.database import DatabaseSession, seal_database
from tartape.exceptions import PathConstraintError, PathConstraintReportError
from tartape.factory import ExcludeType, TarEntryFactory, should_exclude
from tartape.models import TapeMetadata, Track
from tartape.schemas import ChecksumAlgorithm, ChecksumOption, EntryMetadata

logger = logging.getLogger(__name__)


class TapeRecorder:
    """Scans a source directory and records an immutable T0 catalog snapshot.

    Builds the database in a temporary directory and seals it into a standalone
    .tartape sidecar file without modifying the source directory.
    """

    def __init__(
        self,
        directory: str | Path,
        catalog_path: Optional[str | Path] = None,
        exclude: Optional[ExcludeType] = None,
        anonymize: bool = True,
        checksum: ChecksumOption = False,
        overwrite: bool = False,
        auto_truncate: bool = False,
    ):
        self.directory = Path(directory).resolve()
        self.auto_truncate = auto_truncate
        self.overwrite = overwrite
        self.anonymize = anonymize

        if not self.directory.is_dir():
            raise ValueError(f"Root path '{directory}' must be a directory.")

        # Resolve destination catalog path (sidecar by default)
        if catalog_path is not None:
            self.catalog_path = Path(catalog_path).resolve()
        else:
            self.catalog_path = (
                self.directory.parent / f"{self.directory.name}{TAPE_EXTENSION}"
            )

        if self.catalog_path.exists() and not self.overwrite:
            raise FileExistsError(
                f"Catalog already exists at: {self.catalog_path}. "
                "Use overwrite=True to replace it."
            )

        self.exclude = DEFAULT_EXCLUDES if exclude is None else exclude

        # Normalize and validate unified checksum parameter
        if isinstance(checksum, bool):
            self.checksum_algorithm: Optional[ChecksumAlgorithm] = (
                "sha256" if checksum else None
            )
        elif isinstance(checksum, str):
            algo = checksum.lower().strip()
            if algo not in SUPPORTED_CHECKSUM_ALGORITHMS:
                valid_options = ", ".join(
                    f"'{a}'" for a in SUPPORTED_CHECKSUM_ALGORITHMS
                )
                raise ValueError(
                    f"Unsupported checksum algorithm '{checksum}'. "
                    f"Supported algorithms: {valid_options}."
                )
            self.checksum_algorithm = cast(ChecksumAlgorithm, algo)
        else:
            self.checksum_algorithm = None

        self.cache: Optional[HashCacheManager] = None
        if self.checksum_algorithm:
            logger.info(
                f"Pre-computing file checksums using '{self.checksum_algorithm}' during recording."
            )
            self.cache = HashCacheManager(self.directory)

        # Setup working database in isolated temporary directory
        self._temp_dir = tempfile.TemporaryDirectory()
        self._temp_path = Path(self._temp_dir.name) / "temp_index.db"
        self.temp_session = DatabaseSession(self._temp_path)
        self.db = self.temp_session.connect()

        self._buffer: list[Track] = []
        self._batch_size = 300
        self._accumulated_data_size: int = 0

    def _calculate_fingerprint(self) -> str:
        """Generates the identity hash based on the contents of the database."""
        sha = hashlib.sha256()
        for track in Track.select().order_by(Track.arc_path).iterator():
            sha.update(f"{track.arc_path}|{track.size}|{track.mtime}".encode())
        return sha.hexdigest()

    def _finalize_storage(self) -> None:
        """Seals the temporary SQLite file and moves it atomically to the final destination."""
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)

        if self.catalog_path.exists() and self.overwrite:
            if self.catalog_path.is_dir():
                shutil.rmtree(self.catalog_path)
            else:
                self.catalog_path.unlink()

        shutil.move(str(self._temp_path), str(self.catalog_path))
        logger.info(f"Catalog successfully recorded at: {self.catalog_path}")

    def commit(self) -> str:
        """Freezes the tape state.

        Calculates global stream offsets for each track, stores metadata,
        seals the database, and deploys the sidecar file.

        Returns:
            str: The digital fingerprint (SHA-256) of the tape.
        """
        try:
            self._run_discovery()
            self._flush_buffer()

            with self.db.atomic():
                # ADR-001: Deterministic ordering by archive path
                tracks = cast(
                    Iterable[Track], Track.select().order_by(Track.arc_path).iterator()
                )
                current_global_offset = 0
                batch: list[Track] = []

                for track in tracks:
                    track.start_offset = current_global_offset
                    current_global_offset += track.total_block_size
                    track.end_offset = current_global_offset
                    batch.append(track)

                    if len(batch) >= self._batch_size:
                        Track.bulk_update(
                            batch, fields=[Track.start_offset, Track.end_offset]
                        )
                        batch = []

                if batch:
                    Track.bulk_update(
                        batch, fields=[Track.start_offset, Track.end_offset]
                    )

                if callable(self.exclude):
                    func_name = getattr(self.exclude, "__name__", "custom_filter")
                    exclude_val = f"<dynamic_callable: {func_name}>"
                else:
                    exclude_val = json.dumps(self.exclude)

                total_size = int(current_global_offset + TAR_FOOTER_SIZE)
                fingerprint = self._calculate_fingerprint()
                capture_time = str(int(time.time()))

                # Core Metadata
                TapeMetadata.insert(key="fingerprint", value=fingerprint).execute()
                TapeMetadata.insert(key="total_size", value=total_size).execute()
                TapeMetadata.insert(key="created_at", value=capture_time).execute()
                TapeMetadata.insert(key="exclude_patterns", value=exclude_val).execute()

                # Rich Inspection Metadata
                TapeMetadata.insert(
                    key="checksum_algorithm",
                    value=self.checksum_algorithm or "none",
                ).execute()
                TapeMetadata.insert(
                    key="has_file_checksums",
                    value="true" if self.checksum_algorithm else "false",
                ).execute()
                TapeMetadata.insert(
                    key="data_size",
                    value=str(self._accumulated_data_size),
                ).execute()
                TapeMetadata.insert(
                    key="auto_truncated",
                    value="true" if self.auto_truncate else "false",
                ).execute()
                TapeMetadata.insert(
                    key="is_anonymized",
                    value="true" if self.anonymize else "false",
                ).execute()

            # Close active Peewee session before sealing the database file
            self.temp_session.close()

            # Seal the database to eliminate lingering artifacts
            seal_database(self._temp_path)

            # Move sealed database to the catalog destination
            self._finalize_storage()
            return fingerprint

        finally:
            if hasattr(self, "temp_session"):
                self.temp_session.close()
            if self.cache:
                self.cache.close()
            self._temp_dir.cleanup()

    def _run_discovery(self) -> None:
        """Scans the filesystem in a deterministic alphabetical order."""
        path_violations = []

        try:
            safe_root_name = TarEntryFactory.resolve_arcname(
                self.directory.name, auto_truncate=self.auto_truncate, is_dir=True
            )
        except PathConstraintError as e:
            path_violations.append((str(self.directory), str(e)))
            safe_root_name = self.directory.name

        if not path_violations:
            self._add_to_buffer(self.directory, arcname=safe_root_name)
            stack = [(self.directory, safe_root_name)]
        else:
            stack = []

        while stack:
            curr_dir, arc_prefix = stack.pop()
            try:
                with os.scandir(curr_dir) as it:
                    entries = sorted(it, key=lambda e: e.name)
                    for entry in entries:
                        full_path = Path(entry.path)

                        if self._should_exclude(full_path):
                            continue

                        raw_arc_name = f"{arc_prefix}/{entry.name}"
                        is_directory = entry.is_dir(follow_symlinks=False)

                        try:
                            safe_arc_name = TarEntryFactory.resolve_arcname(
                                raw_arc_name,
                                auto_truncate=self.auto_truncate,
                                is_dir=is_directory,
                            )
                        except PathConstraintError as e:
                            path_violations.append((str(full_path), str(e)))
                            continue

                        cached_stat = entry.stat(follow_symlinks=False)

                        self._add_to_buffer(
                            full_path,
                            arcname=safe_arc_name,
                            precomputed_stat=cached_stat,
                        )

                        if is_directory:
                            stack.append((full_path, safe_arc_name))

            except PermissionError:
                logger.warning(f"Permission denied: {curr_dir}")

        if path_violations:
            display_violations = [
                f"{p} -> {reason}" for p, reason in path_violations[:50]
            ]
            report = "\n  - ".join(display_violations)
            if len(path_violations) > 50:
                report += f"\n  ... and {len(path_violations) - 50} more."

            advice = (
                "Note: 'auto_truncate=True' is enabled, but some paths could not be automatically resolved."
                if self.auto_truncate
                else "To automatically shorten these paths and prevent collisions, use 'auto_truncate=True' in record()."
            )

            raise PathConstraintReportError(
                f"Discovery aborted. {len(path_violations)} path(s) violate USTAR limitations:\n"
                f"  - {report}\n\n"
                f"{advice}"
            )

    def _add_to_buffer(
        self,
        source_path: Path,
        arcname: str,
        precomputed_stat: Optional[os.stat_result] = None,
    ) -> None:
        """Parses an entry and appends it to the bulk insert buffer."""
        rel_path = source_path.relative_to(self.directory).as_posix()
        if rel_path == ".":
            rel_path = ""

        metadata: Optional[EntryMetadata] = TarEntryFactory.create_metadata(
            source_path,
            arcname=arcname,
            rel_path=rel_path,
            anonymize=self.anonymize,
            checksum_algorithm=self.checksum_algorithm,
            precomputed_stat=precomputed_stat,
            cache_manager=self.cache,
        )

        if metadata:
            if metadata.has_content:
                self._accumulated_data_size += metadata.size

            track = Track(
                arc_path=metadata.arc_path,
                rel_path=metadata.rel_path,
                size=metadata.size,
                mtime=metadata.mtime,
                mode=metadata.mode,
                uid=metadata.uid,
                gid=metadata.gid,
                uname=metadata.uname,
                gname=metadata.gname,
                is_dir=metadata.is_dir,
                is_symlink=metadata.is_symlink,
                linkname=metadata.linkname,
                checksum=metadata.checksum,
            )

            self._buffer.append(track)
            if len(self._buffer) >= self._batch_size:
                self._flush_buffer()

    def _should_exclude(self, path: Path) -> bool:
        """Determines if a path should be skipped."""
        return should_exclude(path, self.exclude)

    def _flush_buffer(self) -> None:
        """Writes buffered tracks to the database."""
        if not self._buffer:
            return

        with self.db.atomic():
            data = [t.__data__ for t in self._buffer]
            Track.insert_many(data).on_conflict_replace().execute()

        self._buffer = []

    def close(self) -> None:
        self.temp_session.close()
        if self.cache:
            self.cache.close()
