import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Iterable, Optional, cast

from tartape.constants import TAPE_DB_NAME, TAPE_METADATA_DIR
from tartape.database import DatabaseSession
from tartape.factory import ExcludeType, TarEntryFactory
from tartape.schemas import EntryMetadata

from .constants import DEFAULT_EXCLUDES, TAR_FOOTER_SIZE
from .models import TapeMetadata, Track

logger = logging.getLogger(__name__)


class TapeRecorder:
    def __init__(
        self,
        directory: str | Path,
        exclude: Optional[ExcludeType] = None,
        anonymize: bool = True,
        calculate_hashes: bool = False,
    ):
        self.directory = Path(directory).resolve()
        self.calculate_hashes = calculate_hashes
        if not self.directory.is_dir():
            raise ValueError(f"Root path '{directory}' must be a directory.")

        self.exclude = DEFAULT_EXCLUDES if exclude is None else exclude
        self.anonymize = anonymize
        self.tape_db_path = self.directory / TAPE_METADATA_DIR / TAPE_DB_NAME

        if self.tape_db_path.exists():
            raise FileExistsError(f"Catalog already exists at: {self.tape_db_path}")

        self._temp_dir = tempfile.TemporaryDirectory()
        self._temp_path = Path(self._temp_dir.name) / TAPE_DB_NAME
        self.temp_session = DatabaseSession(self._temp_path)
        self.db = self.temp_session.connect()

        self._buffer = []
        self._batch_size = 300

    def _calculate_fingerprint(self):
        """Generates the identity hash based on the contents of the database."""
        sha = hashlib.sha256()
        for track in Track.select().order_by(Track.arc_path).iterator():
            sha.update(f"{track.arc_path}|{track.size}|{track.mtime}".encode())
        return sha.hexdigest()

    def _finalize_storage(self):
        dest_dir = self.directory / TAPE_METADATA_DIR
        dest_dir.mkdir(exist_ok=True)
        shutil.move(str(self._temp_path), str(self.tape_db_path))

        logger.info(f"Catalog successfully recorded on: {self.tape_db_path}")

    def commit(self) -> str:
        """
        Freezes the tape state.
        Calculates the Global Window (start_offset, end_offset) for every track.

        Returns the signature (fingerprint).
        """

        try:
            self._run_discovery()
            self._flush_buffer()

            with self.db.atomic():
                # ADR-001: Important for deterministic ordering
                tracks = cast(
                    Iterable[Track], Track.select().order_by(Track.arc_path).iterator()
                )
                current_global_offset = 0
                batch = []

                for track in tracks:
                    # CALCULATE GLOBAL WINDOW
                    track.start_offset = current_global_offset

                    # Advance the cursor by the full block size (Header + Content + Padding)
                    current_global_offset += track.total_block_size

                    track.end_offset = current_global_offset
                    batch.append(track)

                    if len(batch) == self._batch_size:
                        Track.bulk_update(
                            batch, fields=[Track.start_offset, Track.end_offset]
                        )
                        batch = []
                if batch:
                    Track.bulk_update(
                        batch, fields=[Track.start_offset, Track.end_offset]
                    )
                    batch = []

                if callable(self.exclude):
                    func_name = getattr(self.exclude, "__name__", "custom_filter")
                    exclude_val = f"<dynamic_callable: {func_name}>"
                else:
                    exclude_val = json.dumps(self.exclude)

                total_size = int(current_global_offset + TAR_FOOTER_SIZE)
                fingerprint = self._calculate_fingerprint()
                capture_time = str(int(time.time()))

                TapeMetadata.insert(key="fingerprint", value=fingerprint).execute()
                TapeMetadata.insert(key="total_size", value=total_size).execute()
                TapeMetadata.insert(key="created_at", value=capture_time).execute()
                TapeMetadata.insert(key="exclude_patterns", value=exclude_val).execute()

            self.temp_session.close()
            self._finalize_storage()
            return fingerprint

        finally:
            if hasattr(self, "temp_session"):
                self.temp_session.close()
            self._temp_dir.cleanup()

    def _run_discovery(self):
        """Scans the filesystem in a deterministic manner."""

        self._add_to_buffer(self.directory, arcname=self.directory.name)

        # sorted(os.listdir) to guarantee order before the database
        stack = [(self.directory, self.directory.name)]

        while stack:
            curr_dir, arc_prefix = stack.pop()
            try:
                entries = sorted(os.listdir(curr_dir))
                for name in entries:
                    full_path = curr_dir / name
                    if self._should_exclude(full_path):
                        continue

                    arc_name = f"{arc_prefix}/{name}"
                    self._add_to_buffer(full_path, arcname=arc_name)

                    if full_path.is_dir() and not full_path.is_symlink():
                        stack.append((full_path, arc_name))
            except PermissionError:
                logger.warning(f"Permission denied: {curr_dir}")

    def _add_to_buffer(self, source_path: Path, arcname: str):
        """Parses a file and adds it to the insert buffer."""

        rel_path = source_path.relative_to(self.directory).as_posix()
        if rel_path == ".":
            rel_path = ""

        metadata: Optional[EntryMetadata] = TarEntryFactory.create_metadata(
            source_path,
            arcname=arcname,
            rel_path=rel_path,
            anonymize=self.anonymize,
            calculate_hash=self.calculate_hashes,
        )

        if metadata:
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
                md5sum=metadata.md5sum,
            )

            self._buffer.append(track)

            if len(self._buffer) >= self._batch_size:
                self._flush_buffer()

    def _should_exclude(self, path: Path) -> bool:
        """Determines if a path should be skipped based on the 'self.exclude'."""

        if TAPE_METADATA_DIR in path.parts:
            return True
        if self.exclude is None:
            return False
        if callable(self.exclude):
            return self.exclude(path)
        if isinstance(self.exclude, str):
            return path.match(self.exclude) or path.name == self.exclude
        if isinstance(self.exclude, list):
            return any(path.match(p) or path.name == p for p in self.exclude)
        return False

    def _flush_buffer(self):
        """Write the buffer to the database."""
        if not self._buffer:
            return

        with self.db.atomic():
            data = [t.__data__ for t in self._buffer]
            Track.insert_many(data).on_conflict_replace().execute()

        self._buffer = []

    def close(self):
        self.temp_session.close()
