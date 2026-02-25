import hashlib
import logging
import os
from pathlib import Path
from typing import Optional

from tartape import ExcludeType, TarEntryFactory

from .constants import TAR_BLOCK_SIZE, TAR_FOOTER_SIZE
from .database import DatabaseSession
from .models import TapeMetadata, Track

logger = logging.getLogger(__name__)


class TapeRecorder:
    def __init__(
        self,
        root_path: str | Path,
        tape_db_path: str = ":memory:",
        anonymize: bool = True,
    ):
        self.root_path = Path(root_path).absolute()
        self.db_session = DatabaseSession(tape_db_path)
        self.db = self.db_session.start()
        self.anonymize = anonymize

        if not self.root_path.is_dir():
            raise ValueError(f"Root path {root_path} must be a directory.")

        self._buffer = []
        self._batch_size = 300

    def clear(self):
        with self.db.atomic():
            Track.delete().execute()
            TapeMetadata.delete().execute()

    def _calculate_fingerprint(self):
        sha = hashlib.sha256()
        for track in Track.select().order_by(Track.arc_path):
            entry_data = f"{track.arc_path}|{track.size}|{track.mtime}"
            sha.update(entry_data.encode())
        return sha.hexdigest()

    def save(self) -> str:
        self.flush()

        current_offset = 0

        with self.db.atomic():
            # Important for deterministic ordering
            tracks = Track.select().order_by(Track.arc_path)

            for track in tracks:
                track.start_offset = current_offset

                # Header 512
                header_size = TAR_BLOCK_SIZE

                # + Content (regular files only)
                content_size = (
                    track.size if not (track.is_dir or track.is_symlink) else 0
                )

                # + Padding 512
                padding = (
                    TAR_BLOCK_SIZE - (content_size % TAR_BLOCK_SIZE)
                ) % TAR_BLOCK_SIZE

                total_entry_size = header_size + content_size + padding
                current_offset += total_entry_size

                track.end_offset = current_offset
                track.save()

            total_tape_size = current_offset + TAR_FOOTER_SIZE
            fingerprint = self._calculate_fingerprint()

            TapeMetadata.insert(
                key="fingerprint", value=fingerprint
            ).on_conflict_replace().execute()
            TapeMetadata.insert(
                key="total_size", value=str(total_tape_size)
            ).on_conflict_replace().execute()

        return fingerprint

    def close(self):
        self.db_session.close()

    def add_file(self, source_path: Path, arcname: str):
        source_path = source_path.absolute()

        # Calculate the relative path with respect to the root folder
        try:
            rel_path = str(source_path.relative_to(self.root_path))
        except ValueError:
            # The added file is not inside the root folder
            rel_path = source_path.name

        track = TarEntryFactory.create_track(
            source_path, arcname=arcname, rel_path=rel_path, anonymize=self.anonymize
        )

        if track:
            self._buffer.append(track)

            if len(self._buffer) >= self._batch_size:
                self.flush()

    def _should_exclude(self, path: Path, exclude: Optional[ExcludeType]) -> bool:
        """Determines if a path should be skipped based on the 'exclude' parameter."""
        if exclude is None:
            return False

        # Function or Lambda
        if callable(exclude):
            return exclude(path)

        # String unique - glob patterns
        if isinstance(exclude, str):
            return path.match(exclude) or path.name == exclude

        # List strings
        if isinstance(exclude, list):
            return any(path.match(p) or path.name == p for p in exclude)

        return False

    def add_folder(
        self,
        folder_path: str | Path,
        arcname: str = "",
        recursive: bool = True,
        exclude: Optional[ExcludeType] = None,
    ):
        root_path = Path(folder_path).absolute()
        if not root_path.is_dir():
            raise ValueError(f"Path '{folder_path}' is not a directory.")

        # Prefix for TAR
        prefix = arcname or root_path.name

        if not self._should_exclude(root_path, exclude):
            self.add_file(root_path, arcname=prefix)
            self._scan_and_add(root_path, prefix, root_path, recursive, exclude)

        self.flush()

    def _scan_and_add(
        self,
        current_path: Path,
        arc_prefix: str,
        base_root: Path,
        recursive: bool,
        exclude: Optional[ExcludeType],
    ):
        try:
            with os.scandir(current_path) as it:
                for entry in it:
                    entry_path = Path(entry.path)
                    if self._should_exclude(entry_path, exclude):
                        continue

                    # arc_path: What will be seen in the TAR (ej: backup/fotos/vacas.jpg)
                    entry_arcname = f"{arc_prefix}/{entry.name}"

                    self.add_file(entry_path, arcname=entry_arcname)

                    if recursive and entry.is_dir() and not entry.is_symlink():
                        self._scan_and_add(
                            entry_path, entry_arcname, base_root, recursive, exclude
                        )
        except PermissionError:
            logger.warning(f"Permission denied: {current_path}")

    def flush(self):
        if not self._buffer:
            return

        with self.db.atomic():
            data = [t.__data__ for t in self._buffer]
            Track.insert_many(data).on_conflict_replace().execute()

        self._buffer = []
