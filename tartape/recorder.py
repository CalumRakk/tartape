import hashlib
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional

from tartape.database import DatabaseSession
from tartape.factory import ExcludeType, TarEntryFactory

from .constants import TAR_BLOCK_SIZE, TAR_FOOTER_SIZE
from .models import TapeMetadata, Track

logger = logging.getLogger(__name__)


class TapeRecorder:
    def __init__(
        self,
        root_path: str | Path,
        tartape_path: Optional[Path] = None,
        exclude: Optional[ExcludeType] = None,
        anonymize: bool = True,
    ):
        self.root_path = Path(root_path).absolute()
        if not self.root_path.is_dir():
            raise ValueError(f"La ruta raíz '{root_path}' debe ser un directorio.")

        self.exclude = exclude
        self.anonymize = anonymize
        self.tape_path = (
            tartape_path or self.root_path.parent / f"{self.root_path.name}.tartape"
        )

        if self.tape_path.exists():
            raise FileExistsError(f"Ya existe una cinta en: {self.tape_path}")

        self._temp_dir = tempfile.TemporaryDirectory()
        self._temp_path = Path(self._temp_dir.name) / self.tape_path.name
        self.db_session = DatabaseSession(self._temp_path)
        self.db = self.db_session.start()

        self._buffer = []
        self._batch_size = 300

    def _calculate_fingerprint(self):
        """Generates the identity hash based on the contents of the database."""
        sha = hashlib.sha256()
        for track in Track.select().order_by(Track.arc_path):
            entry_data = f"{track.arc_path}|{track.size}|{track.mtime}"
            sha.update(entry_data.encode())
        return sha.hexdigest()

    def _finalize_tape(self):
        self.db_session.close()
        shutil.move(str(self._temp_path), str(self.tape_path))
        self._temp_dir.cleanup()
        logger.info(f"Tape successfully recorded on: {self.tape_path}")

    def commit(self) -> str:
        """
        Calculates offsets, generates signature and saves metadata.
        Returns the signature (fingerprint).
        """
        self._scan_root()

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
                track.save()  # TODO: move to a buffer.

            total_tape_size = current_offset + TAR_FOOTER_SIZE
            fingerprint = self._calculate_fingerprint()

            TapeMetadata.insert(
                key="fingerprint", value=fingerprint
            ).on_conflict_replace().execute()
            TapeMetadata.insert(
                key="total_size", value=str(total_tape_size)
            ).on_conflict_replace().execute()

        self._finalize_tape()
        return fingerprint

    def _scan_root(self):
        prefix = self.root_path.name
        self._add_to_buffer(self.root_path, arcname=prefix)

        self._recursive_scan(self.root_path, prefix)

    def _recursive_scan(self, current_path: Path, arc_prefix: str):
        try:
            with os.scandir(current_path) as it:
                for entry in it:
                    entry_path = Path(entry.path)
                    if self._should_exclude(entry_path):
                        continue

                    entry_arcname = f"{arc_prefix}/{entry.name}"
                    self._add_to_buffer(entry_path, arcname=entry_arcname)

                    if entry.is_dir() and not entry.is_symlink():
                        self._recursive_scan(entry_path, entry_arcname)
        except PermissionError:
            logger.warning(f"Permission denied: {current_path}")

    def _add_to_buffer(self, source_path: Path, arcname: str):
        """Parses a file and adds it to the insert buffer."""

        rel_path = str(source_path.relative_to(self.root_path))
        if rel_path == ".":
            rel_path = ""

        track = TarEntryFactory.create_track(
            source_path, arcname=arcname, rel_path=rel_path, anonymize=self.anonymize
        )

        if track:
            # We establish the root so that the factory can work with relative paths
            track._source_root = self.root_path
            self._buffer.append(track)

            if len(self._buffer) >= self._batch_size:
                self.flush()

    def _should_exclude(self, path: Path) -> bool:
        """Determines if a path should be skipped based on the 'self.exclude'."""
        if self.exclude is None:
            return False
        if callable(self.exclude):
            return self.exclude(path)
        if isinstance(self.exclude, str):
            return path.match(self.exclude) or path.name == self.exclude
        if isinstance(self.exclude, list):
            return any(path.match(p) or path.name == p for p in self.exclude)
        return False

    def flush(self):
        """Write the buffer to the database."""
        if not self._buffer:
            return

        with self.db.atomic():
            data = [t.__data__ for t in self._buffer]
            Track.insert_many(data).on_conflict_replace().execute()

        self._buffer = []

    def close(self):
        self.db_session.close()
