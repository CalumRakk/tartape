import json
import logging
import shutil
from pathlib import Path
from typing import Generator, List, Optional, Union

import peewee

import tartape
from tartape.catalog import Catalog
from tartape.chunker import TarChunker
from tartape.constants import TAPE_METADATA_DIR
from tartape.factory import validate_integrity, validate_root_structure_integrity
from tartape.models import Track
from tartape.schemas import ByteWindow, ManifestEntry
from tartape.stream import FolderVolume, TapeVolume, TarStreamGenerator

logger = logging.getLogger(__name__)


class Tape:
    """
    The Master Class. It represents a complete data tape.
    It is the engine that orchestrates the Catalog, the Player, and the Chunker.
    """

    def __init__(self, directory: Union[str, Path]):
        self.directory = Path(directory).resolve()
        self._stats = {}
        self._refresh_metadata()

    def _refresh_metadata(self):
        with Catalog.from_directory(self.directory) as cat:
            self._stats = cat.get_stats()
            self._track_count = cat.get_track_count()

    @property
    def count_files(self) -> int:
        """Returns the total number of files in the tape."""
        return self._stats["total_size"]

    @property
    def fingerprint(self) -> str:
        """Returns the digital signature of the tape."""
        return self._stats["fingerprint"]

    @property
    def total_size(self) -> int:
        """Returns the total size that the TAR stream will have (bytes)."""
        return self._stats["total_size"]

    @property
    def created_at(self) -> int:
        return self._stats["created_at"]

    @property
    def exclude_patterns(self) -> List[str] | str:
        value = self._stats["exclude_patterns"]
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    def get_tracks(self):
        """Returns all tracks sorted for the stream.

        This method must be called within a 'with catalog'
        """
        with tartape.get_catalog(self.directory):
            yield from Track.select().order_by(Track.arc_path).iterator()

    def destroy(self):
        if self._catalog:
            self._catalog.close()
            self._catalog = None

        metadata_dir = self.directory / TAPE_METADATA_DIR
        if metadata_dir.exists():
            shutil.rmtree(metadata_dir)

    def verify(self, deep: bool = False, raise_exception: bool = False):
        """Check the integrity of the tape."""
        try:
            with tartape.get_catalog(self.directory):
                validate_root_structure_integrity(self.directory)

                if deep:
                    for track in Track.select().order_by(Track.arc_path).iterator():
                        validate_integrity(track, self.directory)
                else:
                    total = Track.select().count()
                    if total > 0:
                        samples = Track.select().order_by(peewee.fn.Random()).limit(15)
                        for track in samples:
                            validate_integrity(track, self.directory)
                return True
        except Exception:
            if raise_exception:
                raise
            return False

    def _verify_resume_point_integrity(self, catalog: Catalog, absolute_offset: int):
        """
        Resuming a stream is a critical operation. We find the track at the
        exact failure point and verify it hasn't mutated on disk.

        This method must be called within a 'with catalog'
        """

        if absolute_offset < 0 or absolute_offset >= self.total_size:
            raise ValueError(f"Invalid resume offset: {absolute_offset}")

        # The 'Footer Zone' (last 1024 bytes) has no files, it's just padding.
        if absolute_offset >= self.total_size - 1024:
            return

        track = catalog.find_track_at_absolute_offset(absolute_offset)
        full_tape_window = ByteWindow(0, self.total_size)
        entry = ManifestEntry.from_track(track, full_tape_window)
        validate_integrity(entry.info, self.directory)

    def iter_volumes(self, size: int, naming_template: Optional[str] = None):
        """It breaks the tape down into logical and physical volumes."""
        chunker = TarChunker(chunk_size=size)
        yield from chunker.iter_volumes(
            directory=self.directory, naming_template=naming_template
        )

    def play(
        self,
        start_offset: int = 0,
        chunk_size: int = 64 * 1024,
        fast_verify: bool = True,
    ) -> Generator:
        self.verify(deep=not fast_verify, raise_exception=True)

        tape_window = ByteWindow(start=0, end=self.total_size)
        with tartape.get_catalog(self.directory) as cat:
            if start_offset > 0:
                self._verify_resume_point_integrity(cat, start_offset)

            tracks = cat.query_tracks_intersecting_range(start_offset)

            def track_loader():
                for track in tracks:
                    yield ManifestEntry.from_track(track, tape_window)

            engine = TarStreamGenerator(track_loader(), self.directory)
            yield from engine.stream(start_offset=start_offset, chunk_size=chunk_size)

    def get_volume(
        self, vol_name: str, vol_index: int, vol_start: int, vol_end: int
    ) -> TapeVolume:
        if not tartape.exists(self.directory):
            raise FileNotFoundError(f"The tape does not exist in: {self.directory}")

        volume_window = ByteWindow(start=vol_start, end=vol_end)
        if vol_start < 0 or vol_end > self.total_size or vol_start >= vol_end:
            raise ValueError(
                f"Invalid range: {vol_start}-{vol_end}. Total tape size is {self.total_size}"
            )

        with Catalog.from_directory(self.directory):
            manifest = TarChunker.get_volume_manifest_for_range(
                self.fingerprint, vol_index, volume_window
            )

        return FolderVolume(self.directory, manifest, vol_name)
