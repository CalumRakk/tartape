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
from tartape.models import Track
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
            with tartape.get_catalog(self.directory) as cat:
                if deep:
                    for track in Track.select().order_by(Track.arc_path).iterator():
                        track.validate_integrity(self.directory)
                else:
                    total = Track.select().count()
                    if total > 0:
                        samples = Track.select().order_by(peewee.fn.Random()).limit(15)
                        for track in samples:
                            track.validate_integrity(self.directory)
                return True
        except Exception:
            if raise_exception:
                raise
            return False

    def _verify_resume_point(self, catalog: Catalog, offset: int):
        """
        Find the track containing the requested offset and validate its integrity.
        Ensure the resume point is consistent.
        """

        if offset < 0:
            raise ValueError(f"Offset cannot be negative: {offset}")

        if offset >= self.total_size:
            raise ValueError(
                f"Offset {offset} is beyond the total tape size ({self.total_size})"
            )

        # If the offset falls here, there is no file to validate, it's just zeros.
        if offset >= self.total_size - 1024:
            logger.info(
                f"Resume point at {offset} falls into the TAR footer. No file validation needed."
            )
            return

        track = catalog.get_track_at_offset(offset)
        track.validate_integrity(self.directory)

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

        with tartape.get_catalog(self.directory) as cat:
            if start_offset > 0:
                self._verify_resume_point(cat, start_offset)

            query = cat.get_tracks_for_stream(start_offset)

            def track_loader():
                for track in query:
                    track.source_path = self.directory
                    yield track

            engine = TarStreamGenerator(track_loader(), self.directory)
            yield from engine.stream(start_offset=start_offset, chunk_size=chunk_size)

    def get_volume(
        self, vol_name: str, vol_index: int, vol_start: int, vol_end: int
    ) -> TapeVolume:
        if not tartape.exists(self.directory):
            raise FileNotFoundError(f"The tape does not exist in: {self.directory}")

        # Basic range validation
        if vol_start < 0 or vol_end > self.total_size or vol_start >= vol_end:
            raise ValueError(
                f"Invalid range: {vol_start}-{vol_end}. Total tape size is {self.total_size}"
            )

        with Catalog.from_directory(self.directory):
            manifest = TarChunker.get_volume_manifest_for_range(
                self.fingerprint, vol_index, vol_start, vol_end
            )

        return FolderVolume(self.directory, manifest, vol_name)
