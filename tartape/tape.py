import json
import logging
import shutil
from pathlib import Path
from typing import Generator, List, Optional, Tuple, Union

import peewee

import tartape
from tartape.catalog import Catalog
from tartape.chunker import TarChunker
from tartape.constants import TAPE_METADATA_DIR
from tartape.models import Track
from tartape.schemas import VolumeManifest
from tartape.stream import FileVolume, FolderVolume, TapeVolume, TarStreamGenerator

logger = logging.getLogger(__name__)


class Tape:
    """
    The Master Class. It represents a complete data tape.
    It is the engine that orchestrates the Catalog, the Player, and the Chunker.
    """

    def __init__(self, directory: Union[str, Path]):
        self.directory = Path(directory).resolve()
        self._metadata_cache: dict[str, str] = {}
        self._load_metadata()

    def _load_metadata(self):
        if not tartape.exists(self.directory):
            raise FileNotFoundError(f"The tape does not exist in: {self.directory}")
        cat = tartape.get_catalog(self.directory)
        self._metadata_cache = cat.get_metadata_snapshot()

    @property
    def count_files(self) -> int:
        """Returns the total number of files in the tape."""
        return int(self._metadata_cache.get("count_files", 0))

    @property
    def fingerprint(self) -> str:
        """Returns the digital signature of the tape."""
        return self._metadata_cache.get("fingerprint", "")

    @property
    def total_size(self) -> int:
        """Returns the total size that the TAR stream will have (bytes)."""
        return int(self._metadata_cache.get("total_size", 0))

    @property
    def created_at(self) -> int:
        return int(self._metadata_cache.get("created_at", 0))

    @property
    def exclude_patterns(self) -> List[str] | str:
        value = self._metadata_cache.get("exclude_patterns", "")
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

    def iter_volumes(
        self, size: int, naming_template: Optional[str] = None
    ) -> Generator[Tuple[TapeVolume, VolumeManifest], None, None]:
        """It breaks the tape down into logical and physical volumes."""
        with tartape.get_catalog(self.directory):
            chunker = TarChunker(chunk_size=size)
            yield from chunker.iter_volumes(
                self.directory, naming_template=naming_template
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

        # with tartape.get_catalog(self.directory):
        #     player = TapePlayer(self.directory)
        #     yield from player.play(
        #         start_offset=start_offset,
        #         chunk_size=chunk_size,
        #         fast_verify=fast_verify,
        #     )

    def get_volume(
        self, start: int, end: int, name: Optional[str] = None
    ) -> TapeVolume:
        if not tartape.exists(self.directory):
            raise FileNotFoundError(f"The tape does not exist in: {self.directory}")

        name = name or self.directory.name
        return FolderVolume(self.directory, start, end, name)

    @classmethod
    def get_file_volume(cls, path: Path, start: int, end: int, name: str):
        """Instancia un volumen de un archivo plano."""
        return FileVolume(path, start, end, name)
