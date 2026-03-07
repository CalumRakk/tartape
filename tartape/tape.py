import json
import shutil
from pathlib import Path
from typing import Generator, List, Optional, Tuple, Union

import tartape
from tartape.chunker import TarChunker
from tartape.constants import TAPE_METADATA_DIR
from tartape.exceptions import TarIntegrityError
from tartape.models import Track
from tartape.player import TapePlayer
from tartape.schemas import VolumeManifest
from tartape.stream import FileVolume, FolderVolume, TapeVolume


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
        """Returns True if the tape is valid."""

        with tartape.get_catalog(self.directory) as cat:
            player = TapePlayer(self.directory)  # type: ignore

            try:
                if deep:
                    player._verify()
                else:
                    player._spot_check()
                return True
            except TarIntegrityError:
                if raise_exception:
                    raise
                return False

    def iter_volumes(
        self, size: int, naming_template: Optional[str] = None
    ) -> Generator[Tuple[TapeVolume, VolumeManifest], None, None]:
        """It breaks the tape down into logical and physical volumes."""
        with tartape.get_catalog(self.directory):
            player = TapePlayer(self.directory)
            chunker = TarChunker(chunk_size=size)
            yield from chunker.iter_volumes(player, naming_template=naming_template)

    def play(
        self,
        start_offset: int = 0,
        chunk_size: int = 64 * 1024,
        fast_verify: bool = True,
    ) -> Generator:
        with tartape.get_catalog(self.directory):
            player = TapePlayer(self.directory)
            yield from player.play(
                start_offset=start_offset,
                chunk_size=chunk_size,
                fast_verify=fast_verify,
            )

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
