import json
import shutil
from pathlib import Path
from typing import Generator, List, Optional, Tuple, Union

from tartape.catalog import Catalog
from tartape.chunker import TarChunker
from tartape.constants import TAPE_METADATA_DIR
from tartape.exceptions import TarIntegrityError
from tartape.factory import ExcludeType
from tartape.player import TapePlayer
from tartape.recorder import TapeRecorder
from tartape.schemas import VolumeManifest
from tartape.stream import FileVolume, FolderVolume, TapeVolume


class Tape:
    """
    The Master Class. It represents a complete data tape.
    It is the engine that orchestrates the Catalog, the Player, and the Chunker.
    """

    def __init__(self, path: Union[str, Path]):
        self.path = Path(path).resolve()
        self._catalog: Optional[Catalog] = None

    def __enter__(self):
        self._open_catalog()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._catalog:
            self._catalog.close()

    @property
    def files(self):
        self._open_catalog()
        return self._catalog.get_tracks()  # type: ignore

    @property
    def count_files(self) -> int:
        """Returns the total number of files in the tape."""
        self._open_catalog()
        return self._catalog.count_files  # type: ignore

    @property
    def fingerprint(self) -> str:
        self._open_catalog()
        return self._catalog._query_metadata("fingerprint")  # type: ignore

    @property
    def total_size(self) -> int:
        self._open_catalog()
        return int(self._catalog._query_metadata("total_size"))  # type: ignore

    @property
    def created_at(self) -> int:
        self._open_catalog()
        return int(self._catalog._query_metadata("created_at"))  # type: ignore

    @property
    def exclude_patterns(self) -> List[str] | str:
        self._open_catalog()
        value = self._catalog._query_metadata("exclude_patterns")  # type: ignore
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    @classmethod
    def create(
        cls,
        directory: Union[str, Path],
        exclude: Optional[ExcludeType] = None,
        anonymize: bool = True,
        calculate_hashes: bool = False,
    ) -> "Tape":
        """Record a new tape and return the Tape object."""
        recorder = TapeRecorder(directory, exclude, anonymize, calculate_hashes)
        recorder.commit()
        return cls(directory)

    @classmethod
    def open(cls, path: Union[str, Path]) -> "Tape":
        """Open an existing tape."""
        if not Path(path).exists():
            raise FileNotFoundError(f"The tape does not exist in: {path}")
        return cls(path)

    @classmethod
    def exists(cls, path: Union[str, Path]) -> bool:
        try:
            Catalog.discover(path)
            return True
        except FileNotFoundError:
            return False

    def destroy(self):
        if self._catalog:
            self._catalog.close()
            self._catalog = None

        metadata_dir = self.path / TAPE_METADATA_DIR
        if metadata_dir.exists():
            shutil.rmtree(metadata_dir)

    def _open_catalog(self):
        if not self._catalog:
            self._catalog = Catalog.discover(self.path)

    def verify(self, deep: bool = False, raise_exception: bool = False):
        """Returns True if the tape is valid."""
        self._open_catalog()
        player = TapePlayer(self._catalog, self.path)  # type: ignore

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
        self._open_catalog()
        player = TapePlayer(self._catalog, self.path)  # type: ignore
        chunker = TarChunker(self._catalog, chunk_size=size)  # type: ignore
        yield from chunker.iter_volumes(player, naming_template=naming_template)

    def play(
        self,
        start_offset: int = 0,
        chunk_size: int = 64 * 1024,
        fast_verify: bool = True,
    ) -> Generator:
        self._open_catalog()
        player = TapePlayer(self._catalog, self.path)  # type: ignore
        return player.play(
            start_offset=start_offset, chunk_size=chunk_size, fast_verify=fast_verify
        )

    @classmethod
    def get_volume(
        cls, path: str | Path, start: int, end: int, name: Optional[str] = None
    ) -> TapeVolume:
        path = Path(path)
        vol_name = name or path.name

        if cls.exists(path):
            t = cls.open(path)
            player = TapePlayer(t._catalog, t.path)  # type: ignore
            return FolderVolume(player, start, end, vol_name)

        if path.is_file():
            return FileVolume(path, start, end, vol_name)

        raise ValueError("The path must be a file or a directory (Tape).")
