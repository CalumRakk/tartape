from pathlib import Path
from typing import Generator, Optional, Tuple, Union

from tartape.catalog import Catalog
from tartape.chunker import TarChunker
from tartape.factory import ExcludeType
from tartape.player import TapePlayer
from tartape.recorder import TapeRecorder
from tartape.schemas import VolumeManifest
from tartape.volume import TarVolume


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
        assert self._catalog, "Catalog is not open"
        return self._catalog.get_tracks()

    @property
    def count_files(self) -> int:
        """Returns the total number of files in the tape."""
        self._open_catalog()
        assert self._catalog, "Catalog is not open"
        return int(self._catalog._query_metadata("count_files"))

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

    def _open_catalog(self):
        if not self._catalog:
            self._catalog = Catalog.discover(self.path)

    @property
    def fingerprint(self) -> str:
        self._open_catalog()
        return self._catalog.fingerprint  # type: ignore

    @property
    def total_size(self) -> int:
        self._open_catalog()
        return self._catalog.total_size  # type: ignore

    def verify(self, deep: bool = False):
        """Verify the physical integrity of the disc against the catalog."""
        self._open_catalog()
        player = TapePlayer(self._catalog, self.path)  # type: ignore
        if deep:
            player._verify()
        else:
            player._spot_check()

    def iter_volumes(
        self, size: int, naming_template: Optional[str] = None
    ) -> Generator[Tuple[TarVolume, VolumeManifest], None, None]:
        """It breaks the tape down into logical and physical volumes."""
        self._open_catalog()
        player = TapePlayer(self._catalog, self.path)  # type: ignore
        chunker = TarChunker(self._catalog, chunk_size=size)  # type: ignore
        yield from chunker.iter_volumes(player, naming_template=naming_template)
