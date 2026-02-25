from pathlib import Path
from typing import Iterable

from .database import DatabaseSession
from .models import TapeMetadata, Track


class Tape:
    """
    Represents a 'Master Tape' (the .tape/database file).
    It is the entry point for inspecting metadata and opening players.
    """

    def __init__(self, db_path: str | Path):
        self.path = Path(db_path)
        self._session = DatabaseSession(self.path)
        self.db = self._session.start()

    @classmethod
    def open(cls, path: str | Path) -> "Tape":
        """Opens an existing tape."""
        if path != ":memory:" and not Path(path).exists():
            raise FileNotFoundError(f"The tape does not exist in: {path}")
        return cls(path)

    @classmethod
    def discover(cls, directory: str | Path) -> "Tape":
        """
        Automatically searches for a .tartape file in the given directory.
        """
        target_dir = Path(directory)
        if not target_dir.is_dir():
            raise NotADirectoryError(f"{directory} is not a valid directory.")

        candidate = target_dir / ".tartape"
        if candidate.exists() and candidate.is_file():
            return cls(candidate)

        raise FileNotFoundError(f"No tape (.tartape) found in {directory}")

    @property
    def fingerprint(self) -> str:
        """Returns the digital signature of the tape."""
        return TapeMetadata.get(TapeMetadata.key == "fingerprint").value

    @property
    def total_size(self) -> int:
        """Returns the total size that the TAR stream will have (bytes)."""
        val = TapeMetadata.get(TapeMetadata.key == "total_size").value
        return int(val)

    def get_tracks(self) -> Iterable[Track]:
        """Returns all tracks sorted for the stream."""
        return Track.select().order_by(Track.arc_path)

    def close(self):
        """Close the connection to the tape database."""
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
