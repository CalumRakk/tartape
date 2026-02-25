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
            raise FileNotFoundError(f"La cinta no existe en: {path}")
        return cls(path)

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
