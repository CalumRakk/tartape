import logging
from pathlib import Path
from typing import Iterable

import peewee

from tartape.constants import TAPE_DB_NAME, TAPE_METADATA_DIR

from .database import DatabaseSession
from .models import TapeMetadata, Track

logger = logging.getLogger(__name__)


class Catalog:
    """
    Represents a 'Master Catalog' (the .tape/database file).
    It is the entry point for inspecting metadata and opening players.
    """

    def __init__(self, db_path: str | Path):
        self.path = Path(db_path)
        self.db_session = DatabaseSession(self.path)

    @property
    def fingerprint(self) -> str:
        """Returns the digital signature of the tape."""
        return self._query_metadata("fingerprint")

    @property
    def total_size(self) -> int:
        """Returns the total size that the TAR stream will have (bytes)."""
        return int(self._query_metadata("total_size"))

    @property
    def count_files(self) -> int:
        """Returns the total number of files in the tape."""
        return Track.select().count()

    @classmethod
    def open(cls, path: str | Path) -> "Catalog":
        """Opens an existing tape."""
        logger.info(f"Opening tape from: {path}")
        if path != ":memory:" and not Path(path).exists():
            raise FileNotFoundError(f"The tape does not exist in: {path}")
        try:
            return cls(path)
        except peewee.OperationalError:
            logger.error(f"Failed to open tape at {path}. Is it a valid tape file?")
            raise FileNotFoundError(
                f"Failed to open tape at {path}. Is it a valid tape file?"
            )

    @classmethod
    def discover(cls, directory: str | Path) -> "Catalog":
        """
        Automatically searches for a .tartape file in the given directory.
        """
        target_dir = Path(directory)
        if not target_dir.is_dir():
            raise NotADirectoryError(f"{directory} is not a valid directory.")

        candidate = target_dir / TAPE_METADATA_DIR / TAPE_DB_NAME
        if candidate.exists() and candidate.is_file():
            return cls(candidate)

        raise FileNotFoundError(f"No tape found in: {candidate}")

    def get_tracks(self) -> Iterable[Track]:
        """Returns all tracks sorted for the stream."""
        return Track.select().order_by(Track.arc_path).iterator()

    def close(self):
        """Close the connection to the tape database."""
        self.db_session.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __enter__(self):
        self.db_session.connect()
        return self

    def _query_metadata(self, key: str) -> str:
        already_open = not self.db_session.db.is_closed()
        if not already_open:
            self.db_session.connect()

        try:
            return TapeMetadata.get(TapeMetadata.key == key).value
        finally:
            # We only close if we were the ones who opened it
            if not already_open:
                self.db_session.close()
