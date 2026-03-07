import logging
from pathlib import Path

from .database import DatabaseSession
from .models import TapeMetadata

logger = logging.getLogger(__name__)


class Catalog:
    """
    Represents a 'Master Catalog' (the .tape/database file).
    It is the entry point for inspecting metadata and opening players.
    """

    def __init__(self, db_path: str | Path):
        self.path = Path(db_path)
        self.db_session = DatabaseSession(self.path)

    def get_metadata_snapshot(self) -> dict[str, str]:
        "Load all the metadata into a dictionary and close the connection."
        snapshot = {}
        with self.db_session as db:
            query = TapeMetadata.select()
            for meta in query:
                snapshot[meta.key] = meta.value
        return snapshot

    def open(self):
        """Open the connection to the tape database."""
        return self.__enter__()

    def close(self):
        """Close the connection to the tape database."""
        self.__exit__(None, None, None)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db_session.close()

    def __enter__(self):
        self.db_session.connect()
        return self
