import logging
from pathlib import Path
from typing import Iterable

from .database import DatabaseSession
from .models import TapeMetadata, Track

logger = logging.getLogger(__name__)


class Catalog:
    def __init__(self, db_path: str | Path):
        self.path = Path(db_path)
        self.db_session = DatabaseSession(self.path)

    def get_stats(self) -> dict:
        """
        Query the DB and return a clean dictionary of metadata.
        """
        query = TapeMetadata.select()
        stats = {m.key: m.value for m in query}
        return {
            "fingerprint": stats.get("fingerprint", ""),
            "total_size": int(stats.get("total_size", 0)),
            "created_at": int(stats.get("created_at", 0)),
            "exclude_patterns": stats.get("exclude_patterns", ""),
        }

    def get_track_count(self) -> int:
        return Track.select().count()

    def find_track_at_absolute_offset(self, absolute_offset: int) -> Track:
        """
        Finds the raw Track record that covers a specific byte position
        in the global tape map.
        """
        try:
            return Track.get(
                (Track.start_offset <= absolute_offset)
                & (Track.end_offset > absolute_offset)
            )
        except Track.DoesNotExist:  # type: ignore
            raise RuntimeError(f"No track found at absolute offset {absolute_offset}")

    def query_tracks_intersecting_range(self, start_offset: int) -> Iterable[Track]:
        """
        Queries the database for tracks that exist from this point forward.
        Returns raw Track objects (Static Data).
        """
        return (
            Track.select()
            .where(Track.end_offset > start_offset)
            .order_by(Track.arc_path)
            .iterator()
        )

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

    @classmethod
    def from_directory(cls, directory: str | Path) -> "Catalog":
        from tartape import discover

        db_path = discover(directory)

        if not db_path:
            raise FileNotFoundError(f"TarTape index not found in: {directory}")

        return cls(db_path)
