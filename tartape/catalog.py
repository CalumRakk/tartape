import logging
from pathlib import Path

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

    def get_track_at_offset(self, offset: int) -> Track:
        """Find the track that contains a specific offset."""
        try:
            return Track.get(
                (Track.start_offset <= offset) & (Track.end_offset > offset)
            )
        except Track.DoesNotExist:  # type: ignore
            raise RuntimeError(f"No se encontró un track para el offset {offset}")

    def get_tracks_for_stream(self, start_offset: int):
        """Returns an iterator of tracks that should enter the stream."""
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
