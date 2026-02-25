import hashlib
import logging
from pathlib import Path
from typing import Generator

from tartape.stream import TarStreamGenerator

from .database import DatabaseSession
from .models import TapeMetadata, Track
from .schemas import TarEvent

logger = logging.getLogger(__name__)


class TapePlayer:
    def __init__(self, tape_db_path: str | Path, source_root: str | Path):
        self.db_session = DatabaseSession(tape_db_path)
        self.db = self.db_session.start()
        self.source_root = Path(source_root).absolute()

    def get_fingerprint(self) -> str:
        meta = TapeMetadata.get(TapeMetadata.key == "fingerprint")
        return meta.value

    def calculate_current_fingerprint(self) -> str:
        """
        Calcula la firma actual analizando el disco duro.
        """
        sha = hashlib.sha256()
        for track in Track.select().order_by(Track.arc_path):
            current_path = self.source_root / track.rel_path

            # If the file does not exist, the fingerprint will change (os.stat will fail)
            try:
                # TODO: Improve, this is a repeated business rule.
                st = current_path.lstat()
                entry_data = f"{track.arc_path}|{st.st_size}|{st.st_mtime}"
                sha.update(entry_data.encode())
            except FileNotFoundError:
                logger.error(f"File lost during verification: {current_path}")
                sha.update(f"{track.arc_path}|MISSING|0".encode())

        return sha.hexdigest()

    def verify(self) -> bool:
        """Compara la firma grabada con la del disco actual."""
        recorded = self.get_fingerprint()
        current = self.calculate_current_fingerprint()

        is_valid = recorded == current
        if not is_valid:
            logger.warning(
                "WARNING! The signature on the tape does not match the current disk."
            )
        return is_valid

    def play(
        self, start_offset: int = 0, chunk_size: int = 64 * 1024
    ) -> Generator[TarEvent, None, None]:

        # Find those whose 'end_offset' is greater than our starting point
        query = (
            Track.select()
            .where(Track.end_offset > start_offset)
            .order_by(Track.arc_path)
        )

        def track_to_entry_gen():
            for track in query:
                track._source_root = self.source_root
                yield track

        engine = TarStreamGenerator(track_to_entry_gen())
        yield from engine.stream(start_offset=start_offset, chunk_size=chunk_size)

    def close(self):
        self.db_session.close()

    def get_offset_of(self, arc_path: str) -> int:
        track = Track.get(Track.arc_path == arc_path)
        return track.start_offset
