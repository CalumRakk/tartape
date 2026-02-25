import hashlib
import logging
from pathlib import Path
from typing import Generator

from tartape.stream import TarStreamGenerator
from tartape.tape import Tape

from .models import Track
from .schemas import TarEvent

logger = logging.getLogger(__name__)


class TapePlayer:
    def __init__(self, tape: Tape, source_root: str | Path):
        self.tape = tape
        self.source_root = Path(source_root).absolute()

    def verify(self) -> bool:
        """Compare the signature recorded (saved) on the tape with that of the current disk."""
        recorded = self.tape.fingerprint

        sha = hashlib.sha256()
        for track in self.tape.get_tracks():
            current_path = self.source_root / track.rel_path
            try:
                # TODO: Improve, this is a repeated business rule.
                st = current_path.lstat()
                entry_data = f"{track.arc_path}|{st.st_size}|{st.st_mtime}"
                sha.update(entry_data.encode())
            except FileNotFoundError:
                sha.update(f"{track.arc_path}|MISSING|0".encode())

        current = sha.hexdigest()
        is_valid = recorded == current
        if not is_valid:
            logger.warning(
                "ATTENTION! The signature on the disk does not match the tape."
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

    def get_offset_of(self, arc_path: str) -> int:
        track = Track.get(Track.arc_path == arc_path)
        return track.start_offset
