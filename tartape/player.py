import logging
from pathlib import Path
from typing import Generator, cast

import peewee

from tartape.schemas import TarEvent
from tartape.stream import TarStreamGenerator
from tartape.tape import Tape

from .models import Track

logger = logging.getLogger(__name__)


class TapePlayer:
    def __init__(self, tape: Tape, source_root: str | Path):
        self.tape = tape
        self.source_root = Path(source_root).absolute()

    def verify(self) -> bool:
        """Compare the signature recorded (saved) on the tape with that of the current disk."""
        logger.info("Starting full integrity verification...")

        for track in self.tape.get_tracks():
            if not self._assert_track_integrity(track):
                return False

        logger.info("Full integrity check PASSED.")
        return True

    def _assert_track_integrity(self, track: Track) -> bool:
        """
        Centralized method for validating a track against the disk.
        Returns True if valid.
        """
        status = self._get_track_status(track)

        if not status["exists"]:
            logger.error(
                f"Integrity FAILED: File missing -> {track.arc_path} (Expected at: {track.rel_path})"
            )
            return False

        if status["size"] != track.size or status["mtime"] != track.mtime:
            logger.error(
                f"Integrity FAILED: File mutated -> {track.arc_path}\n"
                f"  Expected: size {track.size}, mtime {track.mtime}\n"
                f"  Found:    size {status['size']}, mtime {status['mtime']}"
            )
            return False

        return True

    def _get_track_status(self, track: Track) -> dict:
        """Gets current size and mtime of a track on disk."""
        p = self.source_root / track.rel_path
        try:
            st = p.lstat()
            return {"size": st.st_size, "mtime": int(st.st_mtime), "exists": True}
        except FileNotFoundError:
            return {"size": 0, "mtime": 0, "exists": False}

    def spot_check(self, sample_size: int = 10) -> bool:
        """
        Select N files at random and check their integrity.
        It is a quick way to detect if the folder has been altered
        without processing the entire tape.
        """
        total_tracks = Track.select().count()
        if total_tracks == 0:
            return True

        current_sample_size = min(sample_size, total_tracks)

        # SQLite to give us N random records
        samples = Track.select().order_by(peewee.fn.Random()).limit(current_sample_size)

        logger.info(f"Performing spot check on {current_sample_size} random files...")

        for track in samples:
            if not self._assert_track_integrity(track):
                return False

        logger.info("Spot check PASSED.")
        return True

    def _verify_resume_point(self, offset: int):
        """
        Find the track containing the requested offset and validate its integrity.
        Ensure the resume point is consistent.
        """

        if offset < 0:
            raise ValueError(f"Offset cannot be negative: {offset}")

        if offset >= self.tape.total_size:
            raise ValueError(
                f"Offset {offset} is beyond the total tape size ({self.tape.total_size})"
            )

        # If the offset falls here, there is no file to validate, it's just zeros.
        if offset >= self.tape.total_size - 1024:
            logger.info(
                f"Resume point at {offset} falls into the TAR footer. No file validation needed."
            )
            return

        try:
            track = cast(
                Track,
                Track.get((Track.start_offset <= offset) & (Track.end_offset > offset)),
            )

            logger.info(f"Verifying resume point at file: {track.arc_path}")

            if not self._assert_track_integrity(track):
                raise RuntimeError(
                    f"Resume integrity error: The file '{track.arc_path}' at offset {offset} "
                    f"has changed or is missing. Cannot resume stream safely."
                )

        except Track.DoesNotExist:  # type: ignore
            raise RuntimeError(
                f"Critical error: No track found for offset {offset} despite being within bounds."
            )

    def play(
        self, start_offset: int = 0, chunk_size: int = 64 * 1024
    ) -> Generator[TarEvent, None, None]:

        if not self.spot_check(sample_size=10):
            raise RuntimeError(
                "Integrity check failed (spot check). The disk state does not match the tape."
            )

        if start_offset > 0:
            self._verify_resume_point(start_offset)

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
