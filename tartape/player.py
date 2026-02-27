import logging
import os
from pathlib import Path
from typing import Generator, Iterable, cast

from tartape.factory import TarEntryFactory
from tartape.schemas import TarEvent
from tartape.stream import TarStreamGenerator
from tartape.tape import Tape

from .models import Track

logger = logging.getLogger(__name__)


class TapePlayer:
    def __init__(self, tape: Tape, directory: str | Path):
        self.tape = tape
        self.directory = Path(directory).absolute()

    def _verify(self) -> bool:
        """Compare the signature recorded (saved) on the tape with that of the current disk."""
        logger.info("Starting full integrity verification...")

        for track in self.tape.get_tracks():
            self._assert_track_integrity(track)

        logger.info("Full integrity check PASSED.")
        return True

    def _assert_track_integrity(self, track: Track):
        """
        Centralized method for validating a track against the disk.
        Returns True if valid.
        """
        status = self._get_track_status(track)

        if not status["exists"]:
            raise RuntimeError(f"Integrity FAILED: File missing -> {track.arc_path}")

        if track.is_dir:
            # If it's the ROOT (empty rel_path), we ignore mtime
            if track.rel_path == "" or track.rel_path == ".":
                return

            if status["mtime"] != track.mtime:
                raise RuntimeError(
                    "Integrity FAILED: Subfolder structure modified -> {track.arc_path}."
                    "New or deleted files were detected."
                )
            return
        else:
            if track.is_symlink:
                actual_link = os.readlink(self.directory / track.rel_path)
                if actual_link != track.linkname:
                    raise RuntimeError(
                        f"Integrity FAILED: The link {track.arc_path} points to another location."
                    )
                return

            if status["mtime"] != track.mtime:
                raise RuntimeError(f"File modified (mtime): {track.arc_path}")

            if status["size"] != track.size:
                raise RuntimeError(
                    f"File size changed: '{track.arc_path}'. Expected {track.size}, found {status['size']}."
                )
            if status["mode"] != track.mode:
                raise RuntimeError(
                    f"File mode changed: '{track.arc_path}'. Expected {track.mode}, found {status['mode']}."
                )

    def _get_track_status(self, track: Track) -> dict:
        """Gets current size and mtime of a track on disk."""
        p = self.directory / track.rel_path
        try:
            st = p.lstat()
            file_mode, _, _, _, _= TarEntryFactory._extract_metadata(st)
            return {
                "size": st.st_size,
                "mtime": int(st.st_mtime),
                "exists": True,
                "mode": file_mode,
            }
        except FileNotFoundError:
            return {"size": 0, "mtime": 0, "exists": False}

    def _spot_check(self, sample_size: int = 10):
        """
        Select N files at random and check their integrity.
        It is a quick way to detect if the folder has been altered
        without processing the entire tape.
        """
        total_tracks = Track.select().count()
        if total_tracks == 0:
            return

        current_sample_size = min(sample_size, total_tracks)
        import peewee
        # SQLite to give us N random records
        samples = Track.select().order_by(peewee.fn.Random()).limit(current_sample_size)

        logger.info(f"Performing spot check on {current_sample_size} random files...")

        for track in samples:
            self._assert_track_integrity(track)

        logger.info("Spot check PASSED.")

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
            self._assert_track_integrity(track)
        except Track.DoesNotExist:  # type: ignore
            raise RuntimeError(
                f"Critical error: No track found for offset {offset} despite being within bounds."
            )

    def play(
        self,
        start_offset: int = 0,
        chunk_size: int = 64 * 1024,
        fast_verify: bool = True,
    ) -> Generator[TarEvent, None, None]:
        logger.debug("Starting tape playback...")

        with self.tape:
            if fast_verify:
                self._spot_check(sample_size=15)
            else:
                self._verify()

            if start_offset > 0:
                logger.info(f"Resuming stream from offset: {start_offset} bytes")
                self._verify_resume_point(start_offset)

            # Find those whose 'end_offset' is greater than our starting point
            query = cast(Iterable[Track],
                Track.select()
                .where(Track.end_offset > start_offset)
                .order_by(Track.arc_path)
            )

            def track_to_entry_gen():
                for track in query:
                    track.source_path = self.directory
                    yield track

            engine = TarStreamGenerator(track_to_entry_gen())
            yield from engine.stream(start_offset=start_offset, chunk_size=chunk_size)

    def get_offset_of(self, arc_path: str) -> int:
        track = Track.get(Track.arc_path == arc_path)
        return track.start_offset
