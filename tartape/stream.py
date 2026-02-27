import hashlib
import logging
from typing import Generator, Iterable, Optional

from tartape.header import TarHeader

from .constants import CHUNK_SIZE_DEFAULT, TAR_FOOTER_SIZE
from .models import Track
from .schemas import (
    FileEndMetadata,
    FileStartMetadata,
    TarEvent,
    TarFileDataEvent,
    TarFileEndEvent,
    TarFileStartEvent,
    TarTapeCompletedEvent,
)

logger = logging.getLogger(__name__)


class TarIntegrityError(Exception):
    """Exception thrown when disk does not match inventory (ADR-002)."""

    pass


class TarStreamGenerator:
    def __init__(self, entries: Iterable[Track]):
        self.entries = entries

    def stream(
        self, start_offset: int = 0, chunk_size: Optional[int] = None
    ) -> Generator[TarEvent, None, None]:

        effective_chunk_size = chunk_size or CHUNK_SIZE_DEFAULT
        logger.info(
            f"Starting TAR stream. Offset: {start_offset}, Chunk Size: {effective_chunk_size}"
        )

        last_offset = 0

        for entry in self.entries:

            # If we already passed this entire file, we skip it
            if start_offset >= entry.end_offset:
                last_offset = entry.end_offset
                continue

            # Always played whenever the stream touches this file.
            yield self._create_event_start(entry, start_offset)

            yield from self._emit_header(entry, start_offset)

            md5_hash: Optional[str] = None
            if entry.has_content:
                md5_hash = yield from self._stream_file_content_safely(
                    entry, start_offset, effective_chunk_size
                )
                yield from self._emit_padding(entry, start_offset)

            yield self._create_event_end(entry, md5_hash)
            last_offset = entry.end_offset

        yield from self._emit_tape_footer(start_offset, last_offset)
        yield TarTapeCompletedEvent(type="tape_completed")
        logger.info("TAR stream completed successfully.")

    def _build_header(self, track: Track) -> bytes:
        header = TarHeader(track)
        return header.build()

    def _create_event_start(self, entry: Track, global_skip: int) -> TarFileStartEvent:
        is_resumed = global_skip > entry.start_offset
        return TarFileStartEvent(
            type="file_start",
            entry=entry,
            metadata=FileStartMetadata(
                start_offset=entry.start_offset, resumed=is_resumed
            ),
        )

    def _create_event_end(self, entry: Track, md5: Optional[str]) -> TarFileEndEvent:

        return TarFileEndEvent(
            type="file_end",
            entry=entry,
            metadata=FileEndMetadata(
                md5sum=md5, end_offset=entry.end_offset, is_complete=(md5 is not None)
            ),
        )

    def _emit_header(
        self, entry: Track, global_skip: int
    ) -> Generator[TarEvent, None, None]:
        if global_skip >= entry.header_end_offset:
            return
        local_skip = max(0, global_skip - entry.start_offset)
        header_bytes = self._build_header(entry)[local_skip:]

        if header_bytes:
            yield TarFileDataEvent(type="file_data", data=header_bytes)

    def _stream_file_content_safely(
        self, entry: Track, global_skip: int, chunk_size: int
    ) -> Generator[TarEvent, None, Optional[str]]:
        """Safely stream file content, ensuring that we do not read past the end of the file."""

        content_start = entry.header_end_offset
        if global_skip >= entry.content_end_offset:
            return None  # Jump drops after content

        self._validate_integrity(entry)
        local_skip = max(0, global_skip - content_start)
        bytes_remaining = entry.size - local_skip

        # Only calculate MD5 if we are reading the file from the beginning
        md5 = hashlib.md5() if local_skip == 0 else None

        try:
            with open(entry.source_path, "rb") as f:
                if local_skip > 0:
                    f.seek(local_skip)

                while bytes_remaining > 0:
                    read_size = min(chunk_size, bytes_remaining)
                    chunk = f.read(read_size)

                    if not chunk:
                        raise RuntimeError(f"File shrunk: '{entry.source_path}'")

                    if md5:
                        md5.update(chunk)

                    bytes_remaining -= len(chunk)
                    yield TarFileDataEvent(type="file_data", data=chunk)

                if local_skip == 0 and f.read(1):
                    raise RuntimeError(f"File grew: '{entry.source_path}'")

        except OSError as e:
            raise TarIntegrityError(f"Error leyendo {entry.source_path}") from e

        return md5.hexdigest() if md5 else None

    def _emit_padding(
        self, entry: Track, global_skip: int
    ) -> Generator[TarEvent, None, None]:
        # Padding starts where the data ends and ends at end_offset
        if global_skip >= entry.end_offset or entry.content_end_offset == entry.end_offset:
            return
        local_skip = max(0, global_skip - entry.content_end_offset)
        padding_to_send = (entry.end_offset - entry.content_end_offset) - local_skip

        if padding_to_send > 0:
            yield TarFileDataEvent(type="file_data", data=b"\0" * padding_to_send)

    def _emit_tape_footer(
        self, global_skip: int, footer_start: int
    ) -> Generator[TarEvent, None, None]:
        footer_end = footer_start + TAR_FOOTER_SIZE

        if global_skip >= footer_end:
            return

        local_skip = max(0, global_skip - footer_start)
        footer = b"\0" * (TAR_FOOTER_SIZE - local_skip)

        if footer:
            yield TarFileDataEvent(type="file_data", data=footer)

    def _validate_integrity(self, entry: Track):
        """
        Strict implementation of ADR-002.
        Verify that the file on disk matches the inventory.
        """
        try:
            st = entry.source_path.lstat()
        except OSError as e:
            raise TarIntegrityError(f"Archivo inaccesible: {entry.source_path}") from e

            # Mtime Consistency
            # Using a tiny epsilon for float comparison safety
            # TODO: Make the conversion from mtime to integer more explicit. The logic is repeating itself.        if abs(int(st.st_mtime) - entry.mtime) > 1e-4:
            msg = (
                f"File modified (mtime) between inventory and stream: "
                f"'{entry.source_path}'. Aborting."
            )
            logger.error(msg)
            raise RuntimeError(msg)

        if st.st_size != entry.size:
            msg = (
                f"File size changed: '{entry.source_path}'. "
                f"Expected {entry.size}, found {st.st_size}."
            )
            logger.error(msg)
            raise RuntimeError(msg)
