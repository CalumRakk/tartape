import hashlib
import io
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Iterable, Optional

if TYPE_CHECKING:
    from tartape.player import TapePlayer

import tartape
from tartape.exceptions import TarIntegrityError
from tartape.header import TarHeader

from .constants import CHUNK_SIZE_DEFAULT, TAR_BLOCK_SIZE, TAR_FOOTER_SIZE
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


class TapeVolume(io.BufferedIOBase):
    def __init__(self, name: str, size: int):
        self.name = name
        self.size = size

    @property
    def md5sum(self) -> str:
        raise NotImplementedError

    @property
    def is_completed(self) -> bool:
        raise NotImplementedError


class TarStreamGenerator:
    def __init__(self, entries: Iterable[Track], directory: str | Path):
        self.directory = Path(directory)
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
        local_skip, bytes_to_send = self._get_stream_window(
            global_skip, entry.start_offset, TAR_BLOCK_SIZE
        )
        if bytes_to_send > 0:
            header = self._build_header(entry)[local_skip:]
            yield TarFileDataEvent(type="file_data", data=header)

    def _get_stream_window(
        self, global_skip: int, block_start: int, block_length: int
    ) -> tuple[int, int]:
        """
        Given a global resume position and a data block,
        calculate the local offset and how many bytes actually need to be sent.

        Returns: (local_skip, bytes_to_send)
        """
        block_end = block_start + block_length

        # Case: The restart point has now passed this entire block
        if global_skip >= block_end:
            return 0, 0

        # Case: The block is within or beyond the resume point
        local_skip = max(0, global_skip - block_start)
        bytes_to_send = block_length - local_skip

        return local_skip, bytes_to_send

    def _stream_file_content_safely(
        self, entry: Track, global_skip: int, chunk_size: int
    ) -> Generator[TarEvent, None, Optional[str]]:
        """Safely stream file content, ensuring that we do not read past the end of the file."""

        local_skip, bytes_remaining = self._get_stream_window(
            global_skip, entry.header_end_offset, entry.size
        )

        if bytes_remaining <= 0:
            return None

        entry.validate_integrity(self.directory)
        md5 = hashlib.md5() if local_skip == 0 else None

        try:
            with open(entry.source_path, "rb") as f:
                if local_skip > 0:
                    f.seek(local_skip)

                while bytes_remaining > 0:
                    read_size = min(chunk_size, bytes_remaining)
                    chunk = f.read(read_size)
                    if not chunk:
                        raise TarIntegrityError(f"File shrunk: '{entry.source_path}'")

                    if md5:
                        md5.update(chunk)
                    bytes_remaining -= len(chunk)
                    yield TarFileDataEvent(type="file_data", data=chunk)

                if local_skip == 0:
                    extra = f.read(1)
                    if extra:
                        raise TarIntegrityError(
                            f"File grew: '{entry.source_path}'. Bytes left: {extra}"
                        )

        except OSError as e:
            raise TarIntegrityError(f"Error leyendo {entry.source_path}") from e

        return md5.hexdigest() if md5 else None

    def _emit_padding(
        self, entry: Track, global_skip: int
    ) -> Generator[TarEvent, None, None]:
        # Padding starts where the data ends and ends at end_offset
        padding_total = entry.end_offset - entry.content_end_offset

        _, bytes_to_send = self._get_stream_window(
            global_skip, entry.content_end_offset, padding_total
        )
        if bytes_to_send > 0:
            yield TarFileDataEvent(type="file_data", data=b"\0" * bytes_to_send)

    def _emit_tape_footer(
        self, global_skip: int, footer_start: int
    ) -> Generator[TarEvent, None, None]:
        _, bytes_to_send = self._get_stream_window(
            global_skip, footer_start, TAR_FOOTER_SIZE
        )

        if bytes_to_send > 0:
            yield TarFileDataEvent(type="file_data", data=b"\0" * bytes_to_send)


class FolderVolume(TapeVolume):
    def __init__(
        self,
        directory: Path,
        start_offset: int,
        end_offset: int,
        name: str,
        catalog=None,
    ):
        super().__init__(name, end_offset - start_offset)
        self.directory = directory
        self.start_offset = start_offset
        self.end_offset = end_offset
        self.size = end_offset - start_offset

        # State
        self._catalog = None
        self._external_catalog = catalog is not None
        self._player = None
        self._stream_gen = None
        self._position = 0
        self._buffer = bytearray()
        self._closed = True

        # Hash Status
        self._md5 = hashlib.md5()
        self._hash_cursor = 0
        self._md5_invalid = False

    def _ensure_not_closed(self):
        if self._closed:
            raise ValueError("I/O operation on closed volume.")

    def _init_stream(self, offset_in_volume: int):
        self._position = offset_in_volume
        self._buffer.clear()

        if offset_in_volume == 0:
            self._md5 = hashlib.md5()
            self._hash_cursor = 0
            self._md5_invalid = False
        else:
            # If we jump to the middle of the file, the MD5 hash can no longer be guaranteed.
            # Unless the jump is exactly to where the hash left off.
            if offset_in_volume != self._hash_cursor:
                self._md5_invalid = True

        global_target = self.start_offset + offset_in_volume
        if not self._player:
            raise RuntimeError("Tape player not initialized.")
        self._stream_gen = self._player.play(start_offset=global_target)

    @property
    def md5sum(self) -> str:
        if self._md5_invalid:
            raise RuntimeError(
                f"MD5 not available for '{self.name}': Non-linear read detected"
                "(jumps with seek). To obtain the MD5 hash, the file must be read sequentially."
            )
        if self._hash_cursor < self.size:
            raise RuntimeError(
                f"MD5 not available: Incomplete read ({self._hash_cursor}/{self.size})."
            )
        return self._md5.hexdigest()

    @property
    def is_completed(self) -> bool:
        return self._position == self.size

    def __enter__(self):
        from tartape.player import TapePlayer

        if not self._closed:
            return self

        if not self._external_catalog:
            self._catalog = tartape.get_catalog(self.directory)
            self._catalog.open()

        self._player = TapePlayer(self.directory)
        self._closed = False
        self._init_stream(0)
        return self

    def __exit__(self, *args):
        if not self._external_catalog and self._catalog:
            self._catalog.close()

        self._closed = True
        self._stream_gen = None

    def open(self):
        self.__enter__()

    def close(self):
        self.__exit__(None, None, None)

    def read(self, size: int = -1) -> bytes:  # type: ignore
        self._ensure_not_closed()
        if self._position >= self.size:
            return b""

        remaining = self.size - self._position
        bytes_to_read = (
            remaining if (size is None or size < 0) else min(size, remaining)
        )

        while len(self._buffer) < bytes_to_read:
            try:
                assert self._stream_gen is not None, "Stream generator not initialized"
                event = next(self._stream_gen)
                if event.type == "file_data":
                    self._buffer.extend(event.data)
            except StopIteration:
                break

        chunk_size = min(bytes_to_read, len(self._buffer))
        chunk = bytes(self._buffer[:chunk_size])
        self._buffer = self._buffer[chunk_size:]

        if not self._md5_invalid:
            # If the read cursor matches the hash cursor, we continue processing
            if self._position == self._hash_cursor:
                self._md5.update(chunk)
                self._hash_cursor += len(chunk)
            else:
                # The developer read something out of linear order
                self._md5_invalid = True

        self._position += len(chunk)
        return chunk

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """
        Supports jumping to the beginning (rewind) and the end (so that libraries like requests can calculate the Content-Length).
        """
        self._ensure_not_closed()
        target = 0
        if whence == io.SEEK_SET:
            target = offset
        elif whence == io.SEEK_CUR:
            target = self._position + offset
        elif whence == io.SEEK_END:
            target = self.size + offset
        else:
            raise ValueError("whence inválido")

        target = max(0, min(target, self.size))

        if target == self._position:
            return self._position

        if target == 0:
            self._init_stream(0)
        else:
            # Arbitrary jump: restart the engine at the new point
            # The MD5 hash will be marked as invalid in the next read() if it's not linear
            self._init_stream(target)
            if target != self._hash_cursor:
                self._md5_invalid = True

        return self._position

    def tell(self) -> int:
        self._ensure_not_closed()
        return self._position

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True


class FileVolume(TapeVolume):
    def __init__(self, path: Path, start_offset: int, end_offset: int, name: str):
        super().__init__(name, end_offset - start_offset)
        self.path = path
        self.start_offset = start_offset
        self.end_offset = end_offset
        self._file = None
        self._position = 0
        self._md5 = hashlib.md5()
        self._closed = True

    @property
    def is_completed(self) -> bool:
        return self._position == self.size

    def _ensure_not_closed(self):
        if self._closed:
            raise ValueError("I/O operation on closed file volume.")

    def read(self, size: int = -1) -> bytes:  # type: ignore
        if self._closed:
            raise ValueError("File already closed")

        remaining = self.size - self._position
        if remaining <= 0:
            return b""

        # Determinar cuánto leer (size=-1 significa todo)
        bytes_to_read = remaining if (size < 0) else min(size, remaining)
        if not self._file:
            raise RuntimeError("File not opened")

        chunk = self._file.read(bytes_to_read)
        if not chunk:
            return b""

        self._position += len(chunk)
        self._md5.update(chunk)

        return chunk

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._position + offset
        elif whence == io.SEEK_END:
            new_pos = self.size + offset
        else:
            raise ValueError("whence invalid")

        if new_pos < 0 or new_pos > self.size:
            raise ValueError("Seek out of bounds")

        self._position = new_pos
        if not self._file:
            raise RuntimeError("File not opened")
        self._file.seek(self.start_offset + new_pos)
        return self._position

    def tell(self) -> int:
        return self._position

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    @property
    def md5sum(self) -> str:
        if self._position < self.size:
            raise RuntimeError(
                f"MD5 not available: Incomplete read ({self._position}/{self.size})."
            )
        return self._md5.hexdigest()

    def close(self):
        self.__exit__(None, None, None)

    def open(self):
        self.__enter__()

    def __enter__(self):
        if self._closed:
            self._file = open(self.path, "rb")
            self._file.seek(self.start_offset)
            self._closed = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._closed and self._file:
            self._file.close()
            self._closed = True
