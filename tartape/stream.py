import hashlib
import io
import logging
from pathlib import Path
from typing import Generator, Iterable, Optional

from tartape.exceptions import TarIntegrityError
from tartape.factory import validate_integrity
from tartape.header import TarHeader

from .constants import CHUNK_SIZE_DEFAULT, TAR_BLOCK_SIZE, TAR_FOOTER_SIZE
from .schemas import (
    FileEndMetadata,
    FileStartMetadata,
    ManifestEntry,
    TarEvent,
    TarFileDataEvent,
    TarFileEndEvent,
    TarFileStartEvent,
    TarTapeCompletedEvent,
    VolumeManifest,
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
    def __init__(self, entries: Iterable[ManifestEntry], directory: str | Path):
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
            if start_offset >= entry.global_window.end:
                last_offset = entry.global_window.end
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
            last_offset = entry.global_window.end

        yield from self._emit_stream_gen_footer(start_offset, last_offset)
        yield TarTapeCompletedEvent(type="tape_completed")
        logger.info("TAR stream completed successfully.")

    def _build_header(self, entry: ManifestEntry) -> bytes:
        header = TarHeader(entry.info)
        return header.build()

    def _create_event_start(
        self, entry: ManifestEntry, global_skip: int
    ) -> TarFileStartEvent:
        is_resumed = global_skip > entry.global_window.start
        return TarFileStartEvent(
            type="file_start",
            entry=entry,
            metadata=FileStartMetadata(
                start_offset=entry.global_window.start, resumed=is_resumed
            ),
        )

    def _create_event_end(
        self, entry: ManifestEntry, md5: Optional[str]
    ) -> TarFileEndEvent:
        return TarFileEndEvent(
            type="file_end",
            entry=entry,
            metadata=FileEndMetadata(
                md5sum=md5,
                end_offset=entry.global_window.end,
                is_complete=(md5 is not None),
            ),
        )

    def _emit_header(
        self, entry: ManifestEntry, global_skip: int
    ) -> Generator[TarEvent, None, None]:
        local_skip, bytes_to_send = self._get_stream_window(
            global_skip, entry.global_window.start, TAR_BLOCK_SIZE
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
        self, entry: ManifestEntry, global_skip: int, chunk_size: int
    ) -> Generator[TarEvent, None, Optional[str]]:
        """Safely stream file content, ensuring that we do not read past the end of the file."""

        local_skip, bytes_remaining = self._get_stream_window(
            global_skip, entry.header_end_offset, entry.info.size
        )

        if bytes_remaining <= 0:
            return None

        source_path = entry.get_absolute_path(self.directory)
        validate_integrity(entry.info, self.directory)
        md5 = hashlib.md5() if local_skip == 0 else None

        try:
            with open(source_path, "rb") as f:
                if local_skip > 0:
                    f.seek(local_skip)

                while bytes_remaining > 0:
                    read_size = min(chunk_size, bytes_remaining)
                    chunk = f.read(read_size)
                    if not chunk:
                        raise TarIntegrityError(f"File shrunk: '{source_path}'")

                    if md5:
                        md5.update(chunk)
                    bytes_remaining -= len(chunk)
                    yield TarFileDataEvent(type="file_data", data=chunk)

                if local_skip == 0:
                    extra = f.read(1)
                    if extra:
                        raise TarIntegrityError(
                            f"File grew: '{source_path}'. Bytes left: {extra}"
                        )

        except OSError as e:
            raise TarIntegrityError(f"Error reading {source_path}") from e

        return md5.hexdigest() if md5 else None

    def _emit_padding(
        self, entry: ManifestEntry, global_skip: int
    ) -> Generator[TarEvent, None, None]:
        # Padding starts where the data ends and ends at end_offset
        padding_total = entry.global_window.end - entry.content_end_offset

        _, bytes_to_send = self._get_stream_window(
            global_skip, entry.content_end_offset, padding_total
        )
        if bytes_to_send > 0:
            yield TarFileDataEvent(type="file_data", data=b"\0" * bytes_to_send)

    def _emit_stream_gen_footer(
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
        manifest: "VolumeManifest",
        name: str,
    ):
        super().__init__(name, manifest.chunk_size)
        self.directory = directory
        self.manifest = manifest
        self.start_offset = manifest.start_offset
        self.end_offset = manifest.end_offset

        # Streaming state
        self._stream_gen = None
        self._position = 0
        self._buffer = bytearray()
        self._closed = True

        # Integrity/Hashing state
        self._md5 = hashlib.md5()
        self._hash_cursor = 0
        self._integrity_broken = False
        self._final_md5 = None

    def _ensure_not_closed(self):
        if self._closed:
            raise ValueError("I/O operation on closed volume.")

    def _init_stream(self, offset_in_volume: int):
        """
        Initializes or restarts the stream generator at a specific offset.
        """
        self._position = offset_in_volume
        self._buffer.clear()

        if self._stream_gen:
            self._stream_gen.close()

        if offset_in_volume == 0:
            self._md5 = hashlib.md5()
            self._hash_cursor = 0
            self._integrity_broken = False
            logger.debug(f"Linear hash initialized for volume {self.name}")

        # If the seek target doesn't match where our hashing left off,
        # we mark the linear hash as compromised.
        elif offset_in_volume != self._hash_cursor:
            self._integrity_broken = True
            logger.warning(
                f"Non-linear seek detected in {self.name}. "
                f"Position: {offset_in_volume}, Hash Cursor: {self._hash_cursor}. "
                "Linear MD5 calculation is now disabled for this pass."
            )

        global_target = self.start_offset + offset_in_volume
        engine = TarStreamGenerator(self.manifest.entries, self.directory)
        self._stream_gen = engine.stream(start_offset=global_target)

    def _calculate_manually(self) -> str:
        logger.info(f"Performing manual MD5 pass for volume: {self.name}")
        hasher = hashlib.md5()

        engine = TarStreamGenerator(self.manifest.entries, self.directory)
        stream = engine.stream(start_offset=self.start_offset)

        bytes_hashed = 0
        for event in stream:
            if bytes_hashed >= self.size:
                break

            if event.type == "file_data":
                data = event.data
                remaining_in_volume = self.size - bytes_hashed

                # If the current chunk exceeds the volume boundary, we slice it
                if len(data) > remaining_in_volume:
                    data = data[:remaining_in_volume]

                hasher.update(data)
                bytes_hashed += len(data)

        return hasher.hexdigest()

    @property
    def md5sum(self) -> str:
        """
        Returns the MD5 hash. Uses the linear calculation if possible,
        otherwise falls back to manual calculation.
        """
        if self._final_md5:
            return self._final_md5

        if not self._integrity_broken and self._hash_cursor == self.size:
            self._final_md5 = self._md5.hexdigest()
            return self._final_md5

        self._final_md5 = self._calculate_manually()
        return self._final_md5

    @property
    def is_completed(self) -> bool:
        return self._position == self.size

    def __enter__(self):
        self._closed = False
        self._init_stream(0)
        return self

    def __exit__(self, *args):
        if self._stream_gen:
            self._stream_gen.close()
        self._closed = True

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
                if not self._stream_gen:
                    raise
                event = next(self._stream_gen)
                if event.type == "file_data":
                    self._buffer.extend(event.data)
            except StopIteration:
                if self._stream_gen:
                    self._stream_gen.close()
                break

        chunk_size = min(bytes_to_read, len(self._buffer))
        chunk = bytes(self._buffer[:chunk_size])
        self._buffer = self._buffer[chunk_size:]

        if not self._integrity_broken:
            # If the read cursor matches the hash cursor, we continue processing
            if self._position == self._hash_cursor:
                self._md5.update(chunk)
                self._hash_cursor += len(chunk)
            else:
                # The developer read something out of linear order
                self._integrity_broken = True

        self._position += len(chunk)
        return chunk

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._ensure_not_closed()

        if whence == io.SEEK_SET:
            target = offset
        elif whence == io.SEEK_CUR:
            target = self._position + offset
        elif whence == io.SEEK_END:
            target = self.size + offset
        else:
            raise ValueError("Invalid whence")

        if target < 0 or target > self.size:
            raise ValueError(f"Seek position {target} is out of bounds (0-{self.size})")

        if target == self._position:
            return self._position

        self._init_stream(target)
        return self._position

    def tell(self) -> int:
        self._ensure_not_closed()
        return self._position

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True
