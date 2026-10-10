import hashlib
import io
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Generator, Iterable, Optional

if TYPE_CHECKING:
    from tartape.tape import Tape

from tartape.exceptions import InvalidOffsetError, TarIntegrityError, VolumeStateError
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
    TarObserver,
    TarTapeCompletedEvent,
    VolumeManifest,
)

logger = logging.getLogger(__name__)


class TarStreamGenerator:
    """Deterministic TAR byte stream generator with in-band telemetry dispatch."""

    def __init__(
        self,
        entries: Iterable[ManifestEntry],
        directory: str | Path,
        total_tape_size: int | None = None,
    ):
        self.directory = Path(directory)
        self.entries = entries
        self.total_tape_size = total_tape_size

    def _dispatch(
        self,
        event: TarEvent,
        on_event: Optional[Callable[[TarEvent], None]] = None,
        observer: Optional[TarObserver] = None,
    ) -> None:
        """Synchronously notify functional callbacks and observer objects."""
        if on_event:
            on_event(event)
        if observer:
            if event.type == "file_start" and hasattr(observer, "on_file_start"):
                observer.on_file_start(event)
            elif event.type == "file_end" and hasattr(observer, "on_file_end"):
                observer.on_file_end(event)
            elif event.type == "tape_completed" and hasattr(
                observer, "on_tape_completed"
            ):
                observer.on_tape_completed(event)

    def stream_bytes(
        self,
        start_offset: int = 0,
        chunk_size: int | None = None,
        on_event: Optional[Callable[[TarEvent], None]] = None,
        observer: Optional[TarObserver] = None,
    ) -> Generator[bytes, None, None]:
        """Stream raw TAR bytes directly while dispatching lifecycle events in-band."""
        effective_chunk_size = chunk_size or CHUNK_SIZE_DEFAULT
        last_offset = 0

        for entry in self.entries:
            if start_offset >= entry.global_window.end:
                last_offset = entry.global_window.end
                continue

            start_event = self._create_event_start(entry, start_offset)
            self._dispatch(start_event, on_event, observer)

            yield from self._emit_header_bytes(entry, start_offset)

            md5_hash: Optional[str] = None
            if entry.has_content:
                md5_hash = yield from self._stream_content_bytes(
                    entry, start_offset, effective_chunk_size
                )
                for pad_chunk in self._emit_padding_bytes(entry, start_offset):
                    yield pad_chunk

            end_event = self._create_event_end(entry, md5_hash)
            self._dispatch(end_event, on_event, observer)
            last_offset = entry.global_window.end

        footer_start = (
            self.total_tape_size - TAR_FOOTER_SIZE
            if self.total_tape_size is not None
            else last_offset
        )
        for footer_chunk in self._emit_footer_bytes(start_offset, footer_start):
            yield footer_chunk

        completed_event = TarTapeCompletedEvent(type="tape_completed")
        self._dispatch(completed_event, on_event, observer)

    def stream(
        self, start_offset: int = 0, chunk_size: int | None = None
    ) -> Generator[TarEvent, None, None]:
        """Legacy generator emitting TarEvent wrappers."""
        effective_chunk_size = chunk_size or CHUNK_SIZE_DEFAULT
        last_offset = 0

        for entry in self.entries:
            if start_offset >= entry.global_window.end:
                last_offset = entry.global_window.end
                continue

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

        footer_start = (
            self.total_tape_size - TAR_FOOTER_SIZE
            if self.total_tape_size is not None
            else last_offset
        )
        yield from self._emit_stream_gen_footer(start_offset, footer_start)
        yield TarTapeCompletedEvent(type="tape_completed")

    def _emit_header_bytes(
        self, entry: ManifestEntry, global_skip: int
    ) -> Generator[bytes, None, None]:
        local_skip, bytes_to_send = self._get_stream_window(
            global_skip, entry.global_window.start, TAR_BLOCK_SIZE
        )
        if bytes_to_send > 0:
            yield self._build_header(entry)[local_skip:]

    def _stream_content_bytes(
        self, entry: ManifestEntry, global_skip: int, chunk_size: int
    ) -> Generator[bytes, None, Optional[str]]:
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
                    yield chunk

                if local_skip == 0:
                    extra = f.read(1)
                    if extra:
                        raise TarIntegrityError(
                            f"File grew: '{source_path}'. Bytes left: {extra}"
                        )
        except OSError as e:
            raise TarIntegrityError(f"Error reading {source_path}") from e

        return md5.hexdigest() if md5 else None

    def _emit_padding_bytes(
        self, entry: ManifestEntry, global_skip: int
    ) -> Generator[bytes, None, None]:
        padding_total = entry.global_window.end - entry.content_end_offset
        _, bytes_to_send = self._get_stream_window(
            global_skip, entry.content_end_offset, padding_total
        )
        if bytes_to_send > 0:
            yield b"\0" * bytes_to_send

    def _emit_footer_bytes(
        self, global_skip: int, footer_start: int
    ) -> Generator[bytes, None, None]:
        _, bytes_to_send = self._get_stream_window(
            global_skip, footer_start, TAR_FOOTER_SIZE
        )
        if bytes_to_send > 0:
            yield b"\0" * bytes_to_send

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

    def _get_stream_window(
        self, global_skip: int, block_start: int, block_length: int
    ) -> tuple[int, int]:
        block_end = block_start + block_length
        if global_skip >= block_end:
            return 0, 0
        local_skip = max(0, global_skip - block_start)
        bytes_to_send = block_length - local_skip
        return local_skip, bytes_to_send

    def _emit_header(
        self, entry: ManifestEntry, global_skip: int
    ) -> Generator[TarEvent, None, None]:
        for chunk in self._emit_header_bytes(entry, global_skip):
            yield TarFileDataEvent(type="file_data", data=chunk)

    def _stream_file_content_safely(
        self, entry: ManifestEntry, global_skip: int, chunk_size: int
    ) -> Generator[TarEvent, None, Optional[str]]:
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
        for chunk in self._emit_padding_bytes(entry, global_skip):
            yield TarFileDataEvent(type="file_data", data=chunk)

    def _emit_stream_gen_footer(
        self, global_skip: int, footer_start: int
    ) -> Generator[TarEvent, None, None]:
        for chunk in self._emit_footer_bytes(global_skip, footer_start):
            yield TarFileDataEvent(type="file_data", data=chunk)


class TapeStreamReader(io.BufferedIOBase):
    """File-like wrapper exposing the entire virtual TAR stream as an io.BufferedIOBase."""

    def __init__(self, tape: "Tape", buffer_size: int = CHUNK_SIZE_DEFAULT):
        self._tape = tape
        self._buffer_size = buffer_size
        self._position = 0
        self._buffer = bytearray()
        self._generator: Optional[Generator[bytes, None, None]] = None
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise VolumeStateError("I/O operation on closed tape stream reader.")

    def _init_generator(self, start_offset: int) -> None:
        self._position = start_offset
        self._buffer.clear()
        if self._generator:
            self._generator.close()
        self._generator = self._tape.play(
            start_offset=start_offset, buffer_size=self._buffer_size
        )

    def read(self, size: int = -1) -> bytes:  # type: ignore
        self._ensure_open()
        if self._position >= self._tape.total_size:
            return b""

        if self._generator is None:
            self._init_generator(self._position)

        remaining_in_tape = self._tape.total_size - self._position
        bytes_to_read = (
            remaining_in_tape
            if (size is None or size < 0)
            else min(size, remaining_in_tape)
        )

        while len(self._buffer) < bytes_to_read:
            try:
                assert self._generator is not None
                chunk = next(self._generator)
                self._buffer.extend(chunk)
            except StopIteration:
                break

        actual_bytes = min(bytes_to_read, len(self._buffer))
        chunk = bytes(self._buffer[:actual_bytes])
        self._buffer = self._buffer[actual_bytes:]
        self._position += len(chunk)
        return chunk

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._ensure_open()
        if whence == io.SEEK_SET:
            target = offset
        elif whence == io.SEEK_CUR:
            target = self._position + offset
        elif whence == io.SEEK_END:
            target = self._tape.total_size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")

        if target < 0 or target > self._tape.total_size:
            raise InvalidOffsetError(
                f"Seek offset {target} out of bounds (0 - {self._tape.total_size})"
            )

        if target != self._position:
            self._init_generator(target)
        return self._position

    def tell(self) -> int:
        self._ensure_open()
        return self._position

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def close(self) -> None:
        if not self._closed:
            if self._generator:
                self._generator.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Volume(io.BufferedIOBase):
    """First-class file-like object representing a deterministic logical TAR slice."""

    def __init__(
        self,
        directory: Path,
        manifest: "VolumeManifest",
        name: str,
        layout_tag: Optional[str] = None,
        total_volumes: Optional[int] = None,
        catalog_path: Optional[Path] = None,
    ):
        self.directory = Path(directory)
        self.manifest = manifest
        self.name = name
        self.size = manifest.chunk_size
        self.index = manifest.volume_index
        self.start_offset = manifest.start_offset
        self.end_offset = manifest.end_offset
        self.layout_tag = layout_tag
        self.total_volumes = total_volumes
        self._catalog_path = catalog_path

        # Streaming state
        self._stream_gen = None
        self._position = 0
        self._buffer = bytearray()
        self._closed = True

        # Integrity & hashing state
        self._md5 = hashlib.md5()
        self._hash_cursor = 0
        self._integrity_broken = False
        self._final_md5: Optional[str] = None

    def __repr__(self) -> str:
        return f"<Volume #{self.index} '{self.name}' ({self.size} bytes)>"

    @property
    def total_parts(self) -> Optional[int]:
        """Legacy alias for total_volumes."""
        return self.total_volumes

    def _ensure_not_closed(self):
        if self._closed:
            raise VolumeStateError("I/O operation on closed volume.")

    def _init_stream(self, offset_in_volume: int):
        self._position = offset_in_volume
        self._buffer.clear()

        if self._stream_gen:
            self._stream_gen.close()

        if offset_in_volume == 0:
            self._md5 = hashlib.md5()
            self._hash_cursor = 0
            self._integrity_broken = False
        elif offset_in_volume != self._hash_cursor:
            self._integrity_broken = True
            logger.warning(
                f"Non-linear seek detected in {self.name}. "
                f"Position: {offset_in_volume}, Hash Cursor: {self._hash_cursor}. "
                "Linear MD5 calculation disabled for this pass."
            )

        global_target = self.start_offset + offset_in_volume
        engine = TarStreamGenerator(
            self.manifest.entries,
            self.directory,
            total_tape_size=self.manifest.total_size,
        )
        self._stream_gen = engine.stream(start_offset=global_target)

    def _calculate_manually(self) -> str:
        hasher = hashlib.md5()
        engine = TarStreamGenerator(
            self.manifest.entries,
            self.directory,
            total_tape_size=self.manifest.total_size,
        )
        stream = engine.stream(start_offset=self.start_offset)

        bytes_hashed = 0
        for event in stream:
            if bytes_hashed >= self.size:
                break

            if event.type == "file_data":
                data = event.data
                remaining_in_volume = self.size - bytes_hashed

                if len(data) > remaining_in_volume:
                    data = data[:remaining_in_volume]

                hasher.update(data)
                bytes_hashed += len(data)

        return hasher.hexdigest()

    def _persist_md5(self, md5_val: str) -> None:
        """Atomically persist calculated MD5 to the catalog database in a thread-safe manner."""
        if not self._catalog_path or not self.layout_tag:
            return

        catalog_file = Path(self._catalog_path)
        if not catalog_file.exists():
            return

        import sqlite3

        try:
            with sqlite3.connect(str(catalog_file), timeout=30.0) as conn:
                conn.execute(
                    "UPDATE volumes SET md5sum = ? WHERE layout_tag = ? AND volume_index = ?",
                    (md5_val, self.layout_tag, self.index),
                )
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to persist MD5 for volume {self.index}: {e}")

    @property
    def md5sum(self) -> str:
        """Return the MD5 hash, updating database cache if calculated linearly."""
        if self._final_md5:
            return self._final_md5

        if not self._integrity_broken and self._hash_cursor == self.size:
            self._final_md5 = self._md5.hexdigest()
            self._persist_md5(self._final_md5)
            return self._final_md5

        self._final_md5 = self._calculate_manually()
        self._persist_md5(self._final_md5)
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

        if not self._integrity_broken and self._hash_cursor == self.size:
            if not self._final_md5:
                self._final_md5 = self._md5.hexdigest()
            self._persist_md5(self._final_md5)

    def open(self):
        self.__enter__()
        return self

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
                    raise RuntimeError("Tape stream generator not initialized.")
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
            if self._position == self._hash_cursor:
                self._md5.update(chunk)
                self._hash_cursor += len(chunk)
            else:
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
            raise InvalidOffsetError(
                f"Seek position {target} is out of bounds (0-{self.size})"
            )

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


# Legacy aliases for backwards compatibility
FolderVolume = Volume
TapeVolume = Volume
