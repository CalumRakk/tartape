import hashlib
import io
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Generator, Iterable, Optional

if TYPE_CHECKING:
    from tartape.tape import Tape

from tartape.exceptions import (
    InvalidOffsetError,
    SourceNotFoundError,
    TarIntegrityError,
    VolumeStateError,
)
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
        checksum_algorithm: Optional[str] = None,
    ):
        self.directory = Path(directory)
        self.entries = entries
        self.total_tape_size = total_tape_size
        self.checksum_algorithm = (
            checksum_algorithm.lower() if checksum_algorithm else None
        )

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

            checksum_val: Optional[str] = None
            if entry.has_content:
                checksum_val = yield from self._stream_content_bytes(
                    entry, start_offset, effective_chunk_size
                )
                for pad_chunk in self._emit_padding_bytes(entry, start_offset):
                    yield pad_chunk

            end_event = self._create_event_end(entry, checksum_val)
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

            checksum_val: Optional[str] = None
            if entry.has_content:
                checksum_val = yield from self._stream_file_content_safely(
                    entry, start_offset, effective_chunk_size
                )
                yield from self._emit_padding(entry, start_offset)

            yield self._create_event_end(entry, checksum_val)
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

        hasher = None
        if local_skip == 0 and self.checksum_algorithm:
            try:
                hasher = hashlib.new(self.checksum_algorithm)
            except ValueError:
                hasher = hashlib.new(self.checksum_algorithm, usedforsecurity=False)

        try:
            with open(source_path, "rb") as f:
                if local_skip > 0:
                    f.seek(local_skip)

                while bytes_remaining > 0:
                    read_size = min(chunk_size, bytes_remaining)
                    chunk = f.read(read_size)
                    if not chunk:
                        raise TarIntegrityError(f"File shrunk: '{source_path}'")

                    if hasher:
                        hasher.update(chunk)
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

        return hasher.hexdigest() if hasher else None

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
        self, entry: ManifestEntry, checksum_val: Optional[str]
    ) -> TarFileEndEvent:
        return TarFileEndEvent(
            type="file_end",
            entry=entry,
            metadata=FileEndMetadata(
                checksum=checksum_val,
                end_offset=entry.global_window.end,
                is_complete=(checksum_val is not None),
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

        hasher = None
        if local_skip == 0 and self.checksum_algorithm:
            try:
                hasher = hashlib.new(self.checksum_algorithm)
            except ValueError:
                hasher = hashlib.new(self.checksum_algorithm, usedforsecurity=False)

        try:
            with open(source_path, "rb") as f:
                if local_skip > 0:
                    f.seek(local_skip)

                while bytes_remaining > 0:
                    read_size = min(chunk_size, bytes_remaining)
                    chunk = f.read(read_size)
                    if not chunk:
                        raise TarIntegrityError(f"File shrunk: '{source_path}'")

                    if hasher:
                        hasher.update(chunk)
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

        return hasher.hexdigest() if hasher else None

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
        checksum_algorithm: Optional[str] = "sha256",
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
        self.checksum_algorithm = (
            checksum_algorithm.lower() if checksum_algorithm else None
        )

        # Streaming state
        self._stream_gen = None
        self._position = 0
        self._buffer = bytearray()
        self._closed = True

        # In-flight integrity state
        self._hasher = None
        self._hash_cursor = 0
        self._integrity_broken = False
        self._final_checksum: Optional[str] = None

    def __repr__(self) -> str:
        return f"<Volume #{self.index} '{self.name}' ({self.size} bytes)>"

    @property
    def total_parts(self) -> Optional[int]:
        """Legacy alias for total_volumes."""
        return self.total_volumes

    def _ensure_not_closed(self):
        if self._closed:
            raise VolumeStateError("I/O operation on closed volume.")

    def _init_hasher(self):
        if not self.checksum_algorithm:
            self._hasher = None
            return

        try:
            self._hasher = hashlib.new(self.checksum_algorithm)
        except ValueError:
            self._hasher = hashlib.new(self.checksum_algorithm, usedforsecurity=False)

    def _init_stream(self, offset_in_volume: int):
        self._position = offset_in_volume
        self._buffer.clear()

        if self._stream_gen:
            self._stream_gen.close()

        if offset_in_volume == 0:
            self._init_hasher()
            self._hash_cursor = 0
            self._integrity_broken = False
        elif offset_in_volume != self._hash_cursor:
            self._integrity_broken = True
            logger.debug(
                f"Non-linear seek detected in {self.name}. "
                f"Position: {offset_in_volume}, Hash Cursor: {self._hash_cursor}. "
                "In-flight checksum disabled for this pass."
            )

        global_target = self.start_offset + offset_in_volume
        engine = TarStreamGenerator(
            self.manifest.entries,
            self.directory,
            total_tape_size=self.manifest.total_size,
            checksum_algorithm=self.checksum_algorithm,
        )
        self._stream_gen = engine.stream(start_offset=global_target)

    def _persist_checksum(self, checksum_val: str) -> None:
        """Safely persist calculated checksum to the catalog database."""
        if not self._catalog_path or not self.layout_tag:
            return

        catalog_file = Path(self._catalog_path)
        if not catalog_file.exists():
            return

        import sqlite3

        try:
            with sqlite3.connect(str(catalog_file), timeout=30.0) as conn:
                conn.execute(
                    "UPDATE volumes SET checksum = ?, checksum_algorithm = ? "
                    "WHERE layout_tag = ? AND volume_index = ?",
                    (
                        checksum_val,
                        self.checksum_algorithm,
                        self.layout_tag,
                        self.index,
                    ),
                )
                conn.commit()
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
            logger.debug(
                f"Skipping checksum persistence for volume {self.index} (catalog is read-only or busy): {e}"
            )
        except Exception as e:
            logger.warning(f"Failed to persist checksum for volume {self.index}: {e}")

    def _load_cached_checksum(self) -> Optional[str]:
        """Try loading previously persisted checksum from the catalog DB."""
        if not self._catalog_path or not self.layout_tag:
            return None

        catalog_file = Path(self._catalog_path)
        if not catalog_file.exists():
            return None

        import sqlite3

        try:
            with sqlite3.connect(
                f"file:{catalog_file.as_posix()}?mode=ro", uri=True, timeout=5.0
            ) as conn:
                cursor = conn.execute(
                    "SELECT checksum, checksum_algorithm FROM volumes "
                    "WHERE layout_tag = ? AND volume_index = ?",
                    (self.layout_tag, self.index),
                )
                row = cursor.fetchone()
                if row and row[0]:
                    if not self.checksum_algorithm and row[1]:
                        self.checksum_algorithm = row[1]
                    return row[0]
        except Exception:
            return None
        return None

    @property
    def checksum(self) -> Optional[str]:
        """Return the computed checksum if completed linearly, or cached from catalog.

        Does NOT perform disk I/O. Returns None if unread or interrupted.
        """
        if self._final_checksum:
            return self._final_checksum

        if (
            not self._integrity_broken
            and self._hash_cursor == self.size
            and self._hasher is not None
        ):
            self._final_checksum = self._hasher.hexdigest()
            self._persist_checksum(self._final_checksum)
            return self._final_checksum

        cached_val = self._load_cached_checksum()
        if cached_val:
            self._final_checksum = cached_val
            return self._final_checksum

        return None

    @property
    def md5sum(self) -> Optional[str]:
        """Legacy compatibility alias for checksum."""
        return self.checksum

    def compute_checksum(self, algorithm: Optional[str] = None) -> str:
        """Deliberately compute the volume checksum by reading from source files on disk.

        Raises:
            SourceNotFoundError: If source files are not accessible on disk.
        """
        if not self.directory.exists() or not self.directory.is_dir():
            raise SourceNotFoundError(
                f"Cannot compute checksum: source directory not found at '{self.directory}'"
            )

        algo = (algorithm or self.checksum_algorithm or "sha256").lower()
        try:
            hasher = hashlib.new(algo)
        except ValueError:
            hasher = hashlib.new(algo, usedforsecurity=False)

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

        digest = hasher.hexdigest()
        self._final_checksum = digest
        self.checksum_algorithm = algo
        self._persist_checksum(digest)
        return digest

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

        if (
            not self._integrity_broken
            and self._hash_cursor == self.size
            and self._hasher is not None
        ):
            if not self._final_checksum:
                self._final_checksum = self._hasher.hexdigest()
            self._persist_checksum(self._final_checksum)

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

        if not self._integrity_broken and self._hasher is not None:
            if self._position == self._hash_cursor:
                self._hasher.update(chunk)
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
