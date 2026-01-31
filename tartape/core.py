import hashlib
import logging
import os
import tarfile
from typing import Generator, Iterable, Optional

from .constants import CHUNK_SIZE_DEFAULT, TAR_BLOCK_SIZE, TAR_FOOTER_SIZE
from .enums import TarEventType
from .schemas import (
    FileEndMetadata,
    FileStartMetadata,
    TarEntry,
    TarEvent,
    TarFileDataEvent,
    TarFileEndEvent,
    TarFileStartEvent,
    TarTapeCompletedEvent,
)

logger = logging.getLogger(__name__)


class TarStreamGenerator:
    def __init__(
        self, entries: Iterable[TarEntry], chunk_size: int = CHUNK_SIZE_DEFAULT
    ):
        self.entries = entries
        self.chunk_size = chunk_size
        self._emitted_bytes = 0

    def _build_header(self, item: TarEntry) -> bytes:
        """
        Build the header using the data from TarEntry.
        """
        info = item.as_tarinfo()
        header_bytes = info.tobuf(format=tarfile.GNU_FORMAT)

        # If for some reason the path or metadata forces Python to
        # generate extra blocks (like GNU LongLinks), the size will be > 512.
        # This would violate our mathematical determinism contract.
        if len(header_bytes) != 512:
            raise RuntimeError(
                f"Inventory corruption: Invalid header for {item.arc_path}"
            )

        return header_bytes

    def stream(self) -> Generator[TarEvent, None, None]:
        logger.info("Starting TAR stream.")

        for entry in self.entries:
            # Announce start of file
            yield self._create_event_start(entry)

            # Tar header (512 bytes)
            yield self._emit_header(entry)

            # Process the TAR Body (only if applicable)
            md5_hash: Optional[str] = None
            if self._entry_has_content(entry):
                md5_hash = yield from self._stream_file_content_safely(entry)
                yield from self._emit_padding(entry.size)

            # Finish the file
            yield self._create_event_end(entry, md5_hash)

        # Close the tape: standard tar
        yield from self._emit_tape_footer()

        yield TarTapeCompletedEvent(type=TarEventType.TAPE_COMPLETED)
        logger.info("TAR stream completed successfully.")

    def _entry_has_content(self, entry: TarEntry) -> bool:
        """Only regular archives have a body in TAR format."""
        return not entry.is_dir and not entry.is_symlink

    def _create_event_start(self, entry: TarEntry) -> TarFileStartEvent:
        return TarFileStartEvent(
            type=TarEventType.FILE_START,
            entry=entry,
            metadata=FileStartMetadata(start_offset=self._emitted_bytes),
        )

    def _emit_header(self, entry: TarEntry) -> TarFileDataEvent:
        header_bytes = self._build_header(entry)
        self._emitted_bytes += len(header_bytes)
        return TarFileDataEvent(
            type=TarEventType.FILE_DATA, data=header_bytes, entry=entry
        )

    def _stream_file_content_safely(
        self, entry: TarEntry
    ) -> Generator[TarEvent, None, str]:
        """
        It handles physical reading, integrity validation (ADR-002), and MD5 calculation.
        It returns the MD5 hash upon completion.
        """

        self._validate_snapshot_integrity(entry)

        md5 = hashlib.md5()
        bytes_remaining = entry.size
        try:
            with open(entry.source_path, "rb") as f:
                while bytes_remaining > 0:
                    read_size = min(self.chunk_size, bytes_remaining)
                    chunk = f.read(read_size)

                    if not chunk:
                        raise RuntimeError(
                            f"File shrunk during read: '{entry.source_path}'. "
                            f"Missing {bytes_remaining} bytes."
                        )

                    md5.update(chunk)
                    self._emitted_bytes += len(chunk)
                    bytes_remaining -= len(chunk)

                    yield TarFileDataEvent(
                        type=TarEventType.FILE_DATA, data=chunk, entry=entry
                    )

                # Did file grow during reading?
                # Try to read 1 extra byte. If successful, the file is bigger than promised.
                if f.read(1):
                    raise RuntimeError(
                        f"File grew during read: '{entry.source_path}'. "
                        f"Content exceeds promised size."
                    )

        except OSError as e:
            raise RuntimeError(f"Error reading file {entry.source_path}") from e

        return md5.hexdigest()

    def _validate_snapshot_integrity(self, entry: TarEntry):
        """
        Strict implementation of ADR-002.
        Verify that the file on disk matches the inventory.
        """
        try:
            st = os.stat(entry.source_path)
        except OSError as e:
            raise RuntimeError(f"File inaccessible: {entry.source_path}") from e

        # Mtime Consistency
        # Using a tiny epsilon for float comparison safety
        if abs(st.st_mtime - entry.mtime) > 1e-6:
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

    def _emit_padding(self, size: int) -> Generator[TarEvent, None, None]:
        padding_size = (TAR_BLOCK_SIZE - (size % TAR_BLOCK_SIZE)) % TAR_BLOCK_SIZE
        if padding_size > 0:
            padding = b"\0" * padding_size
            self._emitted_bytes += len(padding)
            yield TarFileDataEvent(type=TarEventType.FILE_DATA, data=padding)

    def _create_event_end(self, entry: TarEntry, md5: Optional[str]) -> TarFileEndEvent:
        return TarFileEndEvent(
            type=TarEventType.FILE_END,
            entry=entry,
            metadata=FileEndMetadata(
                md5sum=md5,
                end_offset=self._emitted_bytes,
            ),
        )

    def _emit_tape_footer(self) -> Generator[TarEvent, None, None]:
        footer = b"\0" * TAR_FOOTER_SIZE
        self._emitted_bytes += len(footer)
        yield TarFileDataEvent(type=TarEventType.FILE_DATA, data=footer)
