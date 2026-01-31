import hashlib
import logging
import os
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


class TarHeader:
    def __init__(self):
        self.buffer = bytearray(512)

    def set_string(self, offset: int, field_width: int, value: str):
        """Writes a UTF-8 encoded and truncated string to the buffer."""
        data = value.encode("utf-8")
        if len(data) > field_width:
            raise ValueError(
                f"Value '{value}' too long for field ({len(data)} > {field_width})"
            )

        self.buffer[offset : offset + len(data)] = data

    def set_octal(self, offset: int, field_width: int, value: int):
        """
        Writes a number in octal format following the TAR standard:
        1. Converts the number to octal.
        2. Pads with leading zeros.
        3. Leaves space for the NULL terminator at the end.
        """
        # Convert integer to octal string (e.g., 511 -> '777')
        octal_string = oct(int(value))[2:]

        # TAR standard expects the field to end with a NULL byte (\0)
        # Therefore, available space for digits is field_width - 1
        max_digits = field_width - 1

        padded_octal = octal_string.zfill(max_digits)

        if len(padded_octal) > field_width:
            raise ValueError(
                f"Number {value} is too large for field width {field_width}"
            )

        final_string = padded_octal + "\0"
        data_bytes = final_string.encode("ascii")

        self.buffer[offset : offset + field_width] = data_bytes

    def set_bytes(self, offset: int, value: bytes):
        """Writes raw bytes at a specific offset."""
        self.buffer[offset : offset + len(value)] = value

    def calculate_checksum(self):
        """
        Calculates and writes the TAR header checksum (USTAR format).

        The checksum is a simple sum of the numeric values of the 512 bytes in the header.
        It is used strictly for basic header integrity verification.

        TAR Standard Rules:
        - The checksum field (offset 148, length 8 bytes) must be treated as if it
          contained ASCII spaces (value 32) during calculation.
        - The final value is stored as 6 octal digits, followed by a NULL byte and a space.
        """

        # Temporarily replace the 8 bytes with spaces (ASCII 32) per standard
        self.buffer[148:156] = b" " * 8

        # Calculate the sum of all 512 bytes
        total_sum = sum(self.buffer)

        # Format: 6 octal digits + NULL + Space
        octal_sum = oct(total_sum)[2:]
        octal_filled = octal_sum.zfill(6)
        final_string = octal_filled + "\0" + " "

        self.buffer[148:156] = final_string.encode("ascii")

    def build(self) -> bytes:
        self.calculate_checksum()
        return bytes(self.buffer)


class TarStreamGenerator:
    def __init__(
        self, entries: Iterable[TarEntry], chunk_size: int = CHUNK_SIZE_DEFAULT
    ):
        self.entries = entries
        self.chunk_size = chunk_size
        self._emitted_bytes = 0

    def _build_header(self, item: TarEntry) -> bytes:
        """Constructs a header for an entry based on USTAR format."""
        full_arcpath = item.arc_path
        if item.is_dir and not full_arcpath.endswith("/"):
            full_arcpath += "/"

        name, prefix = self._split_path(full_arcpath)

        h = TarHeader()
        h.set_string(0, 100, name)  # name
        h.set_octal(100, 8, item.mode)  # mode
        h.set_octal(108, 8, item.uid)  # uid
        h.set_octal(116, 8, item.gid)  # gid
        h.set_octal(124, 12, item.size)  # size
        h.set_octal(136, 12, int(item.mtime))  # mtime

        # TYPE FLAG: '0' = File, '5' = Dir, '2' = Symlink
        if item.is_symlink:
            type_flag = b"2"
        elif item.is_dir:
            type_flag = b"5"
        else:
            type_flag = b"0"

        h.set_bytes(156, type_flag)

        if item.is_symlink:
            h.set_string(157, 100, item.linkname)

        # USTAR Signature
        h.set_bytes(257, b"ustar\0")
        h.set_bytes(263, b"00")

        # User/Group Names
        h.set_string(265, 32, item.uname)
        h.set_string(297, 32, item.gname)

        # Prefix allows full path to reach 255 chars (155 prefix + 100 name)
        h.set_string(345, 155, prefix)
        return h.build()

    @staticmethod
    def _split_path(path: str) -> tuple[str, str]:
        """
        Splits a path to ensure USTAR compatibility.
        Limits: Name (100 bytes), Prefix (155 bytes).
        """
        LIMIT_NAME_BYTES = 100
        LIMIT_PREFIX_BYTES = 155
        SEPARATOR = "/"

        path_bytes = path.encode("utf-8")
        if len(path_bytes) <= LIMIT_NAME_BYTES:
            return path, ""

        # Find a '/' such that:
        # - Left part (prefix) <= 155 bytes
        # - Right part (name) <= 100 bytes
        best_split_index = -1
        path_length = len(path)

        for i in range(path_length):
            if path[i] == SEPARATOR:
                candidate_prefix = path[0:i]
                candidate_name = path[i + 1 :]

                prefix_size = len(candidate_prefix.encode("utf-8"))
                name_size = len(candidate_name.encode("utf-8"))

                if prefix_size <= LIMIT_PREFIX_BYTES and name_size <= LIMIT_NAME_BYTES:
                    best_split_index = i

        if best_split_index == -1:
            raise ValueError(
                f"Path is too long or cannot be split to fit USTAR limits: '{path}'"
            )

        return path[best_split_index + 1 :], path[0:best_split_index]

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
