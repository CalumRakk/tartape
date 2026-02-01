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
    """
    Low-level TAR header builder.

    WHY NOT USE Python 'tarfile':
    The standard library is inconsistent with header sizes when
    mix large files (>8GB) with long paths (>100 chars), generating
    additional 'LongLink' blocks that break the 512-byte TarTape contract.

    This class ensures that, by splitting USTAR routes and encoding
    Base-256 sizes, each header measures EXACTLY 512 bytes.
    """

    def __init__(self, entry: "TarEntry"):
        self.buffer = bytearray(512)
        self.entry = entry

    def _split_path(self, path: str) -> tuple[str, str]:
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

    def set_size(self, size: int):
        """
        Write the size using hybrid strategy: USTAR (Octal) or GNU (Base-256).
        """
        OFFSET = 124
        FIELD_WIDTH = 12
        LIMIT_USTAR = 8589934591  # 8 GiB - 1 byte

        # Small/Normal File (USTAR Standard)
        if size <= LIMIT_USTAR:
            self.set_octal(OFFSET, FIELD_WIDTH, size)
            return

        # Giant File (GNU Base-256)
        # The GNU standard says: If the size > 8GB, set the first byte to 0x80 (128)
        # and use the remaining 11 bytes for the binary (Big-Endian) number.

        # Write the binary flag in the first byte
        self.buffer[OFFSET] = 0x80

        binary_length = FIELD_WIDTH - 1  # 11 bytes
        size_en_bytes = size.to_bytes(binary_length, byteorder="big")

        # We write those 11 bytes right after the 0x80 marker.
        # This covers offset 125 to 135.
        for i in range(len(size_en_bytes)):
            posicion = OFFSET + 1 + i
            self.buffer[posicion] = size_en_bytes[i]

    def set_string(self, offset: int, field_width: int, value: str):
        """Writes a UTF-8 encoded and truncated string to the buffer."""
        data = value.encode("utf-8")
        if len(data) > field_width:
            raise ValueError(
                f"{offset=} '{value}' too long for field ({len(data)} > {field_width})"
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

        if len(octal_string) > max_digits:
            raise ValueError(
                f"Number {value} too large for octal field width {field_width}"
            )

        padded_octal = octal_string.zfill(max_digits)
        final_string = padded_octal + "\0"
        self.buffer[offset : offset + field_width] = final_string.encode("ascii")

    def set_bytes(self, offset: int, value: bytes):
        """Writes raw bytes at a specific offset."""
        if offset + len(value) > 512:
            raise ValueError(f"Write overflow at offset {offset}")

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
        """Constructs a header for an entry."""
        # https://www.ibm.com/docs/en/zos/2.4.0?topic=formats-tar-format-tar-archives#taf__outar
        full_arcpath = self.entry.arc_path
        if self.entry.is_dir and not full_arcpath.endswith("/"):
            full_arcpath += "/"

        name, prefix = self._split_path(self.entry.arc_path)

        self.set_string(0, 100, name)
        # Prefix allows full path to reach 255 chars (155 prefix + 100 name)
        self.set_string(345, 155, prefix)

        self.set_octal(100, 8, self.entry.mode)
        self.set_octal(108, 8, self.entry.uid)
        self.set_octal(116, 8, self.entry.gid)
        self.set_size(self.entry.size)
        self.set_octal(136, 12, int(self.entry.mtime))
        # User/Group Names
        self.set_string(265, 32, self.entry.uname)
        self.set_string(297, 32, self.entry.gname)

        # TYPE FLAG: '0' = File, '5' = Dir, '2' = Symlink
        if self.entry.is_dir:
            type_flag = b"5"
        elif self.entry.is_symlink:
            type_flag = b"2"
            self.set_string(157, 100, self.entry.linkname)
        else:
            type_flag = b"0"

        self.set_bytes(156, type_flag)

        # USTAR Signature (Essential for the Prefix field to be recognized)
        self.set_string(257, 6, "ustar\0")
        self.set_string(263, 2, "00")

        self.calculate_checksum()
        header = bytes(self.buffer)
        if len(header) != 512:
            raise ValueError("Header is not 512 bytes long.")
        return header


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
        header = TarHeader(item)
        header_bytes = header.build()
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
