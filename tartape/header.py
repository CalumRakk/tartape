import logging

from .models import Track

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

    def __init__(self, entry: Track):
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
