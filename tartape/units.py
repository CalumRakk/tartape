"""Size parsing and canonical human-readable formatting utilities."""

import math
import re
from typing import Final

from tartape.constants import TAR_BLOCK_SIZE

# Multipliers in base 1024 (powers of 2)
UNIT_MULTIPLIERS: Final[dict[str, int]] = {
    "B": 1,
    "K": 1024,
    "KB": 1024,
    "KIB": 1024,
    "M": 1024**2,
    "MB": 1024**2,
    "MIB": 1024**2,
    "G": 1024**3,
    "GB": 1024**3,
    "GIB": 1024**3,
    "T": 1024**4,
    "TB": 1024**4,
    "TIB": 1024**4,
    "P": 1024**5,
    "PB": 1024**5,
    "PIB": 1024**5,
}

_SIZE_PATTERN: Final[re.Pattern] = re.compile(r"^([0-9]+(?:\.[0-9]+)?)([a-zA-Z]+)?$")


def parse_size(value: int | str) -> int:
    """Parse a size value into an exact byte count aligned to TAR blocks.

    Args:
        value: Size in bytes as an integer, or human string like '1GB', '500MB', '1.5GB'.

    Returns:
        int: The parsed byte count, guaranteed to be > 0 and divisible by 512.

    Raises:
        TypeError: If value is neither int nor str.
        ValueError: If value contains spaces, has an invalid unit, results in
            fractional bytes, is <= 0, or is not aligned to TAR_BLOCK_SIZE (512).
    """
    if isinstance(value, int):
        if value <= 0:
            raise ValueError(f"Volume size must be greater than 0, got {value}.")
        if value % TAR_BLOCK_SIZE != 0:
            raise ValueError(
                f"Volume size ({value} bytes) must be a multiple of "
                f"TAR block size ({TAR_BLOCK_SIZE} bytes)."
            )
        return value

    if not isinstance(value, str):
        raise TypeError(
            f"Size must be an integer or string, got {type(value).__name__}."
        )

    # Educational check for whitespace
    if re.search(r"\s", value):
        suggestion = re.sub(r"\s+", "", value)
        raise ValueError(
            f"Invalid size format '{value}'. Spaces are not allowed in TarTape "
            f"size strings. Did you mean '{suggestion}'?"
        )

    match = _SIZE_PATTERN.match(value)
    if not match:
        raise ValueError(
            f"Invalid size format '{value}'. Expected format like '100MB', '1GB', "
            "or an integer byte count."
        )

    number_str, unit_str = match.groups()
    unit = unit_str.upper() if unit_str else "B"

    if unit not in UNIT_MULTIPLIERS:
        valid_units = "B, KB, MB, GB, TB, PB"
        raise ValueError(
            f"Unknown size unit '{unit_str}' in '{value}'. Supported units: {valid_units}."
        )

    multiplier = UNIT_MULTIPLIERS[unit]
    raw_bytes = float(number_str) * multiplier

    # Ensure no fractional bytes
    if not math.isclose(raw_bytes, round(raw_bytes)):
        raise ValueError(f"Size '{value}' results in fractional bytes ({raw_bytes}).")

    size_bytes = round(raw_bytes)

    if size_bytes <= 0:
        raise ValueError(f"Volume size must be greater than 0, got {size_bytes}.")

    if size_bytes % TAR_BLOCK_SIZE != 0:
        raise ValueError(
            f"Volume size ({size_bytes} bytes from '{value}') must be a multiple "
            f"of TAR block size ({TAR_BLOCK_SIZE} bytes)."
        )

    return size_bytes


def format_size(size_bytes: int) -> str:
    """Format an integer byte count into a canonical human-readable string.

    Uses standard binary multipliers (1024) and produces clean outputs like
    '1GB', '500MB', '1.5GB', '512B'.

    Args:
        size_bytes: Byte count to format.

    Returns:
        str: Canonical formatted string (e.g., '1GB', '100MB').
    """
    if size_bytes <= 0:
        return f"{size_bytes}B"

    units = [
        ("TB", 1024**4),
        ("GB", 1024**3),
        ("MB", 1024**2),
        ("KB", 1024),
    ]

    for unit_name, unit_val in units:
        if size_bytes >= unit_val:
            # Check for exact integer quotient
            if size_bytes % unit_val == 0:
                return f"{size_bytes // unit_val}{unit_name}"

            # Check for clean decimal up to 2 decimal places
            ratio = size_bytes / unit_val
            rounded_ratio = round(ratio, 2)
            if round(rounded_ratio * unit_val) == size_bytes:
                # Format without trailing zeros (e.g., 1.5 instead of 1.50)
                formatted = f"{rounded_ratio:.2f}".rstrip("0").rstrip(".")
                return f"{formatted}{unit_name}"

    return f"{size_bytes}B"
