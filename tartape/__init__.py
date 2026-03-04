__version__ = "2.2.0b"
__copyright__ = "Copyright (C) 2026-present CalumRakk <https://github.com/CalumRakk>"

from pathlib import Path
from typing import Optional

from tartape.factory import ExcludeType

from .tape import Tape


def open(path: str | Path) -> Tape:
    return Tape.open(path)


def create(
    path: str | Path,
    exclude: Optional[ExcludeType] = None,
    anonymize: bool = True,
    calculate_hashes: bool = False,
) -> Tape:
    return Tape.create(path, exclude, anonymize, calculate_hashes)


def exists(path: str | Path) -> bool:
    return Tape.exists(path)


__all__ = ["Tape", "open", "create"]
