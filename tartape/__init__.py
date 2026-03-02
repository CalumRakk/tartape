__version__ = "2.2.0b"
__copyright__ = "Copyright (C) 2026-present CalumRakk <https://github.com/CalumRakk>"

from .tape import Tape


def open(path: str) -> Tape:
    return Tape.open(path)


def create(path: str, **kwargs) -> Tape:
    return Tape.create(path, **kwargs)


__all__ = ["Tape", "open", "create"]
