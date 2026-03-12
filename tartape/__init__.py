__version__ = "2.2.1"
__copyright__ = "Copyright (C) 2026-present CalumRakk <https://github.com/CalumRakk>"

import shutil
from pathlib import Path
from typing import Optional

from tartape.catalog import Catalog
from tartape.constants import TAPE_DB_NAME, TAPE_METADATA_DIR
from tartape.factory import ExcludeType
from tartape.recorder import TapeRecorder

from .tape import Tape


def create(
    directory: str | Path,
    exclude: Optional[ExcludeType] = None,
    anonymize: bool = True,
    calculate_hashes: bool = False,
    overwrite: bool = False,
) -> Tape:
    """Record a new tape and return the Tape object."""
    if not Path(directory).is_dir():
        raise ValueError(f"Root directory '{directory}' must be a directory.")

    if overwrite:
        metadata_dir = Path(directory) / TAPE_METADATA_DIR
        if metadata_dir.exists():
            shutil.rmtree(metadata_dir)

    recorder = TapeRecorder(directory, exclude, anonymize, calculate_hashes)
    recorder.commit()
    return Tape(directory)


def discover(directory: str | Path) -> Optional[Path]:
    """
    Automatically searches for an .tartape/index.db file in the given directory.
    """
    target_dir = Path(directory)
    if not target_dir.is_dir():
        raise NotADirectoryError(f"{directory} is not a valid directory.")

    candidate = target_dir / TAPE_METADATA_DIR / TAPE_DB_NAME
    if candidate.exists() and candidate.is_file():
        return candidate

    return None


def exists(directory: str | Path) -> bool:
    """
    Automatically searches for a .tartape file in the given directory.
    """
    if discover(directory):
        return True
    return False


def get_catalog(directory: str | Path) -> Catalog:
    if not exists(directory):
        raise FileNotFoundError(f"The tape does not exist in: {directory}")

    db_path = discover(directory)
    assert db_path is not None, "Could not find database file"
    return Catalog(db_path)


def get_tape(directory: str | Path) -> Optional[Tape]:
    if exists(directory):
        return Tape(directory)


__all__ = ["Tape", "create", "discover", "exists", "get_catalog", "get_tape"]
