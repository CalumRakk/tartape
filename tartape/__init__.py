__version__ = "2.3.3"
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
    auto_truncate: bool = False,
) -> Tape:
    """Record a new tape.

    Args:
        directory: The root directory to record. Must be a directory.
        exclude: Patterns or a callable to skip specific files or directories.
        anonymize: If True, scrubs UID/GID and sets ownership to 'root'.
        calculate_hashes: If True, computes MD5 fingerprints for every file during discovery.
        overwrite: If True, deletes any existing .tartape directory before starting.
        auto_truncate: If True, automatically shortens path components exceeding 100 bytes
            using a deterministic hash to prevent ADR-005 violations.

    Returns:
        Tape: A ready-to-play object representing the frozen state of the directory.

    Raises:
        ValueError: If the provided directory is invalid.
        FileExistsError: If a tape already exists and overwrite is False.
        PathConstraintReportError: If paths violate USTAR limits and auto_truncate is False.
    """

    if not Path(directory).is_dir():
        raise ValueError(f"Root directory '{directory}' must be a directory.")

    if overwrite:
        metadata_dir = Path(directory) / TAPE_METADATA_DIR
        if metadata_dir.exists():
            shutil.rmtree(metadata_dir)

    recorder = TapeRecorder(directory, exclude, anonymize, calculate_hashes, auto_truncate)
    recorder.commit()
    return Tape(directory)


def discover(directory: str | Path) -> Optional[Path]:
    """
    Locate the absolute path to the TarTape index database if it exists.

    Args:
        directory: The root directory where the tape was recorded.

    Returns:
        Optional[Path]: The path to 'index.db' if found, otherwise None.

    Raises:
        NotADirectoryError: If the input path is not a valid directory.
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
    Check if a directory contains a valid and recorded TarTape index.

    Args:
        directory: The directory to inspect.

    Returns:
        bool: True if the '.tartape/index.db' exists, False otherwise.
    """
    if discover(directory):
        return True
    return False


def get_catalog(directory: str | Path) -> Catalog:
    """
    Open and retrieve the database catalog for a recorded tape.

    Args:
        directory: The root directory of the recorded tape.

    Returns:
        Catalog: An object to perform low-level queries on the tape metadata.

    Raises:
        FileNotFoundError: If the tape index does not exist in the given directory.
    """
    if not exists(directory):
        raise FileNotFoundError(f"The tape does not exist in: {directory}")

    db_path = discover(directory)
    assert db_path is not None, "Could not find database file"
    return Catalog(db_path)


def get_tape(directory: str | Path) -> Optional[Tape]:
    """
    Initialize a Tape object from an existing directory index.

    Args:
        directory: The root directory where the tape was recorded.

    Returns:
        Optional[Tape]: The Tape instance for streaming, or None if not recorded.
    """
    if exists(directory):
        return Tape(directory)


__all__ = ["Tape", "create", "discover", "exists", "get_catalog", "get_tape"]
