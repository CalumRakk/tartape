__version__ = "3.0.0"
__copyright__ = "Copyright (C) 2026-present CalumRakk <https://github.com/CalumRakk>"

from pathlib import Path
from typing import Optional

from tartape.catalog import Catalog, Layout
from tartape.constants import TAPE_DB_NAME, TAPE_EXTENSION, TAPE_METADATA_DIR
from tartape.exceptions import (
    AmbiguousLayoutError,
    InvalidOffsetError,
    LayoutNotFoundError,
    PathConstraintError,
    PathConstraintReportError,
    TapeCorruptedError,
    TapeNotFoundError,
    TapeVerificationError,
    TarIntegrityError,
    TarTapeError,
    VolumeChecksumMismatchError,
    VolumeNotFoundError,
    VolumeStateError,
)
from tartape.factory import ExcludeType
from tartape.recorder import TapeRecorder
from tartape.schemas import FileGPS, FileSlice, ManifestEntry, TarEvent, TarObserver
from tartape.stream import Volume
from tartape.tape import Tape


def record(
    directory: str | Path,
    catalog_path: Optional[str | Path] = None,
    exclude: Optional[ExcludeType] = None,
    anonymize: bool = True,
    calculate_hashes: bool = False,
    overwrite: bool = False,
    auto_truncate: bool = False,
) -> Path:
    """Record an immutable T0 catalog snapshot of a directory.

    Builds an optimized, standalone .tartape sidecar catalog without modifying
    the target directory. Supports read-only filesystems when catalog_path is provided.

    Args:
        directory: The root directory to scan and record.
        catalog_path: Optional destination path for the sidecar file. Defaults to
            `<directory.parent>/<directory.name>.tartape`.
        exclude: Patterns or callable to skip specific files or directories.
        anonymize: If True, scrubs UID/GID and sets ownership to 'root'.
        calculate_hashes: If True, computes MD5 hashes for all files during scan.
        overwrite: If True, replaces existing catalog file at destination.
        auto_truncate: If True, automatically shortens components exceeding 100 bytes.

    Returns:
        Path: The absolute path to the generated .tartape catalog file.
    """
    recorder = TapeRecorder(
        directory=directory,
        catalog_path=catalog_path,
        exclude=exclude,
        anonymize=anonymize,
        calculate_hashes=calculate_hashes,
        overwrite=overwrite,
        auto_truncate=auto_truncate,
    )
    recorder.commit()
    return recorder.catalog_path


def open(
    directory: str | Path,
    catalog_path: Optional[str | Path] = None,
) -> Tape:
    """Open an existing recorded tape for streaming and volume access.

    Args:
        directory: The root directory of the recorded tape.
        catalog_path: Optional path to an external .tartape catalog sidecar file.

    Returns:
        Tape: A Tape instance ready for streaming, volume slicing, or inspection.

    Raises:
        TapeNotFoundError: If no TarTape catalog is found.
    """
    dir_path = Path(directory)
    if catalog_path is not None:
        cat_file = Path(catalog_path)
        if not cat_file.exists():
            raise TapeNotFoundError(f"Catalog file not found at: {catalog_path}")
        return Tape(dir_path, catalog_path=cat_file)

    if not exists(dir_path):
        raise TapeNotFoundError(f"No TarTape catalog found for: {directory}")

    return Tape(dir_path)


def create(
    directory: str | Path,
    exclude: Optional[ExcludeType] = None,
    anonymize: bool = True,
    calculate_hashes: bool = False,
    overwrite: bool = False,
    auto_truncate: bool = False,
) -> Tape:
    """Legacy alias for backward compatibility. Records a sidecar and returns Tape."""
    _ = record(
        directory=directory,
        exclude=exclude,
        anonymize=anonymize,
        calculate_hashes=calculate_hashes,
        overwrite=overwrite,
        auto_truncate=auto_truncate,
    )
    return Tape(directory)


def discover(directory: str | Path) -> Optional[Path]:
    """Locate the catalog path for a given directory (checking sidecar first, then legacy)."""
    target_dir = Path(directory)
    if not target_dir.is_dir():
        return None

    # 1. Primary: Sidecar file alongside the folder
    sidecar = target_dir.parent / f"{target_dir.name}{TAPE_EXTENSION}"
    if sidecar.exists() and sidecar.is_file():
        return sidecar

    # 2. Fallback: Legacy .tartape/index.db inside the folder
    legacy = target_dir / TAPE_METADATA_DIR / TAPE_DB_NAME
    if legacy.exists() and legacy.is_file():
        return legacy

    return None


def exists(directory: str | Path, catalog_path: Optional[str | Path] = None) -> bool:
    """Check if a directory has a recorded TarTape catalog."""
    if catalog_path is not None:
        return Path(catalog_path).exists()
    return discover(directory) is not None


def get_catalog(directory: str | Path) -> Catalog:
    """Open and retrieve the catalog for a recorded tape."""
    db_path = discover(directory)
    if db_path is None:
        raise TapeNotFoundError(f"No TarTape catalog found for: {directory}")
    return Catalog(db_path)


def get_tape(directory: str | Path) -> Optional[Tape]:
    """Initialize a Tape object from an existing directory catalog."""
    if exists(directory):
        return Tape(directory)
    return None


def open_catalog(catalog_path: str | Path) -> Catalog:
    """Open a standalone .tartape catalog file in offline/zero-disk mode.

    Does not require the original source directory to exist on disk.
    """
    path = Path(catalog_path)
    if not path.exists():
        raise TapeNotFoundError(f"Catalog file not found at: {catalog_path}")
    return Catalog(path)


__all__ = [
    "AmbiguousLayoutError",
    "Catalog",
    "FileGPS",
    "FileSlice",
    "InvalidOffsetError",
    "Layout",
    "LayoutNotFoundError",
    "ManifestEntry",
    "PathConstraintError",
    "PathConstraintReportError",
    "Tape",
    "TapeCorruptedError",
    "TapeNotFoundError",
    "TapeVerificationError",
    "TarEvent",
    "TarIntegrityError",
    "TarObserver",
    "TarTapeError",
    "Volume",
    "VolumeChecksumMismatchError",
    "VolumeNotFoundError",
    "VolumeStateError",
    "create",
    "discover",
    "exists",
    "get_catalog",
    "get_tape",
    "open",
    "open_catalog",
    "record",
]
