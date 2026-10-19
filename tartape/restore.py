"""Restoration and reassembly engine for TarTape volumes and file slices."""

import hashlib
import io
import logging
import os
import tarfile
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Optional

from tartape.constants import SUPPORTED_CHECKSUM_ALGORITHMS
from tartape.exceptions import (
    PathConstraintError,
    TarIntegrityError,
    VolumeChecksumMismatchError,
)
from tartape.schemas import FileSlice

logger = logging.getLogger(__name__)

# Type definition for volumes source (mapping of paths/streams or a lazy provider function)
VolumeProvider = (
    Mapping[int, Path | str | io.IOBase] | Callable[[int], Path | str | io.IOBase]
)


class ChainedVolumeStream(io.RawIOBase):
    """Sequential stream concatenating multiple volume file-like objects into a continuous stream."""

    def __init__(self, streams: Iterable[io.BufferedIOBase]):
        self._stream_iter = iter(streams)
        self._current: Optional[io.BufferedIOBase] = None
        self._advance()

    def _advance(self) -> None:
        # Close the exhausted stream before moving to the next
        if self._current is not None and hasattr(self._current, "close"):
            try:
                self._current.close()
            except Exception:
                pass

        try:
            self._current = next(self._stream_iter)
        except StopIteration:
            self._current = None

    def readinto(self, b) -> int:
        while self._current is not None:
            data = self._current.read(len(b))
            if data:
                b[: len(data)] = data
                return len(data)
            self._advance()
        return 0

    def readable(self) -> bool:
        return True

    def close(self) -> None:
        if self._current is not None and hasattr(self._current, "close"):
            self._current.close()
            self._current = None
        super().close()


def reassemble_file(
    output_path: Path | str,
    slices: Iterable[FileSlice],
    volumes: VolumeProvider,
    expected_size: Optional[int] = None,
    expected_checksum: Optional[str] = None,
    checksum_algorithm: str = "sha256",
    overwrite: bool = False,
) -> Path:
    """Reassemble an individual file from slices provided by volume paths or streams.

    Safely streams bytes into an atomic temporary file, computes the cryptographic
    checksum in-flight, validates integrity, and atomically moves to the final destination.

    Args:
        output_path: Final destination path on disk.
        slices: Ordered or unordered collection of FileSlice coordinates.
        volumes: Either a Mapping {vol_idx: Path | stream} or a Callable(vol_idx) -> Path | stream.
        expected_size: Optional expected uncompressed size in bytes.
        expected_checksum: Optional expected hash hex-digest for verification.
        checksum_algorithm: Algorithm for verification ('sha256', 'md5', etc.).
        overwrite: If True, replaces existing destination file.

    Returns:
        Path: The final resolved destination path.

    Raises:
        FileExistsError: If destination exists and `overwrite` is False.
        VolumeChecksumMismatchError: If the computed checksum does not match expected_checksum.
        TarIntegrityError: If the reassembled size does not match expected_size.
        KeyError: If a required volume index is missing from the volumes mapping.
    """
    dest = Path(output_path).resolve()
    if dest.exists() and not overwrite:
        raise FileExistsError(
            f"Destination file already exists: '{dest}'. Set overwrite=True to replace."
        )

    dest.parent.mkdir(parents=True, exist_ok=True)
    algo = checksum_algorithm.lower()
    if algo not in SUPPORTED_CHECKSUM_ALGORITHMS:
        raise ValueError(f"Unsupported checksum algorithm: '{algo}'")

    sorted_slices = sorted(slices, key=lambda s: s.source_offset)

    # Special Case: Empty file (0 bytes)
    if expected_size == 0 or (not sorted_slices and expected_size is None):
        try:
            hasher = hashlib.new(algo)
        except ValueError:
            hasher = hashlib.new(algo, usedforsecurity=False)
        empty_digest = hasher.hexdigest()

        if expected_checksum and empty_digest.lower() != expected_checksum.lower():
            raise VolumeChecksumMismatchError(
                f"Checksum mismatch for empty file '{dest.name}': "
                f"expected {expected_checksum}, calculated {empty_digest}"
            )

        dest.touch(exist_ok=overwrite)
        return dest

    # Cache for opened streams and list of descriptors opened by this function
    open_streams: dict[int, io.IOBase] = {}
    files_to_close: list[io.IOBase] = []

    def resolve_volume_stream(vol_idx: int) -> io.IOBase:
        if vol_idx in open_streams:
            return open_streams[vol_idx]

        if callable(volumes):
            target = volumes(vol_idx)
        elif isinstance(volumes, Mapping):
            if vol_idx not in volumes:
                raise KeyError(
                    f"Volume index {vol_idx} not found in provided volumes mapping."
                )
            target = volumes[vol_idx]
        else:
            raise TypeError(
                f"volumes must be a Mapping or Callable, got {type(volumes).__name__}"
            )

        if isinstance(target, (str, Path)):
            f_path = Path(target)
            if not f_path.is_file():
                raise FileNotFoundError(f"Volume file not found at: '{f_path}'")
            stream = open(f_path, "rb")  # noqa: SIM115
            files_to_close.append(stream)
        elif isinstance(target, io.IOBase):
            stream = target
        else:
            raise TypeError(
                f"Volume {vol_idx} must resolve to a Path, str, or io.IOBase, got {type(target).__name__}"
            )

        open_streams[vol_idx] = stream
        return stream

    # Atomic temporary file in destination parent directory
    temp_path = dest.parent / f".{dest.name}.assembling_{os.getpid()}"
    temp_path.unlink(missing_ok=True)

    hasher = None
    if expected_checksum:
        try:
            hasher = hashlib.new(algo)
        except ValueError:
            hasher = hashlib.new(algo, usedforsecurity=False)

    try:
        with open(temp_path, "wb") as out_f:
            if expected_size and expected_size > 0:
                out_f.truncate(expected_size)

            for sl in sorted_slices:
                if sl.volume_length <= 0:
                    continue

                stream = resolve_volume_stream(sl.volume_index)
                stream.seek(sl.volume_offset)
                out_f.seek(sl.source_offset)

                # Stream in 64 KB blocks to maintain O(1) RAM usage
                bytes_remaining = sl.volume_length
                while bytes_remaining > 0:
                    read_size = min(bytes_remaining, 64 * 1024)
                    chunk = stream.read(read_size)
                    if not chunk:
                        raise TarIntegrityError(
                            f"Unexpected EOF reading volume {sl.volume_index} for '{dest.name}'. "
                            f"Missing {bytes_remaining} bytes."
                        )
                    out_f.write(chunk)
                    if hasher:
                        hasher.update(chunk)
                    bytes_remaining -= len(chunk)

        # Validate total file size
        actual_size = temp_path.stat().st_size
        if expected_size is not None and actual_size != expected_size:
            temp_path.unlink(missing_ok=True)
            raise TarIntegrityError(
                f"Size mismatch reassembling '{dest.name}': "
                f"expected {expected_size} bytes, got {actual_size} bytes."
            )

        # Validate checksum
        if hasher and expected_checksum:
            computed_digest = hasher.hexdigest()
            if computed_digest.lower() != expected_checksum.lower():
                temp_path.unlink(missing_ok=True)
                raise VolumeChecksumMismatchError(
                    f"Checksum mismatch reassembling '{dest.name}': "
                    f"expected {expected_checksum}, calculated {computed_digest}"
                )

        temp_path.replace(dest)
        return dest

    finally:
        temp_path.unlink(missing_ok=True)
        # Close only the files opened internally by this function
        for f in files_to_close:
            try:
                f.close()
            except Exception:
                pass


def unpack_volumes(
    volume_streams: Iterable[io.BufferedIOBase],
    dest_dir: Path | str,
    on_file_extracted: Optional[Callable[[str, int], None]] = None,
    overwrite: bool = False,
) -> int:
    """Safely unpack a sequence of volume streams into a destination directory.

    Chains volumes sequentially into an uncompressed TAR stream, protects
    against Path Traversal vulnerabilities (Zip Slip), and restores file permissions
    and modification timestamps.

    Args:
        volume_streams: Iterable of open, readable volume streams (e.g. Volume objects).
        dest_dir: Target root directory for extraction.
        on_file_extracted: Optional callback invoked as `(relative_path, file_size)` per file.
        overwrite: If True, replaces existing files on collision.

    Returns:
        int: Total number of regular files extracted.

    Raises:
        PathConstraintError: If an archive entry attempts path traversal outside dest_dir.
        FileExistsError: If a target file exists and overwrite is False.
    """
    resolved_dest = Path(dest_dir).resolve()
    resolved_dest.mkdir(parents=True, exist_ok=True)

    chained_raw = ChainedVolumeStream(volume_streams)
    buffered_reader = io.BufferedReader(chained_raw)
    extracted_count = 0

    try:
        with tarfile.open(fileobj=buffered_reader, mode="r|*") as tar:
            for tarinfo in tar:
                target_path = (resolved_dest / tarinfo.name).resolve()

                # Path Traversal (Zip Slip) security guard
                if not str(target_path).startswith(str(resolved_dest)):
                    raise PathConstraintError(
                        f"Security violation: path traversal detected in TAR member '{tarinfo.name}'"
                    )

                if tarinfo.isdir():
                    target_path.mkdir(parents=True, exist_ok=True)
                    continue

                if tarinfo.issym():
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    if target_path.is_symlink() or target_path.exists():
                        target_path.unlink()
                    target_path.symlink_to(tarinfo.linkname)
                    continue

                if tarinfo.isreg():
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    if target_path.exists() and not overwrite:
                        raise FileExistsError(
                            f"Cannot extract '{tarinfo.name}': destination file exists."
                        )

                    fileobj = tar.extractfile(tarinfo)
                    if fileobj is None:
                        continue

                    with open(target_path, "wb") as out_f:
                        while chunk := fileobj.read(64 * 1024):
                            out_f.write(chunk)

                    # Restore original mtime and POSIX permissions
                    try:
                        os.utime(target_path, (tarinfo.mtime, tarinfo.mtime))
                        os.chmod(target_path, tarinfo.mode)
                    except OSError:
                        pass

                    extracted_count += 1
                    if on_file_extracted:
                        on_file_extracted(tarinfo.name, tarinfo.size)

    finally:
        buffered_reader.close()
        chained_raw.close()

    return extracted_count
