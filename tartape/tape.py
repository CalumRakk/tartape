import json
import logging
import os
import shutil
import time
from collections.abc import Callable
from pathlib import Path
from typing import Generator, Optional

from tartape.catalog import Catalog
from tartape.chunker import TarChunker
from tartape.constants import TAPE_EXTENSION, TAPE_METADATA_DIR
from tartape.exceptions import (
    InvalidOffsetError,
    TapeNotFoundError,
    TapeVerificationError,
    TarIntegrityError,
)
from tartape.factory import (
    check_entry_integrity,
    validate_integrity,
    validate_root_structure_integrity,
)
from tartape.models import Track
from tartape.schemas import (
    ByteWindow,
    Discrepancy,
    FileSlice,
    ManifestEntry,
    TarEvent,
    TarObserver,
    VerificationReport,
)
from tartape.stream import (
    TapeStreamReader,
    TapeVolume,
    TarStreamGenerator,
    Volume,
)

logger = logging.getLogger(__name__)


class Tape(os.PathLike):
    """The Master Class representing a complete data tape.

    Orchestrates the Catalog, the Player, and the Chunker.
    """

    def __init__(
        self,
        directory: str | Path,
        catalog_path: Optional[str | Path] = None,
    ):
        self.directory = Path(directory).resolve()
        self.catalog_path = Path(catalog_path).resolve() if catalog_path else None
        self._stats = {}
        self._track_count = 0
        self._catalog_instance: Optional[Catalog] = None
        self._refresh_metadata()

    def _refresh_metadata(self):
        cat = self._get_catalog()
        with cat:
            self._stats = cat.get_stats()
            self._track_count = cat.file_count

    def _get_catalog(self) -> Catalog:
        if self._catalog_instance is None:
            if self.catalog_path is not None:
                if not self.catalog_path.exists():
                    raise TapeNotFoundError(
                        f"Catalog file not found at: {self.catalog_path}"
                    )
                self._catalog_instance = Catalog(self.catalog_path)
            else:
                self._catalog_instance = Catalog.from_directory(self.directory)
        return self._catalog_instance

    def close(self):
        """Close any cached catalog database connections."""
        if self._catalog_instance is not None:
            self._catalog_instance.close()
            self._catalog_instance = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __fspath__(self) -> str:
        """Return the file system path representation of the sidecar catalog file."""
        if self.catalog_path:
            return str(self.catalog_path)
        return str(self._get_catalog().path)

    def __eq__(self, other: object) -> bool:
        """Allow comparing Tape directly against Path or string catalog paths."""
        if isinstance(other, (str, Path, os.PathLike)):
            return Path(self.__fspath__()) == Path(other)
        return super().__eq__(other)

    @property
    def count_files(self) -> int:
        """Total number of files in the tape."""
        return self.file_count

    @property
    def fingerprint(self) -> str:
        """Return the digital signature of the tape."""
        return self._stats["fingerprint"]

    @property
    def total_size(self) -> int:
        """Return the total size of the TAR stream in bytes."""
        return self._stats["total_size"]

    @property
    def created_at(self) -> int:
        return self._stats["created_at"]

    @property
    def exclude_patterns(self) -> list[str] | str:
        value = self._stats["exclude_patterns"]
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    def get_tracks(self):
        """Yield all tracks sorted for the stream."""
        cat = self._get_catalog()
        with cat, cat.db_session:
            yield from Track.select().order_by(Track.arc_path).iterator()

    def destroy(self):
        """Remove metadata artifacts (sidecar or legacy directory)."""
        metadata_dir = self.directory / TAPE_METADATA_DIR
        if metadata_dir.exists():
            shutil.rmtree(metadata_dir)

        if self.catalog_path and self.catalog_path.exists():
            self.catalog_path.unlink()
        else:
            sidecar = self.directory.parent / f"{self.directory.name}{TAPE_EXTENSION}"
            if sidecar.exists():
                sidecar.unlink()

    def _select_canary_tracks(self, total_tracks: int) -> list[Track]:
        """Select strategic sentinel tracks for ultra-fast canary verification.

        Combines boundary tracks, volatile hot-files, structural directories,
        and stratified offset deciles without using non-deterministic table scans.
        """
        # Small dataset graceful fallback: verify 100% of tracks if count <= 50
        if total_tracks <= 50:
            return list(Track.select().order_by(Track.arc_path))

        canaries: dict[str, Track] = {}

        # Boundary Sentinels: First and Last tracks in the archive stream
        first_track = Track.select().order_by(Track.start_offset.asc()).first()  # type: ignore
        if first_track:
            canaries[first_track.arc_path] = first_track

        last_track = Track.select().order_by(Track.start_offset.desc()).first()  # type: ignore
        if last_track:
            canaries[last_track.arc_path] = last_track

        # Structural Sentinels: All recorded directories (catches additions/deletions)
        for dir_track in Track.select().where(Track.is_dir == True):
            canaries[dir_track.arc_path] = dir_track

        # Volatility Sentinels: Top 10 most recently modified regular files at T0
        hot_tracks = (
            Track.select()
            .where((Track.is_dir == False) & (Track.is_symlink == False))
            .order_by(Track.mtime.desc())  # type: ignore
            .limit(10)
        )
        for hot_track in hot_tracks:
            canaries[hot_track.arc_path] = hot_track

        # Decile Strata Sentinels: Deterministic offset probes across tape coordinates
        if self.total_size > 0:
            for step in range(1, 10):
                target_offset = int(self.total_size * (step / 10.0))
                decile_track = (
                    Track.select()
                    .where(
                        (Track.start_offset <= target_offset)
                        & (Track.end_offset > target_offset)
                    )
                    .first()
                )
                if decile_track:
                    canaries[decile_track.arc_path] = decile_track

        return list(canaries.values())

    def verify(
        self, deep: bool = True, raise_exception: bool = False
    ) -> VerificationReport:
        """Verify whether physical disk state matches the recorded tape catalog.

        Args:
            deep: If True, performs a thorough 100% audit of all recorded tracks.
                If False, runs an ultra-fast canary audit using boundary, hot-file,
                structural, and decile sentinels.
            raise_exception: If True, immediately raises TarIntegrityError on the
                first detected discrepancy instead of returning a report.

        Returns:
            VerificationReport: Rich audit report indicating validity and discrepancies.

        Raises:
            TarIntegrityError: If raise_exception is True and a discrepancy is found.
            TapeVerificationError: If an unexpected system error occurs during verification.
        """
        start_time = time.perf_counter()
        discrepancies: list[Discrepancy] = []

        try:
            cat = self._get_catalog()
            with cat, cat.db_session:
                total_tracks = Track.select().count()

                # Structural check on root directory with exclusion awareness
                root_discrepancy = validate_root_structure_integrity(
                    self.directory,
                    exclude=self.exclude_patterns,
                    raise_exception=raise_exception,
                )
                if root_discrepancy:
                    discrepancies.append(root_discrepancy)

                # Select tracks based on verification mode
                if deep:
                    tracks_to_check = Track.select().order_by(Track.arc_path)
                else:
                    tracks_to_check = self._select_canary_tracks(total_tracks)

                checked_count = 0
                for track in tracks_to_check:
                    checked_count += 1
                    discrepancy = check_entry_integrity(
                        track,
                        self.directory,
                        exclude=self.exclude_patterns,
                    )
                    if discrepancy:
                        if raise_exception:
                            raise TarIntegrityError(discrepancy.message)
                        discrepancies.append(discrepancy)

                duration_ms = (time.perf_counter() - start_time) * 1000.0
                return VerificationReport(
                    is_valid=len(discrepancies) == 0,
                    mode="deep" if deep else "canary",
                    total_tracks=total_tracks,
                    checked_count=checked_count,
                    duration_ms=duration_ms,
                    discrepancies=discrepancies,
                )

        except TarIntegrityError:
            if raise_exception:
                raise
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return VerificationReport(
                is_valid=False,
                mode="deep" if deep else "canary",
                total_tracks=self._track_count,
                checked_count=0,
                duration_ms=duration_ms,
                discrepancies=discrepancies,
            )
        except Exception as e:
            if raise_exception:
                raise TapeVerificationError(
                    f"Unexpected error during verification: {e}"
                ) from e
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            discrepancies.append(
                Discrepancy(
                    arc_path="",
                    rel_path="",
                    reason="structural_change",
                    message=f"Verification failed due to unexpected error: {e}",
                )
            )
            return VerificationReport(
                is_valid=False,
                mode="deep" if deep else "canary",
                total_tracks=self._track_count,
                checked_count=0,
                duration_ms=duration_ms,
                discrepancies=discrepancies,
            )

    def _verify_resume_point_integrity(self, catalog: Catalog, absolute_offset: int):
        if absolute_offset < 0 or absolute_offset >= self.total_size:
            raise InvalidOffsetError(f"Invalid resume offset: {absolute_offset}")

        if absolute_offset >= self.total_size - 1024:
            return

        track = catalog.find_track_at_absolute_offset(absolute_offset)
        full_tape_window = ByteWindow(0, self.total_size)
        entry = ManifestEntry.from_track(track, full_tape_window)
        validate_integrity(entry.info, self.directory)

    def iter_volumes(
        self,
        size: int | str,
        tag: Optional[str] = None,
        is_default: bool = True,
        naming_template: Optional[str] = None,
    ) -> Generator[Volume, None, None]:
        """Partition the tape into logical volumes and register the layout in the catalog."""
        with self._get_catalog() as cat:
            layout = cat.register_layout(
                volume_size=size,
                tag=tag,
                is_default=is_default,
                naming_template=naming_template,
            )
            vol_records = layout.volumes

            for vol_rec in vol_records:
                window = ByteWindow(start=vol_rec.start_offset, end=vol_rec.end_offset)
                manifest = TarChunker.get_volume_manifest_for_range(
                    fingerprint=self.fingerprint,
                    vol_index=vol_rec.volume_index,
                    volume_window=window,
                    total_size=self.total_size,
                )

                volume = Volume(
                    directory=self.directory,
                    manifest=manifest,
                    name=vol_rec.name,
                    layout_tag=layout.tag,
                    total_volumes=layout.total_volumes,
                    catalog_path=cat.path,
                )
                yield volume

    @property
    def file_count(self) -> int:
        """Return the total number of files in the tape."""
        return self._track_count

    def inspect(self) -> Generator[Track, None, None]:
        """Yield recorded tracks in deterministic order without reading file content."""
        cat = self._get_catalog()
        with cat, cat.db_session:
            yield from Track.select().order_by(Track.arc_path).iterator()

    def as_file(self, buffer_size: int = 64 * 1024) -> TapeStreamReader:
        """Expose the entire continuous TAR stream as an io.BufferedIOBase file-like object."""
        self.verify(deep=False, raise_exception=True)
        return TapeStreamReader(self, buffer_size=buffer_size)

    def play(
        self,
        start_offset: int = 0,
        buffer_size: int = 64 * 1024,
        on_event: Optional[Callable[[TarEvent], None]] = None,
        observer: Optional[TarObserver] = None,
        fast_verify: bool = True,
    ) -> Generator[bytes, None, None]:
        """Stream raw TAR bytes directly while dispatching lifecycle telemetry in-band."""
        self.verify(deep=not fast_verify, raise_exception=True)

        tape_window = ByteWindow(start=0, end=self.total_size)
        cat = self._get_catalog()
        with cat:
            if start_offset > 0:
                self._verify_resume_point_integrity(cat, start_offset)

            tracks = list(cat.query_tracks_intersecting_range(start_offset))

            def track_loader():
                for track in tracks:
                    yield ManifestEntry.from_track(track, tape_window)

            engine = TarStreamGenerator(
                track_loader(), self.directory, total_tape_size=self.total_size
            )
            yield from engine.stream_bytes(
                start_offset=start_offset,
                chunk_size=buffer_size,
                on_event=on_event,
                observer=observer,
            )

    def get_volume(
        self,
        index: int | str = 0,
        size: int | str | None = None,
        tag: Optional[str] = None,
        vol_start: int | None = None,
        vol_end: int | None = None,
    ) -> TapeVolume:
        """Retrieve a specific volume by index or legacy parameters."""
        cat = self._get_catalog()
        if not cat.path.exists():
            raise TapeNotFoundError(f"The tape catalog does not exist at: {cat.path}")

        # Legacy 4-parameter mode support: (vol_name, vol_index, vol_start, vol_end)
        if isinstance(index, str) and vol_start is not None and vol_end is not None:
            vol_name = index
            vol_index = int(size) if size is not None else 0
            volume_window = ByteWindow(start=vol_start, end=vol_end)
            if vol_start < 0 or vol_end > self.total_size or vol_start >= vol_end:
                raise InvalidOffsetError(
                    f"Invalid range: {vol_start}-{vol_end}. Total tape size is {self.total_size}"
                )
            with cat:
                manifest = TarChunker.get_volume_manifest_for_range(
                    self.fingerprint,
                    vol_index,
                    volume_window,
                    total_size=self.total_size,
                )
            return Volume(self.directory, manifest, vol_name)

        vol_index = int(index)
        with cat:
            if tag is not None:
                layout = cat.get_layout(tag)
            elif size is not None:
                try:
                    layout = cat.get_layout(size)
                except Exception:
                    layout = cat.register_layout(volume_size=size, is_default=True)
            else:
                layout = cat.default_layout

            vol_rec = layout.get_volume(vol_index)
            window = ByteWindow(start=vol_rec.start_offset, end=vol_rec.end_offset)
            manifest = TarChunker.get_volume_manifest_for_range(
                fingerprint=self.fingerprint,
                vol_index=vol_rec.volume_index,
                volume_window=window,
                total_size=self.total_size,
            )

            return Volume(
                directory=self.directory,
                manifest=manifest,
                name=vol_rec.name,
                layout_tag=layout.tag,
                total_volumes=layout.total_volumes,
                catalog_path=cat.path,
            )

    def get_file_slices(self, arc_path: str, chunk_size: int) -> list[FileSlice]:
        chunker = TarChunker(chunk_size=chunk_size)
        return chunker.get_file_slices(self.directory, arc_path)

    def get_file_slices_map(self, chunk_size: int) -> dict[str, list[FileSlice]]:
        chunker = TarChunker(chunk_size=chunk_size)
        return chunker.get_file_slices_map(self.directory)
