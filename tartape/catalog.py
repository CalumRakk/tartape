import hashlib
import json
import logging
from pathlib import Path
from typing import Iterable, Optional

from tartape.constants import TAR_BLOCK_SIZE
from tartape.exceptions import (
    AmbiguousLayoutError,
    InvalidOffsetError,
    LayoutNotFoundError,
    TapeNotFoundError,
    VolumeNotFoundError,
)
from tartape.models import LayoutRecord, TapeMetadata, Track, VolumeRecord
from tartape.schemas import FileGPS, FileSlice
from tartape.units import format_size, parse_size

from .database import DatabaseSession

logger = logging.getLogger(__name__)


class Layout:
    """Represents a specific partitioning layout (slicing scheme) for a tape.

    Enables surgical GPS lookups and volume verification without requiring
    source files on disk. Can be backed by a persistent database record or
    computed virtually on-the-fly.
    """

    def __init__(
        self,
        session: DatabaseSession,
        volume_size: int,
        tag: str,
        total_volumes: int,
        total_tape_size: int,
        record: Optional[LayoutRecord] = None,
        created_at: int = 0,
        is_default: bool = False,
    ):
        self._session = session
        self._record = record
        self.tag = tag
        self.volume_size = volume_size
        self.total_volumes = total_volumes
        self.total_tape_size = total_tape_size
        self.created_at = created_at
        self.is_default = is_default

    @property
    def is_virtual(self) -> bool:
        """True if this layout is calculated dynamically without a persistent DB record."""
        return self._record is None

    @classmethod
    def from_record(
        cls,
        record: LayoutRecord,
        session: DatabaseSession,
        total_tape_size: int,
    ) -> "Layout":
        return cls(
            session=session,
            volume_size=record.volume_size,
            tag=record.tag,
            total_volumes=record.total_volumes,
            total_tape_size=total_tape_size,
            record=record,
            created_at=record.created_at,
            is_default=record.is_default,
        )

    @classmethod
    def virtual(
        cls,
        volume_size: int,
        total_tape_size: int,
        session: DatabaseSession,
        tag: Optional[str] = None,
    ) -> "Layout":
        from tartape.chunker import calculate_segments

        segments = list(calculate_segments(total_tape_size, volume_size))
        total_vols = max(1, len(segments))
        layout_tag = tag or format_size(volume_size)
        return cls(
            session=session,
            volume_size=volume_size,
            tag=layout_tag,
            total_volumes=total_vols,
            total_tape_size=total_tape_size,
            record=None,
            created_at=0,
            is_default=False,
        )

    @property
    def volumes(self) -> list[VolumeRecord]:
        """Return all VolumeRecords belonging to this layout ordered by index."""
        if self._record is not None:
            with self._session:
                return list(
                    VolumeRecord.select()
                    .where(VolumeRecord.layout_tag == self.tag)
                    .order_by(VolumeRecord.volume_index)
                )

        # Transient synthesis for virtual layouts
        from tartape.chunker import calculate_segments

        template = "{name}_{fingerprint:.8}.tar.{pindex}"
        padding_width = max(3, len(str(self.total_volumes)))
        segments = list(calculate_segments(self.total_tape_size, self.volume_size))

        records = []
        for idx, (v_start, v_end) in enumerate(segments):
            pindex = str(idx + 1).zfill(padding_width)
            vol_name = template.format(
                name="virtual",
                fingerprint="adhoc",
                index=idx,
                pindex=pindex,
                part=idx + 1,
                total=self.total_volumes,
            )
            records.append(
                VolumeRecord(
                    layout_tag=self.tag,
                    volume_index=idx,
                    name=vol_name,
                    start_offset=v_start,
                    end_offset=v_end,
                    size=v_end - v_start,
                    checksum=None,
                    checksum_algorithm=None,
                )
            )
        return records

    def get_volume(self, volume_index: int) -> VolumeRecord:
        """Retrieve a specific volume record by its sequence index."""
        if self._record is not None:
            with self._session:
                try:
                    return VolumeRecord.get(
                        (VolumeRecord.layout_tag == self.tag)
                        & (VolumeRecord.volume_index == volume_index)
                    )
                except VolumeRecord.DoesNotExist:  # type: ignore
                    raise VolumeNotFoundError(
                        f"Volume index {volume_index} does not exist in layout '{self.tag}'"
                    )

        if volume_index < 0 or volume_index >= self.total_volumes:
            raise VolumeNotFoundError(
                f"Volume index {volume_index} out of bounds for virtual layout '{self.tag}'"
            )

        v_start = volume_index * self.volume_size
        v_end = min(self.total_tape_size, v_start + self.volume_size)
        padding_width = max(3, len(str(self.total_volumes)))
        pindex = str(volume_index + 1).zfill(padding_width)

        return VolumeRecord(
            layout_tag=self.tag,
            volume_index=volume_index,
            name=f"virtual_part_{pindex}.tar",
            start_offset=v_start,
            end_offset=v_end,
            size=v_end - v_start,
            checksum=None,
            checksum_algorithm=None,
        )

    def locate(self, arc_path: str) -> FileGPS:
        """Compute the exact GPS coordinates and slices for a file within this layout.

        Uses O(1) arithmetic based on global track offsets and volume boundaries.
        """
        with self._session:
            try:
                track = Track.get(Track.arc_path == arc_path)
            except Track.DoesNotExist:  # type: ignore
                raise KeyError(f"File not found in catalog: '{arc_path}'")

            content_size = track.size if track.has_content else 0
            if content_size == 0 or track.start_offset is None:
                return FileGPS(arc_path=arc_path, file_size=0, fragments=[])

            # In the TAR stream, data starts immediately after the 512-byte header
            content_global_start = track.start_offset + TAR_BLOCK_SIZE
            content_global_end = content_global_start + content_size

            first_vol = content_global_start // self.volume_size
            last_vol = (content_global_end - 1) // self.volume_size

            fragments: list[FileSlice] = []
            for vol_idx in range(first_vol, last_vol + 1):
                vol_start = vol_idx * self.volume_size
                vol_end = vol_start + self.volume_size

                overlap_start = max(content_global_start, vol_start)
                overlap_end = min(content_global_end, vol_end)

                if overlap_start < overlap_end:
                    fragments.append(
                        FileSlice(
                            volume_index=vol_idx,
                            volume_offset=overlap_start - vol_start,
                            volume_length=overlap_end - overlap_start,
                            source_offset=overlap_start - content_global_start,
                        )
                    )

            return FileGPS(
                arc_path=arc_path, file_size=content_size, fragments=fragments
            )

    def verify_volume(self, volume_index: int, file_path: str | Path) -> bool:
        """Verify a downloaded physical volume file against its recorded checksum."""
        path = Path(file_path)
        if not path.is_file():
            return False

        vol_record = self.get_volume(volume_index)
        if not vol_record.checksum:
            logger.warning(
                f"No checksum recorded for volume {volume_index} in layout '{self.tag}'"
            )
            return False

        algo = vol_record.checksum_algorithm or "sha256"
        try:
            hasher = hashlib.new(algo)
        except ValueError:
            hasher = hashlib.new(algo, usedforsecurity=False)

        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(64 * 1024), b""):
                hasher.update(chunk)

        return hasher.hexdigest() == vol_record.checksum


class Catalog:
    """Standalone, zero-disk interface to a .tartape sidecar catalog.

    Operates completely decoupled from the original source files.
    """

    def __init__(self, db_path: str | Path, read_only: bool = False):
        if str(db_path) == ":memory:":
            self.path = Path(":memory:")
            self.db_session = DatabaseSession(":memory:", read_only=read_only)
            self._stats_cache: Optional[dict] = None
        else:
            self.path = Path(db_path).resolve()
            if not self.path.exists() or not self.path.is_file():
                raise TapeNotFoundError(f"TarTape catalog not found at: {self.path}")

            self.db_session = DatabaseSession(self.path, read_only=read_only)
            self._stats_cache: Optional[dict] = None

    def _load_metadata(self) -> dict:
        if self._stats_cache is None:
            try:
                with self.db_session:
                    query = TapeMetadata.select()
                    raw = {m.key: m.value for m in query}
                    self._stats_cache = {
                        "fingerprint": raw.get("fingerprint", ""),
                        "total_size": int(raw.get("total_size", 0)),
                        "created_at": int(raw.get("created_at", 0)),
                        "exclude_patterns": raw.get("exclude_patterns", "[]"),
                        "checksum_algorithm": raw.get("checksum_algorithm", "none"),
                        "has_file_checksums": raw.get("has_file_checksums", "false"),
                        "data_size": int(raw.get("data_size", 0)),
                        "auto_truncated": raw.get("auto_truncated", "false"),
                        "is_anonymized": raw.get("is_anonymized", "false"),
                    }
            except Exception as e:
                from tartape.exceptions import TapeCorruptedError

                raise TapeCorruptedError(
                    f"Failed to read catalog metadata from {self.path}: {e}"
                ) from e
        return self._stats_cache

    @property
    def checksum_algorithm(self) -> Optional[str]:
        """Return the checksum algorithm used for files in this tape, or None."""
        algo = self._load_metadata().get("checksum_algorithm")
        return algo if algo and algo != "none" else None

    @property
    def has_file_checksums(self) -> bool:
        """True if individual files were checksummed at T0."""
        return self._load_metadata().get("has_file_checksums") == "true"

    @property
    def data_size(self) -> int:
        """Total uncompressed payload size in bytes."""
        return int(self._load_metadata().get("data_size", 0))

    @property
    def overhead_size(self) -> int:
        """TAR container overhead size in bytes."""
        return max(0, self.total_size - self.data_size)

    @property
    def auto_truncated(self) -> bool:
        """True if path auto-truncation was active during recording."""
        return self._load_metadata().get("auto_truncated") == "true"

    @property
    def is_anonymized(self) -> bool:
        """True if UID/GID were anonymized."""
        return self._load_metadata().get("is_anonymized") == "true"

    def get_stats(self) -> dict:
        """Return tape metadata dictionary for backward compatibility and internal helpers."""
        return self._load_metadata()

    def get_track_count(self) -> int:
        """Return total track count."""
        return self.file_count

    @property
    def fingerprint(self) -> str:
        """The SHA-256 fingerprint representing the deterministic archive contents."""
        return self._load_metadata()["fingerprint"]

    @property
    def total_size(self) -> int:
        """The exact total size in bytes of the complete TAR stream."""
        return self._load_metadata()["total_size"]

    @property
    def created_at(self) -> int:
        """Timestamp when the catalog was committed."""
        return self._load_metadata()["created_at"]

    @property
    def exclude_patterns(self) -> list[str] | str:
        """Exclusion patterns captured during initial recording."""
        raw = self._load_metadata()["exclude_patterns"]
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    @property
    def file_count(self) -> int:
        """Total number of file, directory, and link tracks recorded in this tape."""
        with self.db_session:
            return Track.select().count()

    # Layouts Management

    @property
    def layouts(self) -> list[Layout]:
        """Return all registered partitioning layouts."""
        with self.db_session:
            records = list(LayoutRecord.select().order_by(LayoutRecord.created_at))
            return [
                Layout.from_record(
                    rec, self.db_session, total_tape_size=self.total_size
                )
                for rec in records
            ]

    @property
    def default_layout(self) -> Layout:
        """Return the default layout, or the sole layout, or a virtual monolithic layout."""
        with self.db_session:
            records = list(LayoutRecord.select())
            if not records:
                # Monolithic fallback: treats the entire continuous stream as Volume 0
                return Layout.virtual(
                    volume_size=max(TAR_BLOCK_SIZE, self.total_size),
                    total_tape_size=self.total_size,
                    session=self.db_session,
                    tag="monolithic",
                )

            if len(records) == 1:
                return Layout.from_record(
                    records[0], self.db_session, total_tape_size=self.total_size
                )

            default_record = next((r for r in records if r.is_default), None)
            if default_record is not None:
                return Layout.from_record(
                    default_record,
                    self.db_session,
                    total_tape_size=self.total_size,
                )

            raise AmbiguousLayoutError(
                f"Multiple layouts exist ({[r.tag for r in records]}) and none is marked as default. "
                "Specify tag explicitly via get_layout(tag)."
            )

    def get_layout(self, tag_or_size: str | int) -> Layout:
        """Retrieve a specific layout by its tag name or volume size.

        If not found in the catalog database but parsable as a valid size,
        returns an on-the-fly virtual Layout without modifying SQLite.
        """
        with self.db_session:
            # 1. Look up by integer byte size in database
            if isinstance(tag_or_size, int):
                try:
                    record = LayoutRecord.get(LayoutRecord.volume_size == tag_or_size)
                    return Layout.from_record(
                        record, self.db_session, total_tape_size=self.total_size
                    )
                except LayoutRecord.DoesNotExist:  # type: ignore
                    pass

            # 2. Direct match on tag string (e.g., '1GB', 'custom_part')
            try:
                record = LayoutRecord.get(LayoutRecord.tag == str(tag_or_size))
                return Layout.from_record(
                    record, self.db_session, total_tape_size=self.total_size
                )
            except LayoutRecord.DoesNotExist:  # type: ignore
                pass

            # 3. Try parsing as size and look up matching volume_size in database
            parsed_bytes: Optional[int] = None
            try:
                parsed_bytes = parse_size(tag_or_size)
                record = LayoutRecord.get(LayoutRecord.volume_size == parsed_bytes)
                return Layout.from_record(
                    record, self.db_session, total_tape_size=self.total_size
                )
            except (ValueError, TypeError, LayoutRecord.DoesNotExist):  # type: ignore
                pass

            # 4. Backwards compatibility with legacy 'vol_{parsed_bytes}' or 'vol_{tag_or_size}'
            try:
                record = LayoutRecord.get(LayoutRecord.tag == f"vol_{tag_or_size}")
                return Layout.from_record(
                    record, self.db_session, total_tape_size=self.total_size
                )
            except LayoutRecord.DoesNotExist:  # type: ignore
                pass

            # 5. Virtual Layout Fallback: If it's a valid size, construct dynamically!
            if parsed_bytes is not None:
                return Layout.virtual(
                    volume_size=parsed_bytes,
                    total_tape_size=self.total_size,
                    session=self.db_session,
                    tag=format_size(parsed_bytes),
                )

            raise LayoutNotFoundError(f"Layout '{tag_or_size}' not found in catalog.")

    def register_layout(
        self,
        volume_size: int | str,
        tag: Optional[str] = None,
        is_default: bool = False,
        naming_template: Optional[str] = None,
    ) -> Layout:
        """Register a new partitioning layout and compute its volume manifest."""
        parsed_volume_size = parse_size(volume_size)
        layout_tag = tag or format_size(parsed_volume_size)
        total_size = self.total_size

        from tartape.chunker import calculate_segments

        segments = list(calculate_segments(total_size, parsed_volume_size))
        total_vols = len(segments)

        template = naming_template or "{name}_{fingerprint:.8}.tar.{pindex}"
        padding_width = max(3, len(str(total_vols)))

        with self.db_session, self.db_session.db.atomic():
            if is_default:
                LayoutRecord.update(is_default=False).execute()

            import time

            layout_rec, created = LayoutRecord.get_or_create(
                tag=layout_tag,
                defaults={
                    "volume_size": parsed_volume_size,
                    "total_volumes": total_vols,
                    "created_at": int(time.time()),
                    "is_default": is_default,
                },
            )
            if not created:
                layout_rec.volume_size = parsed_volume_size
                layout_rec.total_volumes = total_vols
                layout_rec.is_default = is_default
                layout_rec.save()

            volume_records = []
            for idx, (v_start, v_end) in enumerate(segments):
                pindex = str(idx + 1).zfill(padding_width)
                vol_name = template.format(
                    name=self.path.stem,
                    fingerprint=self.fingerprint,
                    index=idx,
                    pindex=pindex,
                    part=idx + 1,
                    total=total_vols,
                )
                volume_records.append(
                    {
                        "layout_tag": layout_tag,
                        "volume_index": idx,
                        "name": vol_name,
                        "start_offset": v_start,
                        "end_offset": v_end,
                        "size": v_end - v_start,
                        "checksum": None,
                        "checksum_algorithm": self.checksum_algorithm or "sha256",
                    }
                )

            VolumeRecord.insert_many(volume_records).on_conflict_replace().execute()

        return Layout.from_record(
            layout_rec, self.db_session, total_tape_size=total_size
        )

    # Convenience Shortcuts

    def locate(self, arc_path: str, tag_or_size: Optional[str | int] = None) -> FileGPS:
        """Query file GPS using the default layout or an explicitly specified layout."""
        layout = (
            self.get_layout(tag_or_size)
            if tag_or_size is not None
            else self.default_layout
        )
        return layout.locate(arc_path)

    # Context & Track Queries

    def find_track_at_absolute_offset(self, absolute_offset: int) -> Track:
        with self.db_session:
            try:
                return Track.get(
                    (Track.start_offset <= absolute_offset)
                    & (Track.end_offset > absolute_offset)
                )
            except Track.DoesNotExist:  # type: ignore
                raise InvalidOffsetError(
                    f"No track found at absolute offset {absolute_offset}"
                )

    def query_tracks_intersecting_range(self, start_offset: int) -> Iterable[Track]:
        with self.db_session:
            yield from (
                Track.select()
                .where(Track.end_offset > start_offset)
                .order_by(Track.arc_path)
                .iterator()
            )

    def open(self):
        """Explicitly open the catalog database session."""
        self.db_session.connect()
        return self

    def close(self):
        """Forcefully close the catalog database session and perform cleanup."""
        self.db_session.close()

    def __enter__(self):
        self.db_session.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.db_session.__exit__(exc_type, exc_val, exc_tb)

    @classmethod
    def from_directory(cls, directory: str | Path) -> "Catalog":
        from tartape import discover

        db_path = discover(directory)
        if not db_path:
            raise TapeNotFoundError(f"TarTape catalog not found for: {directory}")
        return cls(db_path)
