from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

from tartape.constants import TAR_BLOCK_SIZE
from tartape.models import Track


class EntryState(str, Enum):
    """The state of a file in a volume."""

    COMPLETE = "complete"  # The file starts and ends in this volume
    HEAD = "head"  # It starts here, but ends in a future volume
    BODY = "body"  # It comes from the past, passes through here, and goes to the future
    TAIL = "tail"  # It comes from the past and ends in this volume


@dataclass(frozen=True)
class DiskEntryStats:
    exists: bool
    size: int = 0
    mtime: int = 0
    mode: int = 0
    uid: int = 0
    gid: int = 0
    uname: str = ""
    gname: str = ""
    is_dir: bool = False
    is_file: bool = False
    is_symlink: bool = False
    linkname: str = ""


@dataclass(frozen=True)
class FileStartMetadata:
    start_offset: int
    resumed: bool


@dataclass(frozen=True)
class FileEndMetadata:
    end_offset: int
    md5sum: Optional[str]
    is_complete: bool


@dataclass(frozen=True)
class TarFileStartEvent:
    type: Literal["file_start"]
    entry: "ManifestEntry"
    metadata: FileStartMetadata

    model_config = {"arbitrary_types_allowed": True}


@dataclass(frozen=True)
class TarFileDataEvent:
    type: Literal["file_data"]
    data: bytes

    model_config = {"arbitrary_types_allowed": True}


@dataclass(frozen=True)
class TarFileEndEvent:
    type: Literal["file_end"]
    entry: "ManifestEntry"
    metadata: FileEndMetadata

    model_config = {"arbitrary_types_allowed": True}


@dataclass(frozen=True)
class TarTapeCompletedEvent:
    type: Literal["tape_completed"]


TarEvent = Union[
    TarFileStartEvent, TarFileDataEvent, TarFileEndEvent, TarTapeCompletedEvent
]


@dataclass(frozen=True)
class ManifestEntry:
    arc_path: str
    rel_path: str
    track_id: int

    # Volume State
    state: EntryState
    offset_in_volume: int
    bytes_in_volume: int

    # File Metadata
    size: int
    mtime: int
    mode: int
    uid: int
    gid: int
    uname: str
    gname: str
    is_dir: bool
    is_symlink: bool = False
    linkname: str = ""
    md5sum: Optional[str] = None

    start_offset: int = 0
    end_offset: int = 0

    # temp attr : directory + rel_path
    _source_root: Optional[Path] = field(default=None, repr=False, compare=False)

    @property
    def is_file(self) -> bool:
        return not (self.is_dir or self.is_symlink)

    @property
    def has_content(self) -> bool:
        """Determines whether a data block (regular files) is required."""
        return not (self.is_dir or self.is_symlink)

    @property
    def source_path(self) -> Path:
        root = self._source_root or Path(".")
        return root / self.rel_path

    @property
    def header_end_offset(self) -> int:
        return self.start_offset + TAR_BLOCK_SIZE

    @property
    def content_end_offset(self) -> int:
        # Only regular files have content
        content_size = self.size if self.has_content else 0
        return self.header_end_offset + content_size

    @property
    def padding_size(self) -> int:
        if not self.has_content or self.size == 0:
            return 0
        return (TAR_BLOCK_SIZE - (self.size % TAR_BLOCK_SIZE)) % TAR_BLOCK_SIZE

    @property
    def total_block_size(self) -> int:
        content_size = self.size if not (self.is_dir or self.is_symlink) else 0
        return TAR_BLOCK_SIZE + content_size + self.padding_size

    def validate_integrity(self, tape_root_directory: Path):
        """
        Receive the ROOT of the folder being streamed.
        Example: Path("/mnt/data")


        Strict implementation of ADR-002.
        Verify that the file on disk matches the inventory.
        """
        from tartape.exceptions import TarIntegrityError
        from tartape.factory import TarEntryFactory

        full_disk_path = tape_root_directory / self.rel_path
        stats = TarEntryFactory.inspect(full_disk_path)

        if not stats.exists:
            raise TarIntegrityError(f"File missing: {self.arc_path}")

        if self.is_dir:
            if self.rel_path in ("", "."):
                return
            if stats.mtime != self.mtime:
                raise TarIntegrityError(f"Directory structure changed: {self.arc_path}")
            return

        if stats.mtime != self.mtime:
            raise TarIntegrityError(f"File modified (mtime): {self.arc_path}")

        if not self.is_symlink:
            if stats.size != self.size:
                raise TarIntegrityError(f"File size changed: {self.arc_path}")

    @classmethod
    def from_track(cls, track: "Track", start: int, end: int) -> "ManifestEntry":
        starts_inside = track.start_offset >= start
        ends_inside = track.end_offset <= end

        if starts_inside and ends_inside:
            state = EntryState.COMPLETE
        elif starts_inside and not ends_inside:
            state = EntryState.HEAD
        elif not starts_inside and ends_inside:
            state = EntryState.TAIL
        else:
            state = EntryState.BODY

        # If it starts before the volume, its local offset is 0.
        local_start = max(0, track.start_offset - start)

        # The bytes used are the minimum between the end of the file and the end of the volume
        # minus the maximum between the beginning of the file and the beginning of the volume.
        bytes_occupied = min(track.end_offset, end) - max(track.start_offset, start)

        return cls(
            arc_path=track.arc_path,
            rel_path=track.rel_path,
            track_id=track.id,
            state=state,
            offset_in_volume=local_start,
            bytes_in_volume=bytes_occupied,
            size=track.size,
            mtime=track.mtime,
            mode=track.mode,
            uid=track.uid,
            gid=track.gid,
            uname=track.uname,
            gname=track.gname,
            is_dir=track.is_dir,
            is_symlink=track.is_symlink,
            linkname=track.linkname,
            md5sum=track.md5sum,
            start_offset=track.start_offset,
            end_offset=track.end_offset,
        )


@dataclass(frozen=True)
class VolumeManifest:
    tape_fingerprint: str
    volume_index: int
    start_offset: int
    end_offset: int
    chunk_size: int
    entries: List[ManifestEntry]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tape_fingerprint": self.tape_fingerprint,
            "volume_index": self.volume_index,
            "start_offset": self.start_offset,
            "end_offset": self.end_offset,
            "chunk_size": self.chunk_size,
            "entries": self.entries,
        }
