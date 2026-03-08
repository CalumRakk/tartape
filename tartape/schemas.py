from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

from tartape.constants import TAR_BLOCK_SIZE

if TYPE_CHECKING:
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
class ByteWindow:
    """
    Represents a specific segment of the byte stream.
    Used to project files into either a full Tape or a single Volume.
    """

    start: int
    end: int

    def __post_init__(self):
        if self.start < 0:
            raise ValueError(f"Window start cannot be negative: {self.start}")
        if self.end < self.start:
            raise ValueError(
                f"Window end ({self.end}) must be greater than start ({self.start})"
            )

    @property
    def size(self) -> int:
        """The size of the window in bytes."""
        return self.end - self.start

    def contains(self, offset: int) -> bool:
        """Check if an offset is within the window."""
        return self.start <= offset < self.end

    def intersects(self, other_start: int, other_end: int) -> bool:
        """Check if a file's byte range overlaps with this window."""
        return other_start < self.end and other_end > self.start

@dataclass(frozen=True)
class EntryMetadata:
    """
    Pure identity and filesystem attributes of a file or directory.
    This object is agnostic of its position on the tape.
    """

    arc_path: str
    rel_path: str
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

    @property
    def has_content(self) -> bool:
        """Determines if the entry requires a data block in the TAR stream."""
        return not (self.is_dir or self.is_symlink)

    @property
    def padding_size(self) -> int:
        """Calculates the null bytes needed to align the file to a 512-byte block."""
        if not self.has_content or self.size == 0:
            return 0
        return (TAR_BLOCK_SIZE - (self.size % TAR_BLOCK_SIZE)) % TAR_BLOCK_SIZE

    @property
    def total_block_size(self) -> int:
        """
        Returns the total footprint of this entry in bytes.
        Includes Header (512) + Content + Padding.
        """
        content_size = self.size if self.has_content else 0
        return TAR_BLOCK_SIZE + content_size + self.padding_size


@dataclass(frozen=True)
class ManifestEntry:
    info: EntryMetadata
    state: EntryState
    global_window: ByteWindow
    local_window: ByteWindow

    @property
    def is_file(self) -> bool:
        return not (self.info.is_dir or self.info.is_symlink)

    @property
    def has_content(self) -> bool:
        """Determines whether a data block (regular files) is required."""
        return not (self.info.is_dir or self.info.is_symlink)

    @property
    def header_end_offset(self) -> int:
        return self.global_window.start + TAR_BLOCK_SIZE

    @property
    def content_end_offset(self) -> int:
        # Only regular files have content
        content_size = self.info.size if self.has_content else 0
        return self.header_end_offset + content_size

    def get_absolute_path(self, root_dir: Path) -> Path:
        """
        Resolves the physical location of the file on disk.
        The entry itself only knows its identity, while the caller
        provides the current execution context (root_dir).
        """
        return Path(root_dir) / self.info.rel_path

    @classmethod
    def from_track(cls, track: "Track", view_window: ByteWindow) -> "ManifestEntry":
        global_window = ByteWindow(start=track.start_offset, end=track.end_offset)
        info = track.to_metadata()

        # State must be determined using GLOBAL coordinates.
        # Local coordinates only show the clipped projection (what fits inside the view).
        # Comparing global windows is the only way to know if the file naturally ends
        # here, or if it was truncated by the volume's boundary.
        starts_inside = global_window.start >= view_window.start
        ends_inside = global_window.end <= view_window.end

        if starts_inside and ends_inside:
            state = EntryState.COMPLETE
        elif starts_inside and not ends_inside:
            state = EntryState.HEAD
        elif not starts_inside and ends_inside:
            state = EntryState.TAIL
        else:
            state = EntryState.BODY

        overlap_start = max(global_window.start, view_window.start)
        overlap_end = min(global_window.end, view_window.end)

        local_start = max(0, overlap_start - view_window.start)
        local_end = max(0, overlap_end - view_window.start)

        return cls(
            info=info,
            global_window=global_window,
            local_window=ByteWindow(start=local_start, end=local_end),
            state=state,
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
