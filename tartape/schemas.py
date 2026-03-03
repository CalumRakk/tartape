from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from tartape.models import Track


class EntryState(str, Enum):
    """The state of a file in a volume."""

    COMPLETE = "complete"  # The file starts and ends in this volume
    HEAD = "head"  # It starts here, but ends in a future volume
    BODY = "body"  # It comes from the past, passes through here, and goes to the future
    TAIL = "tail"  # It comes from the past and ends in this volume


@dataclass(frozen=True)
class ManifestEntry:
    arc_path: str
    state: EntryState
    # Byte within THIS volume where the file data begins
    offset_in_volume: int
    # How many bytes of this file physically reside in this volume
    bytes_in_volume: int
    md5sum: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "arc_path": self.arc_path,
            "state": self.state.value,
            "offset_in_volume": self.offset_in_volume,
            "bytes_in_volume": self.bytes_in_volume,
            "md5sum": self.md5sum,
        }


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
            "entries": [e.to_dict() for e in self.entries],
        }


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
    entry: Track
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
    entry: Track
    metadata: FileEndMetadata

    model_config = {"arbitrary_types_allowed": True}


@dataclass(frozen=True)
class TarTapeCompletedEvent:
    type: Literal["tape_completed"]


TarEvent = Union[
    TarFileStartEvent, TarFileDataEvent, TarFileEndEvent, TarTapeCompletedEvent
]
