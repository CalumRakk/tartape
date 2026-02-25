from dataclasses import dataclass
from typing import Literal, Optional, Union

from tartape.models import Track


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
