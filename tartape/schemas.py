from typing import Literal, Optional, Union

from pydantic import BaseModel

from .enums import TarEventType


class TarEntry(BaseModel):
    """Represents an item to be recorded on the tape."""

    source_path: str  # Physical path on disk
    arc_path: str  # Path inside the TAR
    size: int
    mtime: float
    is_dir: bool = False
    uid: int
    gid: int
    mode: int
    uname: str
    gname: str
    is_symlink: bool = False
    linkname: str = ""


class FileStartMetadata(BaseModel):
    start_offset: int


class FileEndMetadata(BaseModel):
    end_offset: int
    md5sum: Optional[str]


class TarFileStartEvent(BaseModel):
    type: Literal[TarEventType.FILE_START]
    entry: TarEntry
    metadata: FileStartMetadata


class TarFileDataEvent(BaseModel):
    type: Literal[TarEventType.FILE_DATA]
    entry: Optional[TarEntry] = None
    data: bytes


class TarFileEndEvent(BaseModel):
    type: Literal[TarEventType.FILE_END]
    entry: TarEntry
    metadata: FileEndMetadata


class TarTapeCompletedEvent(BaseModel):
    type: Literal[TarEventType.TAPE_COMPLETED]


TarEvent = Union[
    TarFileStartEvent, TarFileDataEvent, TarFileEndEvent, TarTapeCompletedEvent
]
