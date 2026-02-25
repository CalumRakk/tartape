from typing import Literal, Optional, Union

from pydantic import BaseModel

from tartape.models import Track

from .enums import TarEventType


class FileStartMetadata(BaseModel):
    start_offset: int


class FileEndMetadata(BaseModel):
    end_offset: int
    md5sum: Optional[str]


class TarFileStartEvent(BaseModel):
    type: Literal[TarEventType.FILE_START]
    entry: Track
    metadata: FileStartMetadata

    model_config = {"arbitrary_types_allowed": True}


class TarFileDataEvent(BaseModel):
    type: Literal[TarEventType.FILE_DATA]
    entry: Optional[Track] = None
    data: bytes

    model_config = {"arbitrary_types_allowed": True}


class TarFileEndEvent(BaseModel):
    type: Literal[TarEventType.FILE_END]
    entry: Track
    metadata: FileEndMetadata

    model_config = {"arbitrary_types_allowed": True}


class TarTapeCompletedEvent(BaseModel):
    type: Literal[TarEventType.TAPE_COMPLETED]


TarEvent = Union[
    TarFileStartEvent, TarFileDataEvent, TarFileEndEvent, TarTapeCompletedEvent
]
