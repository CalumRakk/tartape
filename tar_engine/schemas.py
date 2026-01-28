from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from .enums import TarEventType


class TarEntry(BaseModel):
    """Representa un ítem que entrará en la cinta."""

    source_path: str  # Ruta física en disco
    arc_path: str  # Ruta que tendrá dentro del TAR
    size: int
    mtime: float


class TarEvent(BaseModel):
    """Información que el motor devuelve al usuario."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    type: TarEventType
    entry: Optional[TarEntry] = None
    data: Optional[bytes] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
