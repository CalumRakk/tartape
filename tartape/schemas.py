import tarfile
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

    def as_tarinfo(self) -> tarfile.TarInfo:
        """Converts a TarEntry into a TarInfo object from the standard library."""
        info = tarfile.TarInfo(name=self.arc_path)
        info.size = self.size
        info.mtime = int(self.mtime)
        info.mode = self.mode
        info.uid = self.uid
        info.gid = self.gid
        info.uname = self.uname
        info.gname = self.gname

        if self.is_dir:
            info.type = tarfile.DIRTYPE
        elif self.is_symlink:
            info.type = tarfile.SYMTYPE
            info.linkname = self.linkname
        else:
            info.type = tarfile.REGTYPE
        return info

    def _is_ustar_splittable(self, path: str) -> bool:
        """
        Check if a route can be split into Name(100) and Prefix(155).
        """
        path_b = path.encode("utf-8")
        if len(path_b) <= 100:
            return True

        if len(path_b) > 255:
            return False

        path_str = path
        for i, char in enumerate(path_str):
            if char == "/":
                prefix = path_str[:i].encode("utf-8")
                name = path_str[i + 1 :].encode("utf-8")
                if len(prefix) <= 155 and len(name) <= 100:
                    return True
        return False

    def validate_compliance(self):
        """
        Ensures the file is compatible with TarTape without truncation.
        Acts as a 'Lie Detector' for Tarfile before generating the header.
        """
        reasons = []

        # The standard reserves exactly 32 bytes. We will not allow truncation.
        uname_b = self.uname.encode("utf-8")
        if len(uname_b) > 32:
            reasons.append(
                f"User '{self.uname}' exceeds 32 bytes (has {len(uname_b)})."
            )

        gname_b = self.gname.encode("utf-8")
        if len(gname_b) > 32:
            reasons.append(
                f"Group '{self.gname}' exceeds 32 bytes (has {len(gname_b)})."
            )

        if self.is_symlink:
            link_b = self.linkname.encode("utf-8")
            if len(link_b) > 100:
                reasons.append(
                    f"Destination very long link ({len(link_b)} bytes). Maximum 100."
                )

        path_b = self.arc_path.encode("utf-8")
        if len(path_b) > 255:
            reasons.append(f"Path too long ({len(path_b)} bytes). Maximum 255.")

        # If there are violations, we fail before Tarfile can lie to us
        if reasons:
            error_msg = " | ".join(reasons)
            raise ValueError(
                f"Incompatible with TarTape (512-byte contract): {error_msg}"
            )

        info = self.as_tarinfo()
        try:
            header = info.tobuf(format=tarfile.GNU_FORMAT)
            if len(header) != 512:
                raise ValueError(
                    "The structure requires additional blocks (LongLink/PAX)."
                )
        except Exception as e:
            raise ValueError(f"TAR serialization error: {str(e)}")


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
