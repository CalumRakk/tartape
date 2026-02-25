from pathlib import Path
from typing import cast

from peewee import (
    BooleanField,
    CharField,
    IntegerField,
    Model,
)

from tartape.database import db_proxy


class BaseModel(Model):
    class Meta:
        database = db_proxy


class TapeMetadata(BaseModel):
    """Saves the global information of the tape (Signature, Total size, etc.)"""

    key = CharField(unique=True)
    value = CharField()


class Track(BaseModel):
    """Represents a file/folder on the tape"""

    arc_path = cast(str, CharField(primary_key=True))
    rel_path = cast(str, CharField())

    # Tar Header
    size = cast(int, IntegerField())
    mtime = cast(int, IntegerField())
    mode = cast(int, IntegerField())
    uid = cast(int, IntegerField())
    gid = cast(int, IntegerField())
    uname = cast(str, CharField())
    gname = cast(str, CharField())

    is_dir = BooleanField(default=False)
    is_symlink = BooleanField(default=False)
    linkname = cast(str, CharField(null=True))

    start_offset = cast(int, IntegerField(null=True))
    end_offset = cast(int, IntegerField(null=True))

    _source_root = None

    @property
    def source_path(self) -> Path:
        if self._source_root is not None:
            return Path(self._source_root) / self.rel_path
        return Path(self.rel_path)

    @source_path.setter
    def source_path(self, value: Path):
        self._source_root = value
