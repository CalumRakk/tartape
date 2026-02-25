from pathlib import Path
from typing import cast

from peewee import (
    BooleanField,
    CharField,
    FloatField,
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
    size = IntegerField()
    mtime = FloatField()
    mode = IntegerField()
    uid = IntegerField()
    gid = IntegerField()
    uname = CharField()
    gname = CharField()

    is_dir = BooleanField(default=False)
    is_symlink = BooleanField(default=False)
    linkname = CharField(null=True)

    start_offset = IntegerField(null=True)
    end_offset = IntegerField(null=True)

    _source_root = None

    @property
    def source_path(self) -> Path:
        if self._source_root is not None:
            return Path(self._source_root) / self.rel_path
        return Path(self.rel_path)

    @source_path.setter
    def source_path(self, value: Path):
        self._source_root = value
