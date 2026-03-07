from typing import Optional, cast

from peewee import (
    BooleanField,
    CharField,
    IntegerField,
    Model,
)

from tartape.constants import TAR_BLOCK_SIZE
from tartape.database import db_proxy
from tartape.schemas import EntryMetadata


class BaseModel(Model):
    class Meta:
        database = db_proxy


class TapeMetadata(BaseModel):
    """Saves the global information of the tape (Signature, Total size, etc.)"""

    key = CharField(unique=True)
    value = CharField()


class Track(BaseModel):
    """
    Represents a file's existence on the Tape.
    It stores the EntryMetadata + The Global Window (start/end offsets).
    """

    id: int
    arc_path = cast(str, CharField(primary_key=True))
    rel_path = cast(str, CharField())

    # Metadata fields (Mirroring EntryMetadata)
    size = cast(int, IntegerField())
    mtime = cast(int, IntegerField())
    mode = cast(int, IntegerField())
    uid = cast(int, IntegerField())
    gid = cast(int, IntegerField())
    uname = cast(str, CharField())
    gname = cast(str, CharField())

    is_dir = cast(bool, BooleanField(default=False))
    is_symlink = cast(bool, BooleanField(default=False))
    linkname = cast(str, CharField(null=True))
    md5sum = cast(Optional[str], CharField(null=True))

    # The Global Window (Tape Coordinates)
    start_offset = cast(int, IntegerField(null=True))
    end_offset = cast(int, IntegerField(null=True))

    @property
    def is_file(self) -> bool:
        return not (self.is_dir or self.is_symlink)

    @property
    def has_content(self) -> bool:
        return not (self.is_dir or self.is_symlink)

    @property
    def padding_size(self) -> int:
        if not self.has_content or self.size == 0:
            return 0
        return (TAR_BLOCK_SIZE - (self.size % TAR_BLOCK_SIZE)) % TAR_BLOCK_SIZE

    @property
    def total_block_size(self) -> int:
        """Required by the Recorder to calculate the next offset."""
        content_size = self.size if self.has_content else 0
        return TAR_BLOCK_SIZE + content_size + self.padding_size

    def to_metadata(self) -> EntryMetadata:
        """Hydrates the pure metadata object from the DB record."""
        return EntryMetadata(
            arc_path=self.arc_path,
            rel_path=self.rel_path,
            size=self.size,
            mtime=self.mtime,
            mode=self.mode,
            uid=self.uid,
            gid=self.gid,
            uname=self.uname,
            gname=self.gname,
            is_dir=self.is_dir,
            is_symlink=self.is_symlink,
            linkname=self.linkname or "",
            md5sum=self.md5sum,
        )
