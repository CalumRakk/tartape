from pathlib import Path
from typing import cast

from peewee import (
    BooleanField,
    CharField,
    IntegerField,
    Model,
)

from tartape.constants import TAR_BLOCK_SIZE
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
    def is_file(self) -> bool:
        return not (self.is_dir or self.is_symlink)

    @property
    def has_content(self) -> bool:
        """Determines whether a data block (regular files) is required."""
        return not (self.is_dir or self.is_symlink)

    @property
    def source_path(self) -> Path:
        if self._source_root is not None:
            return Path(self._source_root) / self.rel_path
        return Path(self.rel_path)

    @source_path.setter
    def source_path(self, value: Path):
        self._source_root = value

    @property
    def header_end_offset(self) -> int:
        return self.start_offset + TAR_BLOCK_SIZE

    @property
    def content_end_offset(self) -> int:
        # Only regular files have content
        content_size = self.size if self.has_content else 0
        return self.header_end_offset + content_size

    @property
    def padding_size(self) -> int:
        if not self.has_content or self.size == 0:
            return 0
        return (TAR_BLOCK_SIZE - (self.size % TAR_BLOCK_SIZE)) % TAR_BLOCK_SIZE

    @property
    def total_block_size(self) -> int:
        content_size = self.size if not (self.is_dir or self.is_symlink) else 0
        return TAR_BLOCK_SIZE + content_size + self.padding_size

    def validate_integrity(self, tape_root_directory: Path):
        """
        Receive the ROOT of the folder being streamed.
        Example: Path("/mnt/data")


        Strict implementation of ADR-002.
        Verify that the file on disk matches the inventory.
        """
        from tartape.factory import TarEntryFactory

        full_disk_path = tape_root_directory / self.rel_path

        stats = TarEntryFactory.inspect(full_disk_path)
        if not stats.exists:
            raise RuntimeError(f"File missing: {self.arc_path}")

        if self.is_dir:
            # ADR-002: Root's mtime is ignored
            if self.rel_path in ("", "."):
                return
            if stats.mtime != self.mtime:
                raise RuntimeError(f"Directory structure changed: {self.arc_path}")
            return

        if stats.mtime != self.mtime:
            raise RuntimeError(f"File modified (mtime): {self.arc_path}")

        if not self.is_symlink:
            if stats.size != self.size:
                raise RuntimeError(f"File size changed: {self.arc_path}")
            if stats.mode != self.mode:
                raise RuntimeError(f"Permissions changed: {self.arc_path}")
        else:
            if stats.linkname != self.linkname:
                raise RuntimeError(f"Symlink target changed: {self.arc_path}")
