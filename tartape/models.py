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

    arc_path = CharField(primary_key=True)
    rel_path = CharField()

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
