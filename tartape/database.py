from pathlib import Path
from typing import Literal, Union

import peewee

db_proxy = peewee.Proxy()


class DatabaseSession:
    """Context manager for the database. Encapsulates the initialization, creation of tables and their closure."""

    def __init__(self, db_path: Union[Union[str, Path], Literal[":memory:"]]):
        self.db_path = Path(db_path) if db_path != ":memory:" else db_path
        self.db = None

    def __enter__(self):
        if db_proxy.obj is not None:
            current_db_path = getattr(db_proxy.obj, "database", None)
            if current_db_path == str(self.db_path) and not db_proxy.is_closed():
                return db_proxy

            if not db_proxy.obj.is_closed():
                db_proxy.obj.close()

        if isinstance(self.db_path, Path):
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db = peewee.SqliteDatabase(
            str(self.db_path),
            pragmas={"journal_mode": "wal", "cache_size": -1024 * 64},
            timeout=10,
        )

        db_proxy.initialize(self.db)
        self.db.connect()

        from tartape.models import TapeMetadata, Track

        db_proxy.create_tables(
            [Track, TapeMetadata],
            safe=True,
        )
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db and not self.db.is_closed():
            self.db.close()

        if db_proxy.obj and not db_proxy.obj.is_closed():
            db_proxy.obj.close()

    def start(self):
        return self.__enter__()

    def close(self):
        return self.__exit__(None, None, None)
