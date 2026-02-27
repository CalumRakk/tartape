from pathlib import Path
from typing import Literal, Union

import peewee

db_proxy = peewee.Proxy()


class DatabaseSession:
    """Context manager for the database. Encapsulates the initialization, creation of tables and their closure."""

    def __init__(self, db_path: Union[Union[str, Path], Literal[":memory:"]]):
        self.db_path = Path(db_path) if db_path != ":memory:" else db_path
        self.db = peewee.SqliteDatabase(
            str(self.db_path),
            pragmas={
                "journal_mode": "wal",
                "cache_size": -1024 * 64,      # 64MB cache
                "foreign_keys": 1,
                "synchronous": "NORMAL",
            },
            timeout=10,
        )
        from tartape.models import Track, TapeMetadata
        self._models = [Track, TapeMetadata]
        # 'bind' Ignore what your Meta.database (the proxy) says and use ME directly
        self.db.bind(self._models, bind_refs=True, bind_backrefs=True)

    def __enter__(self):
        if self.db.is_closed():
            self.db.connect()
        self.db.create_tables(self._models, safe=True)
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.db.is_closed():
            self.db.close()

    def connect(self):
        return self.__enter__()

    def close(self):
        return self.__exit__(None, None, None)
