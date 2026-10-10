import sqlite3
from pathlib import Path
from typing import Literal

import peewee

db_proxy = peewee.Proxy()


class DatabaseSession:
    """Context manager encapsulating database initialization and closure."""

    def __init__(self, db_path: str | Path | Literal[":memory:"]):
        self.db_path = Path(db_path) if db_path != ":memory:" else db_path
        self._depth = 0
        self._tables_initialized = False

        self.db = peewee.SqliteDatabase(
            str(self.db_path),
            pragmas={
                "journal_mode": "wal",
                "cache_size": -1024 * 64,  # 64MB cache
                "foreign_keys": 1,
                "synchronous": "NORMAL",
                "busy_timeout": 30000,  # 30s timeout for concurrent workers
            },
            timeout=30,
        )
        from tartape.models import LayoutRecord, TapeMetadata, Track, VolumeRecord

        self._models = [Track, TapeMetadata, LayoutRecord, VolumeRecord]
        self.db.bind(self._models, bind_refs=True, bind_backrefs=True)

    def __enter__(self):
        try:
            if self._depth == 0:
                if self.db.is_closed():
                    self.db.connect()
                if not self._tables_initialized:
                    self.db.create_tables(self._models, safe=True)
                    self._tables_initialized = True
            self._depth += 1
            return self.db
        except (peewee.DatabaseError, sqlite3.DatabaseError) as e:
            from tartape.exceptions import TapeCorruptedError

            raise TapeCorruptedError(
                f"Corrupted or invalid database at '{self.db_path}': {e}"
            ) from e

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._depth = max(0, self._depth - 1)
        if self._depth == 0 and not self.db.is_closed():
            try:
                self.db.close()
            except Exception:
                pass

        if exc_type and issubclass(
            exc_type, (peewee.DatabaseError, sqlite3.DatabaseError)
        ):
            from tartape.exceptions import TapeCorruptedError

            raise TapeCorruptedError(
                f"Database error at '{self.db_path}': {exc_val}"
            ) from exc_val

    def connect(self):
        return self.__enter__()

    def close(self):
        self._depth = 0
        if not self.db.is_closed():
            self.db.close()


def seal_database(db_path: str | Path) -> None:
    """Finalize and seal an SQLite database file.

    Checkpoints WAL journal, switches to DELETE mode, vacuums unused pages,
    and removes lingering WAL/SHM artifacts to ensure a single standalone file.
    """
    path = Path(db_path)
    if not path.exists() or not path.is_file():
        return

    raw_db = peewee.SqliteDatabase(
        str(path),
        timeout=30,
    )
    raw_db.connect(reuse_if_open=True)
    try:
        raw_db.execute_sql("PRAGMA wal_checkpoint(TRUNCATE);")
        raw_db.execute_sql("PRAGMA journal_mode = DELETE;")
        raw_db.execute_sql("PRAGMA VACUUM;")
        raw_db.execute_sql("PRAGMA optimize;")
    finally:
        if not raw_db.is_closed():
            raw_db.close()

    for suffix in ("-wal", "-shm", ".db-wal", ".db-shm"):
        aux_file = path.parent / f"{path.name}{suffix}"
        if aux_file.exists():
            aux_file.unlink(missing_ok=True)
