import sqlite3
from pathlib import Path
from typing import Literal

import peewee

db_proxy = peewee.Proxy()


class DatabaseSession:
    """Context manager encapsulating database initialization, concurrency, and clean closure."""

    def __init__(
        self,
        db_path: str | Path | Literal[":memory:"],
        read_only: bool = False,
    ):
        self._read_only = read_only
        self._depth = 0
        self._tables_initialized = False

        if db_path == ":memory:":
            self.db_path = ":memory:"
            db_target = ":memory:"
            uri = False
            pragmas = {
                "journal_mode": "memory",
                "foreign_keys": 1,
            }
        else:
            self.db_path = Path(db_path).resolve()
            if self._read_only:
                # Open with URI mode=ro: SQLite will not create or touch WAL/SHM files
                db_target = f"file:{self.db_path.as_posix()}?mode=ro"
                uri = True
                pragmas = {
                    "query_only": 1,
                    "foreign_keys": 1,
                    "cache_size": -1024 * 64,
                    "busy_timeout": 30000,
                }
            else:
                db_target = str(self.db_path)
                uri = False
                pragmas = {
                    "journal_mode": "wal",
                    "cache_size": -1024 * 64,  # 64MB cache
                    "foreign_keys": 1,
                    "synchronous": "NORMAL",
                    "busy_timeout": 30000,  # 30s timeout for concurrent workers
                }

        self.db = peewee.SqliteDatabase(
            db_target,
            uri=uri,
            pragmas=pragmas,
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
                if not self._read_only and not self._tables_initialized:
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
            self._cleanup_on_close()

        if exc_type and issubclass(
            exc_type, (peewee.DatabaseError, sqlite3.DatabaseError)
        ):
            from tartape.exceptions import TapeCorruptedError

            raise TapeCorruptedError(
                f"Database error at '{self.db_path}': {exc_val}"
            ) from exc_val

    def _cleanup_on_close(self) -> None:
        """Perform a clean WAL checkpoint and close the database connection."""
        if not self.db.is_closed():
            if not self._read_only and self.db_path != ":memory:":
                try:
                    # Truncates WAL file back to 0 bytes so folder stays pristine
                    self.db.execute_sql("PRAGMA wal_checkpoint(TRUNCATE);")
                except Exception:
                    pass
            try:
                self.db.close()
            except Exception:
                pass

    def connect(self):
        return self.__enter__()

    def close(self):
        self._depth = 0
        self._cleanup_on_close()


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
