import sqlite3
from typing import Generator, Optional

from .schemas import TarEntry


class SqlInventory:
    """
    SQLite-based inventory engine.
    Ensures determinism (ORDER BY) and constant memory.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path)
        self._setup()
        self._batch_count = 0

    def _setup(self):
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA journal_mode = MEMORY")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory (
                arc_path TEXT PRIMARY KEY,
                data TEXT
            )
        """
        )
        self.conn.commit()

    def add(self, entry: TarEntry):
        self.conn.execute(
            "INSERT OR REPLACE INTO inventory (arc_path, data) VALUES (?, ?)",
            (entry.arc_path, entry.model_dump_json()),
        )
        self._batch_count += 1

        if self._batch_count >= 1000:
            self.conn.commit()
            self._batch_count = 0

    def get_entries(
        self, start_after: Optional[str] = None
    ) -> Generator[TarEntry, None, None]:
        self.conn.commit()

        query = "SELECT data FROM inventory "
        params = []
        if start_after:
            query += "WHERE arc_path > ? "
            params.append(start_after)

        # Determinism lives here: ORDER BY arc_path
        query += "ORDER BY arc_path ASC"

        cursor = self.conn.cursor()
        for (data_json,) in cursor.execute(query, params):
            yield TarEntry.model_validate_json(data_json)

    def count(self) -> int:
        cursor = self.conn.execute("SELECT COUNT(*) FROM inventory")
        return cursor.fetchone()[0]

    def commit(self):
        if self._batch_count > 0:
            self.conn.commit()
            self._batch_count = 0
