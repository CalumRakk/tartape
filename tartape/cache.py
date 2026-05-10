import hashlib
import logging
import os
from pathlib import Path
from typing import Optional

import peewee

from tartape.constants import CACHE_DIR_NAME, CACHE_MAX_FILES, CACHE_MAX_SIZE_MB

logger = logging.getLogger(__name__)

# Dedicated database instance for the cache.
# This completely isolates it from the main Tape index (db_proxy).
cache_db = peewee.SqliteDatabase(None)


class HashStore(peewee.Model):
    """Peewee model for the cache table."""
    arc_path = peewee.CharField()
    size = peewee.IntegerField()
    mtime = peewee.IntegerField()
    md5sum = peewee.CharField()

    class Meta:
        database = cache_db
        # ADR-002: Unique composite key to handle file mutations naturally
        primary_key = peewee.CompositeKey("arc_path", "size", "mtime")


def get_global_cache_dir() -> Path:
    if os.name == "nt":
        local_app_data = os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))
        base_dir = Path(local_app_data) / "tartape"
    else:
        base_dir = Path.home() / ".tartape"

    cache_dir = base_dir / CACHE_DIR_NAME
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def generate_cache_db_name(target_directory: Path | str) -> str:
    abs_path = str(Path(target_directory).resolve())
    path_hash = hashlib.sha1(abs_path.encode("utf-8")).hexdigest()
    return f"cache_{path_hash}.db"


class HashCacheManager:
    # 100 MB threshold. If we compute hashes for data exceeding this weight, we save to disk.
    FLUSH_THRESHOLD_BYTES = 100 * 1024 * 1024

    def __init__(self, target_directory: Path | str):
        self.cache_dir = get_global_cache_dir()
        self._enforce_retention_policy()

        db_name = generate_cache_db_name(target_directory)
        self.db_path = self.cache_dir / db_name

        # Initialize and connect the isolated Peewee DB
        cache_db.init(
            str(self.db_path),
            pragmas={
                "journal_mode": "wal",
                "synchronous": "NORMAL",
            }
        )
        cache_db.connect(reuse_if_open=True)
        cache_db.create_tables([HashStore], safe=True)

        # Smart Batching state
        self._batch = []
        self._accumulated_weight = 0

    def _enforce_retention_policy(self) -> None:
        try:
            db_files = list(self.cache_dir.glob("*.db"))
            files_with_stats = []
            total_size_bytes = 0

            for f in db_files:
                try:
                    stat = f.stat()
                    files_with_stats.append((f, stat.st_mtime, stat.st_size))
                    total_size_bytes += stat.st_size
                except OSError:
                    continue

            max_size_bytes = CACHE_MAX_SIZE_MB * 1024 * 1024
            files_with_stats.sort(key=lambda x: x[1])
            files_count = len(files_with_stats)

            for f_path, _, f_size in files_with_stats:
                f_path: Path

                if files_count <= CACHE_MAX_FILES and total_size_bytes <= max_size_bytes:
                    break

                try:
                    # TODO: Is this trick safe? Analyze whether this could cause any borde cases
                    wal_file = f_path.with_suffix(".db-wal")
                    shm_file = f_path.with_suffix(".db-shm")

                    f_path.unlink()
                    if wal_file.exists():
                        wal_file.unlink()
                    if shm_file.exists():
                        shm_file.unlink()

                    files_count -= 1
                    total_size_bytes -= f_size
                    logger.debug(f"Cache GC: Evicted {f_path.name}")
                except OSError:
                    pass

        except Exception as e:
            logger.warning(f"Failed to enforce hash cache retention policy: {e}")

    def get_hash(self, arc_path: str, size: int, mtime: int) -> Optional[str]:
        try:
            record = HashStore.get(
                (HashStore.arc_path == arc_path) &
                (HashStore.size == size) &
                (HashStore.mtime == mtime)
            )
            return record.md5sum
        except HashStore.DoesNotExist: # type: ignore
            return None

    def save_hash(self, arc_path: str, size: int, mtime: int, md5sum: str) -> None:
        """
        Queues a hash to be saved. If the computational weight (bytes hashed)
        exceeds the threshold, it triggers an immediate flush.
        """
        self._batch.append({
            "arc_path": arc_path,
            "size": size,
            "mtime": mtime,
            "md5sum": md5sum
        })
        self._accumulated_weight += size

        if self._accumulated_weight >= self.FLUSH_THRESHOLD_BYTES:
            self.flush()

    def flush(self) -> None:
        """Writes the queued hashes to the database using an atomic transaction."""
        if not self._batch:
            return

        with cache_db.atomic():
            HashStore.insert_many(self._batch).on_conflict_replace().execute()

        self._batch = []
        self._accumulated_weight = 0

    def close(self) -> None:
        """Ensures pending hashes are saved and safely closes the DB."""
        self.flush()
        if not cache_db.is_closed():
            cache_db.close()
