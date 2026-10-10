TAR_BLOCK_SIZE = 512
TAR_FOOTER_SIZE = 1024
CHUNK_SIZE_DEFAULT = 64 * 1024  # 64KB for disk reads

# Sidecar metadata extension (zero-pollution)
TAPE_EXTENSION = ".tartape"

# Legacy directory name (kept for backwards compatibility detection)
TAPE_METADATA_DIR = ".tartape"
TAPE_DB_NAME = "index.db"

DEFAULT_EXCLUDES = [
    ".DS_Store",
    "Thumbs.db",
    "__pycache__",
    "*.db-wal",
    "*.db-shm",
    "*.sock",
]

# --- Cache Configuration ---
CACHE_DIR_NAME = "hash_cache"
CACHE_MAX_FILES = 10
CACHE_MAX_SIZE_MB = 1024  # 1 GB hard limit
