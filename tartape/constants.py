TAR_BLOCK_SIZE = 512
TAR_FOOTER_SIZE = 1024
CHUNK_SIZE_DEFAULT = 64 * 1024  # 64KB para lectura de disco

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
