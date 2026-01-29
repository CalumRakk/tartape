import os
import stat as stat_module
from pathlib import Path
from typing import Generator, List, Optional, Tuple

try:
    import grp
    import pwd
except ImportError:
    pwd = None
    grp = None

from .core import TarStreamGenerator
from .schemas import TarEntry, TarEvent


class TarEntryFactory:
    """
    Exclusively responsible for inspecting the file system
    and instantiating valid TarEntry objects.

    Centralizes:
    1. Usage of lstat (to avoid following symlinks).
    2. Type filtering (Only File, Dir, Link are supported).
    3. Metadata extraction (Users, Groups, Permissions).
    """

    @classmethod
    def create(cls, source_path: Path, arcname: str) -> Optional[TarEntry]:
        """
        Analyzes a path and creates a TarEntry.
        Returns None if the file is an unsupported type (Socket, Pipe, etc).
        Raises OSError/FileNotFoundError if there are access issues.
        """
        st = source_path.lstat()
        mode = st.st_mode

        is_dir, is_file, is_symlink = cls._diagnose_type(mode)

        if not (is_dir or is_file or is_symlink):
            return None

        file_mode, uid, gid, uname, gname = cls._extract_metadata(st)

        linkname = ""
        size = st.st_size

        if is_symlink:
            linkname = os.readlink(source_path)
            size = 0  # In TAR, symlinks have a size of 0
        elif is_dir:
            size = 0  # Directories have a size of 0 in the TAR header

        return TarEntry(
            source_path=str(source_path.absolute()),
            arc_path=arcname,
            size=size,
            mtime=st.st_mtime,
            is_dir=is_dir,
            is_symlink=is_symlink,
            linkname=linkname,
            mode=file_mode,
            uid=uid,
            gid=gid,
            uname=uname,
            gname=gname,
        )

    @staticmethod
    def _diagnose_type(mode: int) -> Tuple[bool, bool, bool]:
        """Returns (is_dir, is_reg, is_symlink) based on the mode."""
        return (
            stat_module.S_ISDIR(mode),
            stat_module.S_ISREG(mode),
            stat_module.S_ISLNK(mode),
        )

    @staticmethod
    def _extract_metadata(st: os.stat_result) -> Tuple[int, int, int, str, str]:
        """Safely extracts mode, uid, gid, uname, and gname."""
        # S_IMODE clears type bits (e.g., removes the "I am a directory" bit)
        # keeping only the permissions (e.g., 0o755).
        mode = stat_module.S_IMODE(st.st_mode)

        uid = st.st_uid
        gid = st.st_gid
        uname = ""
        gname = ""

        if pwd:
            try:
                uname = pwd.getpwuid(uid).pw_name  # type: ignore
            except (KeyError, AttributeError):
                uname = str(uid)

        if grp:
            try:
                gname = grp.getgrgid(gid).gr_name  # type: ignore
            except (KeyError, AttributeError):
                gname = str(gid)

        return mode, uid, gid, uname, gname


class TarTape:
    """User-friendly interface for recording a TAR tape."""

    def __init__(self):
        self._entries: List[TarEntry] = []

    def add_folder(self, folder_path: str | Path, recursive: bool = True):
        """Scans a folder and adds its contents to the archive."""
        root = Path(folder_path)

        # Add the root folder itself
        self.add_file(root, arcname=root.name)

        pattern = "**/*" if recursive else "*"
        for p in root.glob(pattern):
            try:
                rel_path = p.relative_to(root.parent)
                self.add_file(p, arcname=rel_path.as_posix())
            except (ValueError, OSError):
                # If glob lists something inaccessible, skip it rather than failing the whole process.
                continue

    def add_file(self, source_path: str | Path, arcname: str | None = None):
        """Adds a single file/entry to the tape.

        Args:
            source_path: Physical path to the file.
            arcname: Target path inside the TAR archive.

        Returns:
            None
        """
        p = Path(source_path)
        name = arcname or p.name
        # Ensure path uses Unix-style separators
        name_unix = name.replace("\\", "/")

        entry = TarEntryFactory.create(p, name_unix)
        if entry:
            self._entries.append(entry)
        # If entry is None, it was silently ignored (Socket/Pipe/etc)

    def stream(self, chunk_size: int = 64 * 1024) -> Generator[TarEvent, None, None]:
        """Starts the recording and emits the stream of events/bytes."""
        engine = TarStreamGenerator(self._entries, chunk_size=chunk_size)
        yield from engine.stream()
