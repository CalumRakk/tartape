import logging
import os
import stat as stat_module
from pathlib import Path
from typing import Callable, Generator, List, Optional, Tuple, Union

try:
    import grp
    import pwd
except ImportError:
    pwd = None
    grp = None

from .core import TarStreamGenerator
from .schemas import TarEntry, TarEvent

ExcludeType = Union[str, List[str], Callable[[Path], bool]]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


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

    def _should_exclude(self, path: Path, exclude: Optional[ExcludeType]) -> bool:
        """Determines if a path should be skipped based on the 'exclude' parameter."""
        if exclude is None:
            return False

        # Function or Lambda
        if callable(exclude):
            return exclude(path)

        # String único - glob patterns
        if isinstance(exclude, str):
            return path.match(exclude) or path.name == exclude

        # List strings
        if isinstance(exclude, list):
            return any(path.match(p) or path.name == p for p in exclude)

        return False

    def add_folder(
        self,
        folder_path: str | Path,
        recursive: bool = True,
        exclude: Optional[ExcludeType] = None,
    ):
        """Scans a folder and adds its contents to the archive."""
        root_path = Path(folder_path).absolute()
        if not root_path.is_dir():
            raise ValueError(
                f"The path '{folder_path}' is not a directory or does not exist."
            )

        if not self._should_exclude(root_path, exclude):
            self.add_file(root_path, arcname=root_path.name)
            self._scan_and_add(root_path, root_path.name, recursive, exclude)

    def _scan_and_add(
        self,
        current_path: Path,
        arc_prefix: str,
        recursive: bool,
        exclude: Optional[ExcludeType],
    ):
        """Recursively scans a folder and adds its contents to the archive."""
        try:
            with os.scandir(current_path) as it:
                for entry in it:
                    entry_path = Path(entry.path)
                    if self._should_exclude(entry_path, exclude):
                        logger.debug(f"Excluding path: {entry_path}")
                        continue

                    entry_arcname = f"{arc_prefix}/{entry.name}"
                    self.add_file(entry_path, arcname=entry_arcname)

                    if recursive and entry.is_dir() and not entry.is_symlink():
                        self._scan_and_add(
                            entry_path, entry_arcname, recursive, exclude
                        )

        except PermissionError as e:
            logger.warning(f"Permission denied: {current_path}. Skipping directory.")
            return

    def add_file(
        self,
        source_path: str | Path,
        arcname: str | None = None,
        exclude: Optional[ExcludeType] = None,
    ):
        """Adds a single file/entry to the tape.

        Args:
            source_path: Physical path to the file.
            arcname: Target path inside the TAR archive.

        Returns:
            None
        """
        p = Path(source_path)
        if self._should_exclude(p, exclude):
            logger.debug(f"Excluding file: {p}")
            return

        name = arcname or p.name
        # Ensure path uses Unix-style separators
        name_unix = name.replace("\\", "/")

        entry = TarEntryFactory.create(p, name_unix)
        if entry:
            self._entries.append(entry)
        else:
            # If entry is None, it was silently ignored (Socket/Pipe/etc)
            logger.info(f"Skipping unsupported file type: {p}")

    def stream(self, chunk_size: int = 64 * 1024) -> Generator[TarEvent, None, None]:
        """Starts the recording and emits the stream of events/bytes."""
        engine = TarStreamGenerator(self._entries, chunk_size=chunk_size)
        yield from engine.stream()
