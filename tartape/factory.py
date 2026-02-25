import logging
import os
import stat as stat_module
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

from tartape.models import Track

try:
    import grp
    import pwd
except ImportError:
    pwd = None
    grp = None


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
    def create_track(
        cls,
        source_path: Union[Path, str],
        rel_path: str,
        arcname: str,
        anonymize: bool = True,
    ) -> Optional[Track]:
        """
        Analyzes a path and creates a TarEntry.
        Returns None if the file is an unsupported type (Socket, Pipe, etc).
        Raises OSError/FileNotFoundError if there are access issues.
        """
        source_path = Path(source_path)
        st = source_path.lstat()
        mode = st.st_mode

        is_dir, is_file, is_symlink = cls._diagnose_type(mode)

        if not (is_dir or is_file or is_symlink):
            return None

        file_mode, uid, gid, uname, gname = cls._extract_metadata(st)
        if anonymize:
            uid, gid, uname, gname = 0, 0, "root", "root"

        linkname = ""
        size = st.st_size

        if is_symlink:
            linkname = os.readlink(source_path)
            size = 0  # In TAR, symlinks have a size of 0
        elif is_dir:
            size = 0  # Directories have a size of 0 in the TAR header

        return Track(
            arc_path=arcname,
            rel_path=rel_path,
            size=size,
            mtime=int(st.st_mtime),
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
