import logging
import os
import stat as stat_module
from pathlib import Path
from typing import Callable, List, Optional, Union

from tartape.models import Track
from tartape.schemas import DiskEntryStats

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

    @staticmethod
    def inspect(path: Path) -> DiskEntryStats:
        """
        Single point of file inspection on the system.
        """
        try:
            st = path.lstat()

            # Extract only the permission bits (0o755, 0o644, etc.)
            permissions = stat_module.S_IMODE(st.st_mode)

            # Identify the object type using the full st_mode
            is_dir = stat_module.S_ISDIR(st.st_mode)
            is_file = stat_module.S_ISREG(st.st_mode)
            is_symlink = stat_module.S_ISLNK(st.st_mode)

            # Securely extract usernames/group names
            uname, gname = "", ""
            if pwd:
                try: uname = pwd.getpwuid(st.st_uid).pw_name # type: ignore
                except (KeyError, AttributeError): uname = str(st.st_uid)
            if grp:
                try: gname = grp.getgrgid(st.st_gid).gr_name # type: ignore
                except (KeyError, AttributeError): gname = str(st.st_gid)

            return DiskEntryStats(
                exists=True,
                size=st.st_size if not is_dir else 0,
                mtime=int(st.st_mtime),
                mode=permissions,
                uid=st.st_uid,
                gid=st.st_gid,
                uname=uname,
                gname=gname,
                is_dir=is_dir,
                is_file=is_file,
                is_symlink=is_symlink,
                linkname=os.readlink(path) if is_symlink else ""
            )
        except (FileNotFoundError, ProcessLookupError):
            return DiskEntryStats(exists=False)


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
        stats = cls.inspect(Path(source_path))
        if not stats.exists or not (stats.is_dir or stats.is_file or stats.is_symlink):
            return None

        linkname = ""
        size = stats.size

        if stats.is_symlink:
            linkname = os.readlink(source_path)
            size = 0  # In TAR, symlinks have a size of 0
        elif stats.is_dir:
            size = 0  # Directories have a size of 0 in the TAR header

        return Track(
            arc_path=arcname,
            rel_path=rel_path,
            size=size,
            mtime=int(stats.mtime),
            is_dir=stats.is_dir,
            is_symlink=stats.is_symlink,
            linkname=linkname,
            mode=stats.mode,
            uid=0 if anonymize else stats.uid,
            gid=0 if anonymize else stats.gid,
            uname="root" if anonymize else stats.uname,
            gname="root" if anonymize else stats.gname,
        )
