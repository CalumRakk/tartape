import hashlib
import logging
import os
import stat as stat_module
from pathlib import Path
from typing import Callable, List, Optional, Union

from tartape.constants import TAPE_METADATA_DIR
from tartape.exceptions import TarIntegrityError
from tartape.models import Track
from tartape.schemas import DiskEntryStats, EntryMetadata

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
    def validate_path_constraints(arcname: str):
        """
        Validates ADR-005 constraints during the recording phase.
        Ensures that the path will be compatible with USTAR and TarTape
        before adding it to the catalog.
        """
        path_bytes = arcname.encode("utf-8")

        # USTAR absolute limit
        if len(path_bytes) > 255:
            raise ValueError(
                f"Path too long ({len(path_bytes)} bytes). Max 255 allowed by USTAR."
            )

        # ADR-005: Component limit (100 bytes)
        components = arcname.split("/")
        for component in components:
            if len(component.encode("utf-8")) > 100:
                raise ValueError(
                    f"ADR-005 Violation: Path component '{component}' exceeds 100 bytes. "
                    "This is required to ensure directory metadata integrity."
                )

    @staticmethod
    def calculate_md5(path: Path) -> str:
        """Calculate the MD5 hash of a file in 64 KB blocks."""
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(64 * 1024), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def inspect(path: Path) -> DiskEntryStats:
        """Performs low-level lstat on the path and returns raw stats."""
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
                try:
                    uname = pwd.getpwuid(st.st_uid).pw_name  # type: ignore
                except (KeyError, AttributeError):
                    uname = str(st.st_uid)
            if grp:
                try:
                    gname = grp.getgrgid(st.st_gid).gr_name  # type: ignore
                except (KeyError, AttributeError):
                    gname = str(st.st_gid)

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
                linkname=os.readlink(path) if is_symlink else "",
            )
        except (FileNotFoundError, ProcessLookupError):
            return DiskEntryStats(exists=False)

    @classmethod
    def create_metadata(
        cls,
        source_path: Union[Path, str],
        rel_path: str,
        arcname: str,
        anonymize: bool = True,
        calculate_hash: bool = False,
    ) -> Optional[EntryMetadata]:
        """
        Analyzes a path and creates a TarEntry.
        Returns None if the file is an unsupported type (Socket, Pipe, etc).
        Raises OSError/FileNotFoundError if there are access issues.
        """
        cls.validate_path_constraints(arcname)

        path = Path(source_path)
        stats = cls.inspect(path)

        if not stats.exists or not (stats.is_dir or stats.is_file or stats.is_symlink):
            return None

        # Determine link target for symlinks
        linkname = os.readlink(path) if stats.is_symlink else ""

        # Directories and symlinks have 0 size in TAR headers
        effective_size = 0 if (stats.is_dir or stats.is_symlink) else stats.size

        uid = 0 if anonymize else stats.uid
        gid = 0 if anonymize else stats.gid
        uname = "root" if anonymize else stats.uname
        gname = "root" if anonymize else stats.gname

        md5_value = None
        if calculate_hash and stats.is_file:
            md5_value = cls.calculate_md5(Path(source_path))

        return EntryMetadata(
            arc_path=arcname,
            rel_path=rel_path,
            size=effective_size,
            mtime=int(stats.mtime),
            mode=stats.mode,
            uid=uid,
            gid=gid,
            uname=uname,
            gname=gname,
            is_dir=stats.is_dir,
            is_symlink=stats.is_symlink,
            linkname=linkname,
            md5sum=md5_value,
        )


def validate_integrity(
    expected: EntryMetadata | Track, tape_root_directory: Path
) -> None:
    """
    Strict implementation of ADR-002.
    Compares the expected pure metadata against the current physical disk state.
    Raises TarIntegrityError if any discrepancy is found.
    """
    full_disk_path = Path(tape_root_directory) / expected.rel_path
    stats = TarEntryFactory.inspect(full_disk_path)

    if not stats.exists:
        raise TarIntegrityError(f"File missing: {expected.arc_path}")

    # ADR-002: Directory structural integrity
    if expected.is_dir:
        if expected.rel_path in ("", "."):
            return  # Root directory mtime is ignored
        if stats.mtime != expected.mtime:
            raise TarIntegrityError(f"Directory structure changed: {expected.arc_path}")
        return

    # ADR-002: File integrity
    if stats.mtime != expected.mtime:
        raise TarIntegrityError(f"File modified (mtime): {expected.arc_path}")

    if not expected.is_symlink:
        if stats.size != expected.size:
            raise TarIntegrityError(f"File size changed: {expected.arc_path}")


def validate_root_structure_integrity(root_path: Path) -> None:
    """
    Checks if the root directory structure has been compromised by adding
    new untracked items. This complements ADR-002, where the root
    mtime is ignored.
    """
    try:
        disk_items_count = 0
        with os.scandir(root_path) as it:
            for entry in it:
                if entry.name != TAPE_METADATA_DIR:
                    disk_items_count += 1
    except OSError as e:
        raise TarIntegrityError(f"Root directory is inaccessible: {e}")

    db_items_count = (
        Track.select()
        .where((Track.rel_path != "") & (~Track.rel_path.contains("/")))  # type: ignore
        .count()
    )

    if disk_items_count > db_items_count:
        diff = disk_items_count - db_items_count
        raise TarIntegrityError(
            f"Integrity compromised: {diff} untracked item(s) detected in root directory. "
            f"The dataset no longer matches the T0 snapshot."
        )
