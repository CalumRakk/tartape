import hashlib
import logging
import os
import stat as stat_module
from pathlib import Path
from typing import TYPE_CHECKING, Callable, List, Optional, Union

from tartape.constants import TAPE_METADATA_DIR
from tartape.exceptions import PathConstraintError, TarIntegrityError
from tartape.models import Track
from tartape.schemas import DiskEntryStats, EntryMetadata

if TYPE_CHECKING:
    from tartape.cache import HashCacheManager

try:
    import grp
    import pwd
except ImportError:
    pwd = None
    grp = None


ExcludeType = Union[str, List[str], Callable[[Path], bool]]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def validate_ustar_path(
    arcname: str, is_dir: bool = False
) -> tuple[bool, Optional[str]]:
    """
    Verifies whether a route strictly complies with the USTAR (155/100) splitting rules and the 255-byte total limit.
    """
    target = arcname if not is_dir or arcname.endswith("/") else arcname + "/"
    target_bytes = target.encode("utf-8")

    if len(target_bytes) > 255:
        return (
            False,
            f"Total path exceeds USTAR 255 byte limit ({len(target_bytes)} > 255 bytes)",
        )

    components = arcname.strip("/").split("/")
    max_leaf = 99 if is_dir else 100
    for i, comp in enumerate(components):
        limit = max_leaf if i == len(components) - 1 else 100
        comp_len = len(comp.encode("utf-8"))
        if comp_len > limit:
            return False, f"Component '{comp}' exceeds {limit} bytes ({comp_len} bytes)"

    if len(target_bytes) <= 100:
        return True, None

    # Check if there is at least one split at '/' that satisfies prefix <= 155 and name <= 100
    has_valid_cut = False
    for i, char in enumerate(target):
        if char == "/":
            prefix = target[:i]
            name = target[i + 1 :]
            if not name:
                continue
            if len(prefix.encode("utf-8")) <= 155 and len(name.encode("utf-8")) <= 100:
                has_valid_cut = True
                break

    if not has_valid_cut:
        return (
            False,
            "Path cannot be split into USTAR prefix (<= 155 bytes) and name (<= 100 bytes)",
        )

    return True, None


def shorten_path_ustar(arcname: str, is_dir: bool = False) -> str:
    """
    Deterministically shortens a path to comply with USTAR.
    Preserves the root and the final name, collapsing intermediate directories into a hash.
    """
    components = arcname.split("/")
    max_leaf_bytes = 99 if is_dir else 100

    # Ensure that no individual component exceeds 100 bytes.
    clean_components = []
    for i, comp in enumerate(components):
        limit = max_leaf_bytes if i == len(components) - 1 else 100
        if len(comp.encode("utf-8")) > limit:
            comp = truncate_component_safe(comp, limit)
        clean_components.append(comp)

    candidate = "/".join(clean_components)
    valid, _ = validate_ustar_path(candidate, is_dir=is_dir)
    if valid:
        return candidate

    # If it is only one component (e.g., root)
    if len(clean_components) == 1:
        comp = clean_components[0]
        if len(comp.encode("utf-8")) > max_leaf_bytes:
            comp = truncate_component_safe(comp, max_leaf_bytes)
        return comp

    # Separate sheet and prefix
    leaf = clean_components[-1]
    prefix_components = clean_components[:-1]
    full_prefix = "/".join(prefix_components)

    # Maximum limit for the prefix, ensuring the total is <= 255 (or 254 if it is a folder)
    max_total = 254 if is_dir else 255
    max_prefix_bytes = min(155, max_total - 1 - len(leaf.encode("utf-8")))

    # Deterministic hash of the original prefix path
    prefix_hash = hashlib.md5(full_prefix.encode("utf-8")).hexdigest()[:8]
    marker = f"~{prefix_hash}"

    root = prefix_components[0]
    # If the root alone is too long, truncate it to fit the marker
    min_root_space = max_prefix_bytes - len(marker.encode("utf-8")) - 1
    if len(root.encode("utf-8")) > min_root_space:
        root = truncate_component_safe(root, max(10, min_root_space))

    base_prefix = f"{root}/{marker}"
    current_bytes = len(base_prefix.encode("utf-8"))

    # Try to fit in as many final immediate folders as possible
    tail_candidates = prefix_components[1:]
    chosen_tail = []

    for comp in reversed(tail_candidates):
        comp_bytes = len(comp.encode("utf-8"))
        if current_bytes + 1 + comp_bytes <= max_prefix_bytes:
            chosen_tail.insert(0, comp)
            current_bytes += 1 + comp_bytes
        else:
            break

    if chosen_tail:
        shortened_prefix = f"{base_prefix}/" + "/".join(chosen_tail)
    else:
        shortened_prefix = base_prefix

    return f"{shortened_prefix}/{leaf}"


def truncate_component_safe(component: str, max_bytes: int = 100) -> str:
    """
    Truncates a path component to a maximum byte length, ensuring
    UTF-8 validity and preventing name collisions via hashing.
    """
    comp_bytes = component.encode("utf-8")

    if len(comp_bytes) <= max_bytes:
        return component

    hash_suffix = hashlib.md5(comp_bytes).hexdigest()[:14]
    limit_for_prefix = max_bytes - 15

    prefix_bytes = comp_bytes[:limit_for_prefix]

    # Decode back to string. 'ignore' is crucial: if byte 85 was the
    # start of a 4-byte emoji, it will be dropped, preventing
    # "Invalid UTF-8" errors.
    safe_prefix = prefix_bytes.decode("utf-8", errors="ignore")

    result = f"{safe_prefix}_{hash_suffix}"

    # If this fails, we decrease the prefix length further.
    # This handles edge cases with certain multi-byte combining characters.
    while len(result.encode("utf-8")) > max_bytes:
        safe_prefix = safe_prefix[:-1]
        result = f"{safe_prefix}_{hash_suffix}"

    return result


class TarEntryFactory:
    """
    Exclusively responsible for inspecting the file system
    and instantiating valid TarEntry objects.

    Centralizes:
    1. Usage of lstat (to avoid following symlinks).f
    2. Type filtering (Only File, Dir, Link are supported).
    3. Metadata extraction (Users, Groups, Permissions).
    """

    @staticmethod
    def resolve_arcname(
        arcname: str, auto_truncate: bool = False, is_dir: bool = False
    ) -> str:
        """
        Validates USTAR restrictions. If auto_truncate is True,
        it deterministically truncates the path.
        """
        valid, reason = validate_ustar_path(arcname, is_dir=is_dir)
        if valid:
            return arcname

        if not auto_truncate:
            raise PathConstraintError(reason or "Path violates USTAR constraints.")

        resolved = shorten_path_ustar(arcname, is_dir=is_dir)

        valid_final, reason_final = validate_ustar_path(resolved, is_dir=is_dir)
        if not valid_final:
            raise PathConstraintError(
                f"Critical failure: Path '{resolved}' still violates constraints: {reason_final}"
            )

        return resolved

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
            raise PathConstraintError(
                f"Path too long ({len(path_bytes)} bytes). Max 255 allowed by USTAR."
            )

        # ADR-005: Component limit (100 bytes)
        components = arcname.split("/")
        for component in components:
            if len(component.encode("utf-8")) > 100:
                raise PathConstraintError(
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
    def inspect(
        path: Path, precomputed_stat: Optional[os.stat_result] = None
    ) -> DiskEntryStats:
        """
        Performs low-level lstat on the path, or uses a precomputed stat result
        (e.g., from os.scandir) to avoid redundant syscalls.
        """
        try:
            st = precomputed_stat if precomputed_stat else path.lstat()

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
        precomputed_stat: Optional[os.stat_result] = None,
        cache_manager: Optional["HashCacheManager"] = None,
    ) -> Optional[EntryMetadata]:
        """
        Analyzes a path and creates a TarEntry.
        Accepts a precomputed_stat to avoid double stat syscalls during discovery.

        Returns None if the file is an unsupported type (Socket, Pipe, etc).
        Raises OSError/FileNotFoundError if there are access issues.
        """

        path = Path(source_path)
        stats = cls.inspect(path, precomputed_stat=precomputed_stat)

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
            if cache_manager:
                md5_value = cache_manager.get_hash(
                    arcname, effective_size, int(stats.mtime)
                )
            if not md5_value:
                md5_value = cls.calculate_md5(Path(source_path))
                if cache_manager:
                    cache_manager.save_hash(
                        arcname, effective_size, int(stats.mtime), md5_value
                    )

        final_mode = cls.normalize_mode(stats, anonymize)

        return EntryMetadata(
            arc_path=arcname,
            rel_path=rel_path,
            size=effective_size,
            mtime=int(stats.mtime),
            mode=final_mode,
            uid=uid,
            gid=gid,
            uname=uname,
            gname=gname,
            is_dir=stats.is_dir,
            is_symlink=stats.is_symlink,
            linkname=linkname,
            md5sum=md5_value,
        )

    @staticmethod
    def normalize_mode(stats: DiskEntryStats, anonymize: bool) -> int:
        """
        Normalizes POSIX permissions to ensure cross-platform determinism.

        Why: Windows emulates permissions as 0o666 (rw-rw-rw-) for most files,
        while Linux typically uses 0o644 (rw-r--r--). This discrepancy breaks
        the MD5 hash.
        """
        if not anonymize:
            return stats.mode

        if stats.is_dir:
            return 0o755

        if stats.is_symlink:
            return 0o777

        # 'Execution Intent' detection:
        # Works by checking the executable bit (0o111) which Python emulates
        # on Windows based on file extensions (.exe, .bat, etc.) and reads
        # natively on Linux.
        is_executable = (stats.mode & 0o111) != 0

        # We snap to a clean POSIX standard to eliminate environmental noise.
        return 0o755 if is_executable else 0o644


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
