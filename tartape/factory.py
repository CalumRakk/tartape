import hashlib
import logging
import os
import stat as stat_module
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional

from tartape.constants import DEFAULT_EXCLUDES, TAPE_METADATA_DIR
from tartape.exceptions import PathConstraintError, TarIntegrityError
from tartape.models import Track
from tartape.schemas import Discrepancy, DiskEntryStats, EntryMetadata

if TYPE_CHECKING:
    from tartape.cache import HashCacheManager

try:
    import grp
    import pwd
except ImportError:
    pwd = None
    grp = None


ExcludeType = str | list[str] | Callable[[Path], bool]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def should_exclude(path: Path | str, exclude: Optional[ExcludeType] = None) -> bool:
    """Determine whether a path matches global or custom exclusion rules.

    Args:
        path: File or directory path to evaluate.
        exclude: Glob pattern, list of patterns, callable, or None for defaults.

    Returns:
        bool: True if path should be ignored, False otherwise.
    """
    p = Path(path)
    if TAPE_METADATA_DIR in p.parts:
        return True

    effective = DEFAULT_EXCLUDES if exclude is None else exclude

    if callable(effective):
        try:
            return bool(effective(p))
        except Exception:
            return False

    if isinstance(effective, str):
        return p.match(effective) or p.name == effective

    if isinstance(effective, (list, tuple, set)):
        return any(p.match(pattern) or p.name == pattern for pattern in effective)

    return False


def check_directory_structural_integrity(
    expected: EntryMetadata | Track,
    tape_root_directory: Path,
    exclude: Optional[ExcludeType] = None,
) -> Optional[Discrepancy]:
    """Verify structural integrity of a directory while tolerating excluded OS artifacts."""
    full_disk_path = Path(tape_root_directory) / expected.rel_path
    stats = TarEntryFactory.inspect(full_disk_path)

    if not stats.exists:
        return Discrepancy(
            arc_path=expected.arc_path,
            rel_path=expected.rel_path,
            reason="missing",
            message=f"Directory missing: '{expected.arc_path}'",
        )

    if expected.rel_path in ("", "."):
        return None

    if stats.mtime == expected.mtime:
        return None

    try:
        untracked_unexcluded: list[str] = []
        has_excluded_items = False

        with os.scandir(full_disk_path) as it:
            for entry in it:
                entry_path = Path(entry.path)
                if should_exclude(entry_path, exclude):
                    has_excluded_items = True
                    continue

                child_rel = (
                    f"{expected.rel_path}/{entry.name}"
                    if expected.rel_path
                    else entry.name
                )
                is_tracked = Track.select().where(Track.rel_path == child_rel).exists()
                if not is_tracked:
                    untracked_unexcluded.append(entry.name)

        if untracked_unexcluded:
            return Discrepancy(
                arc_path=expected.arc_path,
                rel_path=expected.rel_path,
                reason="untracked_item",
                found=untracked_unexcluded,
                message=(
                    f"Directory '{expected.arc_path}' contains untracked items: "
                    f"{', '.join(untracked_unexcluded)}"
                ),
            )

        if not has_excluded_items:
            return Discrepancy(
                arc_path=expected.arc_path,
                rel_path=expected.rel_path,
                reason="structural_change",
                expected=expected.mtime,
                found=stats.mtime,
                message=f"Directory structure changed: {expected.arc_path}",
            )

    except OSError as e:
        return Discrepancy(
            arc_path=expected.arc_path,
            rel_path=expected.rel_path,
            reason="structural_change",
            message=f"Error accessing directory '{expected.arc_path}': {e}",
        )

    return None


def check_entry_integrity(
    expected: EntryMetadata | Track,
    tape_root_directory: Path,
    exclude: Optional[ExcludeType] = None,
) -> Optional[Discrepancy]:
    """Perform integrity check on a single entry, returning Discrepancy if invalid."""
    full_disk_path = Path(tape_root_directory) / expected.rel_path
    stats = TarEntryFactory.inspect(full_disk_path)

    if not stats.exists:
        return Discrepancy(
            arc_path=expected.arc_path,
            rel_path=expected.rel_path,
            reason="missing",
            message=f"File missing: {expected.arc_path}",
        )

    if expected.is_dir:
        return check_directory_structural_integrity(
            expected, tape_root_directory, exclude=exclude
        )

    if stats.mtime != expected.mtime:
        return Discrepancy(
            arc_path=expected.arc_path,
            rel_path=expected.rel_path,
            reason="mtime_mismatch",
            expected=expected.mtime,
            found=stats.mtime,
            message=f"File modified (mtime): {expected.arc_path}",
        )

    if not expected.is_symlink and stats.size != expected.size:
        return Discrepancy(
            arc_path=expected.arc_path,
            rel_path=expected.rel_path,
            reason="size_mismatch",
            expected=expected.size,
            found=stats.size,
            message=f"File size changed: {expected.arc_path}",
        )

    return None


def validate_integrity(
    expected: EntryMetadata | Track,
    tape_root_directory: Path,
    exclude: Optional[ExcludeType] = None,
) -> None:
    """Strict implementation of ADR-002 Fail-Fast for streaming runtime guard."""
    discrepancy = check_entry_integrity(expected, tape_root_directory, exclude=exclude)
    if discrepancy:
        raise TarIntegrityError(discrepancy.message)


def validate_root_structure_integrity(
    root_path: Path,
    exclude: Optional[ExcludeType] = None,
    raise_exception: bool = True,
) -> Optional[Discrepancy]:
    """Check root directory structure for untracked items, respecting exclusion rules."""
    try:
        disk_items = []
        with os.scandir(root_path) as it:
            for entry in it:
                if entry.name == TAPE_METADATA_DIR:
                    continue
                entry_path = Path(entry.path)
                if should_exclude(entry_path, exclude):
                    continue
                disk_items.append(entry.name)
    except OSError as e:
        msg = f"Root directory is inaccessible: {e}"
        if raise_exception:
            raise TarIntegrityError(msg) from e
        return Discrepancy(
            arc_path="",
            rel_path="",
            reason="structural_change",
            message=msg,
        )

    tracked_root_items = set(
        Track.select(Track.rel_path)
        .where((Track.rel_path != "") & (~Track.rel_path.contains("/")))  # type: ignore
        .scalars()
    )

    untracked = [name for name in disk_items if name not in tracked_root_items]

    if untracked:
        msg = (
            f"Integrity compromised: {len(untracked)} untracked item(s) "
            f"detected in root directory ({', '.join(untracked[:5])})."
        )
        if raise_exception:
            raise TarIntegrityError(msg)
        return Discrepancy(
            arc_path="",
            rel_path="",
            reason="untracked_item",
            found=untracked,
            message=msg,
        )

    return None


def validate_ustar_path(
    arcname: str, is_dir: bool = False
) -> tuple[bool, Optional[str]]:
    """Verifies whether a route strictly complies with USTAR rules and 255-byte limit."""
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


def truncate_component_safe(component: str, max_bytes: int = 100) -> str:
    """Truncates a path component safely using MD5 with usedforsecurity=False."""
    comp_bytes = component.encode("utf-8")

    if len(comp_bytes) <= max_bytes:
        return component

    # Non-cryptographic hashing: usedforsecurity=False ensures FIPS compliance
    hash_suffix = hashlib.md5(comp_bytes, usedforsecurity=False).hexdigest()[:14]
    limit_for_prefix = max_bytes - 15

    prefix_bytes = comp_bytes[:limit_for_prefix]
    safe_prefix = prefix_bytes.decode("utf-8", errors="ignore")

    result = f"{safe_prefix}_{hash_suffix}"

    while len(result.encode("utf-8")) > max_bytes:
        safe_prefix = safe_prefix[:-1]
        result = f"{safe_prefix}_{hash_suffix}"

    return result


def shorten_path_ustar(arcname: str, is_dir: bool = False) -> str:
    """Deterministically shortens a path to comply with USTAR standard."""
    components = arcname.split("/")
    max_leaf_bytes = 99 if is_dir else 100

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

    if len(clean_components) == 1:
        comp = clean_components[0]
        if len(comp.encode("utf-8")) > max_leaf_bytes:
            comp = truncate_component_safe(comp, max_leaf_bytes)
        return comp

    leaf = clean_components[-1]
    prefix_components = clean_components[:-1]
    full_prefix = "/".join(prefix_components)

    max_total = 254 if is_dir else 255
    max_prefix_bytes = min(155, max_total - 1 - len(leaf.encode("utf-8")))

    # Non-cryptographic hashing: usedforsecurity=False ensures FIPS compliance
    prefix_hash = hashlib.md5(
        full_prefix.encode("utf-8"), usedforsecurity=False
    ).hexdigest()[:8]
    marker = f"~{prefix_hash}"

    root = prefix_components[0]
    min_root_space = max_prefix_bytes - len(marker.encode("utf-8")) - 1
    if len(root.encode("utf-8")) > min_root_space:
        root = truncate_component_safe(root, max(10, min_root_space))

    base_prefix = f"{root}/{marker}"
    current_bytes = len(base_prefix.encode("utf-8"))

    tail_candidates = prefix_components[1:]
    chosen_tail = []

    for comp in reversed(tail_candidates):
        comp_bytes = len(comp.encode("utf-8"))
        if current_bytes + 1 + comp_bytes <= max_prefix_bytes:
            chosen_tail.insert(0, comp)
            current_bytes += 1 + comp_bytes
        else:
            break

    shortened_prefix = (
        f"{base_prefix}/" + "/".join(chosen_tail) if chosen_tail else base_prefix
    )
    return f"{shortened_prefix}/{leaf}"


class TarEntryFactory:
    """Exclusively responsible for inspecting the filesystem and building EntryMetadata."""

    @staticmethod
    def resolve_arcname(
        arcname: str, auto_truncate: bool = False, is_dir: bool = False
    ) -> str:
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
    def calculate_checksum(path: Path, algorithm: str = "sha256") -> str:
        """Calculate the cryptographic checksum of a file in 64 KB blocks."""
        try:
            hasher = hashlib.new(algorithm)
        except ValueError:
            hasher = hashlib.new(algorithm, usedforsecurity=False)

        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(64 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    # Alias for smooth transition
    calculate_md5 = staticmethod(lambda p: TarEntryFactory.calculate_checksum(p, "md5"))

    @staticmethod
    def inspect(
        path: Path, precomputed_stat: Optional[os.stat_result] = None
    ) -> DiskEntryStats:
        """Performs low-level lstat on the path or uses precomputed stat result."""
        try:
            st = precomputed_stat if precomputed_stat else path.lstat()
            permissions = stat_module.S_IMODE(st.st_mode)

            is_dir = stat_module.S_ISDIR(st.st_mode)
            is_file = stat_module.S_ISREG(st.st_mode)
            is_symlink = stat_module.S_ISLNK(st.st_mode)

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
        source_path: Path | str,
        rel_path: str,
        arcname: str,
        anonymize: bool = True,
        checksum_algorithm: Optional[str] = None,
        precomputed_stat: Optional[os.stat_result] = None,
        cache_manager: Optional["HashCacheManager"] = None,
    ) -> Optional[EntryMetadata]:
        """Analyzes a path and creates an EntryMetadata instance.

        Args:
            source_path: Physical path on disk.
            rel_path: Path relative to tape root.
            arcname: Resolved TAR archive path.
            anonymize: Whether to scrub local UID/GID.
            checksum_algorithm: Algorithm to compute file hash ('sha256', 'md5', etc.), or None to skip.
            precomputed_stat: Optional precomputed os.stat_result.
            cache_manager: Optional HashCacheManager for caching.
        """
        path = Path(source_path)
        stats = cls.inspect(path, precomputed_stat=precomputed_stat)

        if not stats.exists or not (stats.is_dir or stats.is_file or stats.is_symlink):
            return None

        linkname = os.readlink(path) if stats.is_symlink else ""
        effective_size = 0 if (stats.is_dir or stats.is_symlink) else stats.size

        uid = 0 if anonymize else stats.uid
        gid = 0 if anonymize else stats.gid
        uname = "root" if anonymize else stats.uname
        gname = "root" if anonymize else stats.gname

        checksum_value = None
        if checksum_algorithm and stats.is_file:
            algo = checksum_algorithm.lower()
            if cache_manager:
                checksum_value = cache_manager.get_checksum(
                    arcname, effective_size, int(stats.mtime), algorithm=algo
                )
            if not checksum_value:
                checksum_value = cls.calculate_checksum(path, algorithm=algo)
                if cache_manager:
                    cache_manager.save_checksum(
                        arcname,
                        effective_size,
                        int(stats.mtime),
                        checksum_value,
                        algorithm=algo,
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
            checksum=checksum_value,
        )

    @staticmethod
    def normalize_mode(stats: DiskEntryStats, anonymize: bool) -> int:
        if not anonymize:
            return stats.mode
        if stats.is_dir:
            return 0o755
        if stats.is_symlink:
            return 0o777
        is_executable = (stats.mode & 0o111) != 0
        return 0o755 if is_executable else 0o644
