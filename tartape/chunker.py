import logging
from pathlib import Path
from typing import Generator, Iterable, Optional, Tuple, cast

from tartape.catalog import Catalog
from tartape.constants import TAR_BLOCK_SIZE
from tartape.models import Track
from tartape.schemas import ByteWindow, FileSlice, ManifestEntry, VolumeManifest
from tartape.stream import FolderVolume, TapeVolume

logger = logging.getLogger(__name__)


def calculate_segments(
    total_size: int, chunk_size: int
) -> Generator[tuple[int, int], None, None]:
    """
    Generate `(start, end)` byte ranges used to split a stream into chunks.

    Each range follows Python slicing semantics: `start` is inclusive and
    `end` is exclusive.

    Args:
        total_size: Total size of the stream in bytes.
        chunk_size: Desired size of each chunk.

    Yields:
        Tuple[int, int]: A `(start, end)` pair representing the byte range of a chunk.
    """
    for start in range(0, total_size, chunk_size):
        end = min(start + chunk_size, total_size)
        yield start, end


class TarChunker:
    """
    High-level volume scheduler and partitioner.
    Divides a Master Catalog into logical segments (VolumeManifest) and
    calculates precise FileSlice ranges for surgical extraction.
    """

    def __init__(self, chunk_size: int):
        if chunk_size <= 0:
            raise ValueError("The volume size (chunk_size) must be greater than 0.")

        if chunk_size % TAR_BLOCK_SIZE != 0:
            raise ValueError(
                f"Volume chunk_size ({chunk_size}) must be a multiple of "
                f"TAR block size ({TAR_BLOCK_SIZE} bytes)."
            )

        self.chunk_size = chunk_size

    @classmethod
    def get_volume_manifest_for_range(
        cls, fingerprint: str, vol_index: int, volume_window: ByteWindow
    ) -> VolumeManifest:
        """
        Calculates the manifest for a specific byte range window.

        This method must be called within an active Catalog database context.
        """
        # Only tracks that touch this volume window.
        overlapping_tracks = cast(
            Iterable[Track],
            Track.select()
            .where(
                (Track.start_offset < volume_window.end)
                & (Track.end_offset > volume_window.start)
            )
            .order_by(Track.start_offset)
            .iterator(),
        )

        entries = [
            ManifestEntry.from_track(track, volume_window, vol_index=vol_index)
            for track in overlapping_tracks
        ]
        return VolumeManifest(
            tape_fingerprint=fingerprint,
            volume_index=vol_index,
            start_offset=volume_window.start,
            end_offset=volume_window.end,
            chunk_size=volume_window.end - volume_window.start,
            entries=entries,
        )

    def _resolve_volume_name(
        self,
        fingerprint: str,
        root_name: str,
        vol_index: int,
        total_vols: int,
        template: Optional[str] = None,
    ) -> str:
        default_template = "{name}_{fingerprint:.8}.tar.{pindex}"
        actual_template = template or default_template

        padding_width = max(3, len(str(total_vols)))
        pindex = str(vol_index + 1).zfill(padding_width)
        part_num = vol_index + 1

        try:
            return actual_template.format(
                name=root_name,
                fingerprint=fingerprint,
                index=vol_index,
                pindex=pindex,
                part=part_num,
                total=total_vols,
            )
        except (KeyError, ValueError) as e:
            logger.warning(f"Naming template error: {e}. Falling back to default.")
            return default_template.format(
                name=root_name, fingerprint=fingerprint, pindex=pindex
            )

    def iter_volumes(
        self,
        directory: Path,
        naming_template: Optional[str] = None,
    ) -> Generator[tuple[TapeVolume, VolumeManifest], None, None]:
        """
        Main iterator. Yields the File-Like Object (FolderVolume) along with its VolumeManifest.
        """
        with Catalog.from_directory(directory) as cat:
            stats = cat.get_stats()

        fingerprint = stats["fingerprint"]
        total_size = stats["total_size"]
        segments = list(calculate_segments(total_size, self.chunk_size))
        total_vols = len(segments)
        root_name = directory.name

        template = naming_template or "{name}_{fingerprint:.8}.tar.{pindex}"
        for i, (vol_start, vol_end) in enumerate(segments):
            with Catalog.from_directory(directory):
                window = ByteWindow(start=vol_start, end=vol_end)
                manifest = self.get_volume_manifest_for_range(fingerprint, i, window)

            filename = self._resolve_volume_name(
                fingerprint=fingerprint,
                root_name=root_name,
                vol_index=i,
                total_vols=total_vols,
                template=template,
            )

            volume = FolderVolume(
                directory=directory,
                manifest=manifest,
                name=filename,
            )
            yield volume, manifest

    def get_file_slices_map(self, directory: Path | str) -> dict[str, list[FileSlice]]:
        """
        Computes the complete map of FileSlices grouped by file arc_path across all volumes.

        Returns:
            dict[str, list[FileSlice]]: A mapping of {arc_path: [FileSlice, ...]}
            containing only regular files with content (empty files and directories excluded).
        """
        dir_path = Path(directory)
        slices_map: dict[str, list[FileSlice]] = {}

        with Catalog.from_directory(dir_path) as cat:
            stats = cat.get_stats()
            fingerprint = stats["fingerprint"]
            total_size = stats["total_size"]

            segments = list(calculate_segments(total_size, self.chunk_size))
            for i, (vol_start, vol_end) in enumerate(segments):
                window = ByteWindow(start=vol_start, end=vol_end)
                manifest = self.get_volume_manifest_for_range(fingerprint, i, window)
                for entry in manifest.entries:
                    if entry.slice is not None:
                        slices_map.setdefault(entry.info.arc_path, []).append(
                            entry.slice
                        )

        return slices_map

    def get_file_slices(self, directory: Path | str, arc_path: str) -> list[FileSlice]:
        """
        Returns all FileSlices required to assemble a specific file across volumes.

        Args:
            directory: Root directory of the tape.
            arc_path: Archive path of the file to inspect.

        Returns:
            list[FileSlice]: Ordered slices needed to reconstruct the file.
        """
        slices_map = self.get_file_slices_map(directory)
        return slices_map.get(arc_path, [])
