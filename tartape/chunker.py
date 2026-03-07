import logging
from pathlib import Path
from typing import Generator, Iterable, Optional, Tuple, cast

from tartape.catalog import Catalog
from tartape.models import Track
from tartape.schemas import ByteWindow, ManifestEntry, VolumeManifest
from tartape.stream import FolderVolume, TapeVolume

logger = logging.getLogger(__name__)


def calculate_segments(
    total_size: int, chunk_size: int
) -> Generator[tuple[int, int], None, None]:
    """
    Generate `(start, end)` byte ranges used to split a file into chunks.

    Each range follows Python slicing semantics: `start` is inclusive and
    `end` is exclusive.

    Ranges are produced lazily (as a generator), so the full list is not
    materialized in memory.

    Reference:
        https://chatgpt.com/share/68a6ec82-8874-8012-9c27-af04127e28b0

    Args:
        file_size: Total size of the file in bytes.
        chunk_size: Desired size of each chunk.

    Yields:
        Tuple[int, int]: A `(start, end)` pair representing the byte range
        of a chunk. Example: `(0, 100)`, `(100, 200)`, ...
    """
    for start in range(0, total_size, chunk_size):
        end = min(start + chunk_size, total_size)
        yield start, end


class TarChunker:
    """
    High-level volume scheduler.
    Divides a Master Catalog into logical segments (VolumeManifest) and
    generates adapters (TarVolume) ready for network transmission.
    """

    def __init__(self, chunk_size: int):
        if chunk_size <= 0:
            raise ValueError("The volume size (chunk_size) must be greater than 0.")
        self.chunk_size = chunk_size

    @classmethod
    def get_volume_manifest_for_range(
        cls, fingerprint: str, vol_index: int, volume_window: ByteWindow
    ) -> VolumeManifest:
        """

        Calculates the manifest for a specific range of bytes.

        This method assumes it is called within a database context.
        """

        # Only the files that "touch" this byte window.
        # Overlap condition: The file starts before the volume ends,
        # And ends after the volume starts.
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
            ManifestEntry.from_track(track, volume_window)
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
                index=vol_index,  # 0, 1, 2...
                pindex=pindex,  # 001, 002...
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
        naming_template=None,
    ) -> Generator[Tuple[TapeVolume, VolumeManifest], None, None]:
        """
        Main iterator. Returns the File-Like Object (TarVolume) along with its Manifest.
        If no previous plan is passed, it generates one.
        """
        with Catalog.from_directory(directory) as cat:
            stats = cat.get_stats()

        fingerprint = stats["fingerprint"]
        total_size = stats["total_size"]
        segments = list(calculate_segments(total_size, self.chunk_size))
        total_vols = len(segments)
        root_name = directory.name

        default_template = "{name}_{fingerprint:.8}.tar.{pindex}"
        template = naming_template or default_template
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
