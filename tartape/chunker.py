import logging
import math
from typing import Generator, List, Optional, Tuple

from tartape.models import Track
from tartape.player import TapePlayer
from tartape.schemas import EntryState, ManifestEntry, VolumeManifest
from tartape.tape import Tape
from tartape.volume import TarVolume

logger = logging.getLogger(__name__)


class TarChunker:
    """
    High-level volume scheduler.
    Divides a Master Tape into logical segments (VolumeManifest) and
    generates adapters (TarVolume) ready for network transmission.
    """

    def __init__(self, tape: Tape, chunk_size: int):
        if chunk_size <= 0:
            raise ValueError("The volume size (chunk_size) must be greater than 0.")

        self.tape = tape
        self.chunk_size = chunk_size
        self.fingerprint = self.tape.fingerprint
        self.total_size = self.tape.total_size

    def generate_plan(self) -> List[VolumeManifest]:
        """
        It analyzes the O(N) database and calculates exactly which file fragments
        will fall on which volume. It does not read bytes from the disk.
        """
        logger.info(f"Generating volume plan (Chunk Size: {self.chunk_size} bytes)...")
        manifests = []

        total_volumes = math.ceil(self.total_size / self.chunk_size)

        for vol_index in range(total_volumes):
            vol_start = vol_index * self.chunk_size
            vol_end = min(vol_start + self.chunk_size, self.total_size)
            actual_chunk_size = vol_end - vol_start

            # Only the files that "touch" this byte window.
            # Overlap condition: The file starts before the volume ends,
            # And ends after the volume starts.
            overlapping_tracks = (
                Track.select()
                .where((Track.start_offset < vol_end) & (Track.end_offset > vol_start))
                .order_by(Track.start_offset)
                .iterator()
            )

            entries = []
            for track in overlapping_tracks:
                starts_inside = track.start_offset >= vol_start
                ends_inside = track.end_offset <= vol_end

                if starts_inside and ends_inside:
                    state = EntryState.COMPLETE
                elif starts_inside and not ends_inside:
                    state = EntryState.HEAD
                elif not starts_inside and ends_inside:
                    state = EntryState.TAIL
                else:
                    state = EntryState.BODY

                # If it starts before the volume, its local offset is 0.
                local_start = max(0, track.start_offset - vol_start)

                # The bytes used are the minimum between the end of the file and the end of the volume
                # minus the maximum between the beginning of the file and the beginning of the volume.
                bytes_occupied = min(track.end_offset, vol_end) - max(
                    track.start_offset, vol_start
                )

                entries.append(
                    ManifestEntry(
                        arc_path=track.arc_path,
                        state=state,
                        offset_in_volume=local_start,
                        bytes_in_volume=bytes_occupied,
                    )
                )

            manifest = VolumeManifest(
                tape_fingerprint=self.fingerprint,
                volume_index=vol_index,
                start_offset=vol_start,
                end_offset=vol_end,
                chunk_size=actual_chunk_size,
                entries=entries,
            )
            manifests.append(manifest)

        logger.info(
            f"Plan successfully generated: {len(manifests)} calculated volumes."
        )
        return manifests

    def iter_volumes(
        self, player: TapePlayer, plan: Optional[List[VolumeManifest]] = None
    ) -> Generator[Tuple[TarVolume, VolumeManifest], None, None]:
        """
        Main iterator. Returns the File-Like Object (TarVolume) along with its Manifest.
        If no previous plan is passed, it generates one.
        """
        if plan is None:
            plan = self.generate_plan()

        for manifest in plan:
            net_name = f"{self.fingerprint}_vol_{manifest.volume_index}.tar"
            volume = TarVolume(
                player=player,
                start_offset=manifest.start_offset,
                end_offset=manifest.end_offset,
                name=net_name,
            )
            yield volume, manifest
