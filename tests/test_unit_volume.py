import hashlib
import io

from tartape.recorder import TapeRecorder
from tartape.tape import Tape
from tests.base import TarTapeTestCase


class TestFolderVolumeStress(TarTapeTestCase):
    """
    Stress tests for FolderVolume integrity and non-linear access.
    """

    def setUp(self):
        super().setUp()
        self.create_file("file_a.bin", "A" * 5000)
        self.create_file("file_b.bin", "B" * 5000)

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()
        self.tape = Tape(self.data_dir)

    def test_volume_md5_recovery_on_rewind(self):
        """
        A client seeks to the end to check size, then rewinds to 0.
        The linear hash should reset and be valid at the end without manual pass.
        """

        vol_size = 2048
        volumes = list(self.tape.iter_volumes(size=vol_size))
        volume, manifest = volumes[0]

        with volume:
            # Curious read: Seek to end, then back to 0
            volume.seek(0, io.SEEK_END)
            self.assertEqual(volume.tell(), vol_size)

            volume.seek(0)
            self.assertEqual(volume.tell(), 0)

            # Integrity should NOT be broken yet because we are at 0
            self.assertFalse(volume._integrity_broken)  # type: ignore

            # Linear read
            content = volume.read()
            expected_md5 = hashlib.md5(content).hexdigest()

            # Validation
            self.assertEqual(volume.md5sum, expected_md5)
            # Should have used the linear context (no manual pass)
            self.assertFalse(volume._integrity_broken)  # type: ignore

    def test_volume_non_linear_md5_fallback(self):
        """
        Non-linear jumps that break the hash cursor.
        The volume must correctly fallback to manual calculation.
        """
        vol_size = 4096
        volume, _ = list(self.tape.iter_volumes(size=vol_size))[0]

        with volume:
            # Read first 100 bytes
            part1 = volume.read(100)

            # Jump forward (skipping bytes)
            volume.seek(500)
            part2 = volume.read(100)

            self.assertTrue(
                volume._integrity_broken,  # type: ignore
                "Integrity should be marked as broken after jump",
            )

            # Now we ask for the MD5. It should trigger _calculate_manually
            actual_md5 = volume.md5sum

            # To verify, we read the volume manually and compare
            volume.seek(0)
            full_content = volume.read()
            expected_md5 = hashlib.md5(full_content).hexdigest()

            self.assertEqual(
                actual_md5,
                expected_md5,
                "Manual fallback MD5 does not match real content",
            )

    def test_manual_calculation_boundary_limit(self):
        """
        Verify the fix for the 'overflow' bug in _calculate_manually.
        The manual MD5 must NOT include data from subsequent volumes.
        """
        # Split into two small volumes
        vol_size = 1024
        volumes = list(self.tape.iter_volumes(size=vol_size))

        vol1, _ = volumes[0]
        vol2, _ = volumes[1]

        with vol1:
            # Force broken integrity so it uses _calculate_manually
            vol1.seek(10)
            vol1.read(10)

            md5_manual = vol1.md5sum

            # Get real content for comparison
            vol1.seek(0)
            content_linear = vol1.read()
            md5_linear = hashlib.md5(content_linear).hexdigest()

            self.assertEqual(
                md5_manual,
                md5_linear,
                "Manual MD5 leaked data from outside the volume window",
            )
            self.assertEqual(len(content_linear), vol_size)

    def test_seek_out_of_bounds_protection(self):
        """
        Ensures the volume protects against illegal seek operations.
        """
        vol_size = 512
        volume, _ = list(self.tape.iter_volumes(size=vol_size))[0]

        with volume:
            with self.assertRaises(ValueError):
                volume.seek(-1)  # Negative offset

            with self.assertRaises(ValueError):
                volume.seek(vol_size + 1)
