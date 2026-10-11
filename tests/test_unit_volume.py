import hashlib
import io

from tartape.recorder import TapeRecorder
from tartape.tape import Tape
from tests.base import TarTapeTestCase


class TestFolderVolumeStress(TarTapeTestCase):
    """Stress tests for Volume integrity and non-linear access."""

    def setUp(self):
        super().setUp()
        self.create_file("file_a.bin", "A" * 5000)
        self.create_file("file_b.bin", "B" * 5000)

        # Record with explicit MD5 to validate MD5 volume workflows
        recorder = TapeRecorder(self.data_dir, checksum="md5")
        recorder.commit()
        self.tape = Tape(self.data_dir)

    def test_volume_md5_recovery_on_rewind(self):
        """
        A client seeks to the end to check size, then rewinds to 0.
        The linear hash should reset and be valid at the end without manual pass.
        """
        vol_size = 2048
        volumes = list(self.tape.iter_volumes(size=vol_size))
        volume = volumes[0]

        with volume:
            volume.seek(0, io.SEEK_END)
            self.assertEqual(volume.tell(), vol_size)

            volume.seek(0)
            self.assertEqual(volume.tell(), 0)

            self.assertFalse(volume._integrity_broken)

            content = volume.read()
            expected_md5 = hashlib.md5(content).hexdigest()

            self.assertEqual(volume.checksum, expected_md5)
            self.assertEqual(volume.md5sum, expected_md5)
            self.assertFalse(volume._integrity_broken)

    def test_volume_non_linear_md5_fallback(self):
        """
        Non-linear jumps break the in-flight hash cursor.
        The volume must return None for .checksum and require explicit compute_checksum().
        """
        vol_size = 4096
        volume = next(iter(self.tape.iter_volumes(size=vol_size)))

        with volume:
            _ = volume.read(100)
            volume.seek(500)
            _ = volume.read(100)

            self.assertTrue(volume._integrity_broken)

            # In the new design, .checksum/.md5sum is strictly passive: returns None on broken stream
            self.assertIsNone(volume.checksum)
            self.assertIsNone(volume.md5sum)

            # Calling compute_checksum explicitly computes it from source
            actual_md5 = volume.compute_checksum(algorithm="md5")

            volume.seek(0)
            full_content = volume.read()
            expected_md5 = hashlib.md5(full_content).hexdigest()

            self.assertEqual(
                actual_md5,
                expected_md5,
                "Explicit compute_checksum does not match real content",
            )

    def test_manual_calculation_boundary_limit(self):
        """
        Verify compute_checksum does NOT leak data from subsequent volumes.
        """
        vol_size = 1024
        volumes = list(self.tape.iter_volumes(size=vol_size))
        vol1 = volumes[0]

        with vol1:
            vol1.seek(10)
            vol1.read(10)

            # Stream was not fully read linearly, so .checksum is None
            self.assertIsNone(vol1.checksum)

            # Explicit computation must be scoped strictly to the volume window
            md5_computed = vol1.compute_checksum(algorithm="md5")

            vol1.seek(0)
            content_linear = vol1.read()
            md5_linear = hashlib.md5(content_linear).hexdigest()

            self.assertEqual(
                md5_computed,
                md5_linear,
                "Explicit checksum leaked data from outside the volume window",
            )
            self.assertEqual(len(content_linear), vol_size)

    def test_seek_out_of_bounds_protection(self):
        """Ensures the volume protects against illegal seek operations."""
        vol_size = 512
        volume = next(iter(self.tape.iter_volumes(size=vol_size)))

        with volume:
            with self.assertRaises(ValueError):
                volume.seek(-1)

            with self.assertRaises(ValueError):
                volume.seek(vol_size + 1)
