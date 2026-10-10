import tempfile
import unittest
from pathlib import Path

import tartape
from tartape.constants import TAR_BLOCK_SIZE, TAR_FOOTER_SIZE


class TestFooterVolumeIntegrity(unittest.TestCase):
    def test_volume_containing_only_footer_emits_full_zeros(self):
        """
        Prueba que si un volumen cae exactamente en la zona del footer (sin archivos),
        FolderVolume emita correctamente los bytes nulos y alcance is_completed == True.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create a small file: exactly 512 bytes
            sample_file = tmp_path / "hello.txt"
            sample_file.write_bytes(b"A" * 512)

            tape = tartape.create(tmp_path, overwrite=True)
            total_size = tape.total_size

            # Chunk size chosen so the last volume touches only the footer
            # A 512-byte multiple chunk_size
            chunk_size = 512
            vols = list(tape.iter_volumes(size=chunk_size))
            self.assertGreater(len(vols), 1)

            # Test each volume can be fully read to its declared size
            total_bytes_read = 0
            for volume, manifest in vols:
                with volume:
                    data = volume.read()
                    self.assertEqual(len(data), manifest.chunk_size)
                    self.assertTrue(volume.is_completed)
                    total_bytes_read += len(data)

            self.assertEqual(total_bytes_read, total_size)


if __name__ == "__main__":
    unittest.main()
