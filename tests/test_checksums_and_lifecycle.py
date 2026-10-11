import hashlib
import tempfile
import unittest
from pathlib import Path

import tartape
from tartape.exceptions import SourceNotFoundError


class TestChecksumsAndLifecycle(unittest.TestCase):
    """Test suite for unified checksums, volume lifecycle, and SQLite WAL hygiene."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root_path = Path(self.temp_dir.name) / "test_data"
        self.root_path.mkdir(parents=True, exist_ok=True)

        # Create sample files
        self.file1 = self.root_path / "hello.txt"
        self.file1.write_bytes(b"Hello World! This is TarTape test payload.")

        self.file2 = self.root_path / "data.bin"
        self.file2.write_bytes(b"\x00\x01\x02\x03" * 1024)  # 4 KB

        self.sub_dir = self.root_path / "nested"
        self.sub_dir.mkdir(parents=True, exist_ok=True)
        self.file3 = self.sub_dir / "item.txt"
        self.file3.write_bytes(b"Nested file content.")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_record_checksum_disabled_by_default(self):
        """Verifica que record() por defecto no calcula checksums de archivo (fast scan)."""
        tape = tartape.record(self.root_path, overwrite=True)
        try:
            self.assertFalse(tape.has_file_checksums)
            self.assertIsNone(tape.checksum_algorithm)
            self.assertGreater(tape.data_size, 0)
            self.assertGreater(tape.total_size, tape.data_size)
            self.assertEqual(tape.overhead_size, tape.total_size - tape.data_size)

            for track in tape.inspect():
                if track.is_file:
                    self.assertIsNone(track.checksum)
        finally:
            tape.close()

    def test_record_checksum_sha256(self):
        """Verifica el cálculo de checksums de archivo en T0 usando SHA-256."""
        tape = tartape.record(self.root_path, checksum="sha256", overwrite=True)
        try:
            self.assertTrue(tape.has_file_checksums)
            self.assertEqual(tape.checksum_algorithm, "sha256")

            expected_f1_sha256 = hashlib.sha256(self.file1.read_bytes()).hexdigest()

            found = False
            for track in tape.inspect():
                if track.rel_path == "hello.txt":
                    self.assertEqual(track.checksum, expected_f1_sha256)
                    found = True
            self.assertTrue(found, "Track hello.txt was not found in catalog")
        finally:
            tape.close()

    def test_record_checksum_md5(self):
        """Verifica el cálculo de checksums de archivo en T0 usando MD5."""
        tape = tartape.record(self.root_path, checksum="md5", overwrite=True)
        try:
            self.assertTrue(tape.has_file_checksums)
            self.assertEqual(tape.checksum_algorithm, "md5")

            expected_f1_md5 = hashlib.md5(self.file1.read_bytes()).hexdigest()

            for track in tape.inspect():
                if track.rel_path == "hello.txt":
                    self.assertEqual(track.checksum, expected_f1_md5)
        finally:
            tape.close()

    def test_volume_checksum_passive_lifecycle(self):
        """Verifica que volume.checksum sea pasivo (None antes de leer, valor tras lectura completa)."""
        with tartape.record(self.root_path, checksum="sha256", overwrite=True) as tape:
            volumes = list(tape.iter_volumes(size="1MB"))
            self.assertGreaterEqual(len(volumes), 1)

            vol = volumes[0]
            # 1. Antes de leer, debe ser None (cero lecturas de disco ocultas)
            self.assertIsNone(vol.checksum)
            self.assertEqual(vol.checksum_algorithm, "sha256")

            # 2. Leer linealmente hasta el final
            hasher = hashlib.sha256()
            with vol:
                while chunk := vol.read(1024):
                    hasher.update(chunk)

            # 3. Tras lectura completa lineal, checksum debe estar listo
            self.assertIsNotNone(vol.checksum)
            self.assertEqual(vol.checksum, hasher.hexdigest())

    def test_volume_compute_checksum_explicit(self):
        """Verifica que compute_checksum() lea explícitamente de disco bajo demanda."""
        with tartape.record(self.root_path, overwrite=True) as tape:
            vol = tape.get_volume(0, size="1MB")
            self.assertIsNone(vol.checksum)

            computed = vol.compute_checksum(algorithm="sha256")
            self.assertIsNotNone(computed)
            self.assertEqual(vol.checksum, computed)

    def test_volume_compute_checksum_fails_when_source_missing(self):
        """Verifica que compute_checksum() lance SourceNotFoundError si los archivos no existen."""
        tape = tartape.record(self.root_path, overwrite=True)
        catalog_path = tape.catalog_path
        tape.close()

        # Abrir el catálogo de forma aislada apuntando a una ruta inexistente
        fake_dir = Path(self.temp_dir.name) / "non_existent_folder"
        ghost_tape = tartape.open(fake_dir, catalog_path=catalog_path)
        try:
            vol = ghost_tape.get_volume(0, size="1MB")
            with self.assertRaises(SourceNotFoundError):
                vol.compute_checksum()
        finally:
            ghost_tape.close()

    def test_clean_wal_checkpoint_on_close(self):
        """Verifica que el context manager o close() ejecute PRAGMA wal_checkpoint(TRUNCATE)."""
        tape = tartape.record(self.root_path, overwrite=True)
        catalog_file = Path(tape.catalog_path)  # type: ignore

        # Simular escrituras en T1 registrando un layout
        with tape:
            list(tape.iter_volumes(size="512KB"))

        # Al cerrar, los archivos WAL y SHM deben eliminarse o quedar truncados a 0 bytes
        wal_file = catalog_file.parent / f"{catalog_file.name}-wal"
        shm_file = catalog_file.parent / f"{catalog_file.name}-shm"

        if wal_file.exists():
            self.assertEqual(wal_file.stat().st_size, 0)
        if shm_file.exists():
            self.assertEqual(shm_file.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
