import os
import stat
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import tartape
from tartape.database import DatabaseSession, seal_database
from tartape.models import LayoutRecord, TapeMetadata, Track, VolumeRecord


class TestDatabaseAndModels(unittest.TestCase):
    """Valida el esquema relacional, modelos y el sellado libre de archivos WAL."""

    def test_schema_and_models(self):
        """Verifica que todos los modelos Peewee se creen e interactúen correctamente."""
        with DatabaseSession(":memory:"):
            # 1. TapeMetadata
            TapeMetadata.create(key="fingerprint", value="sha256_sample")
            self.assertEqual(TapeMetadata.get(key="fingerprint").value, "sha256_sample")

            # 2. LayoutRecord
            LayoutRecord.create(
                tag="s3",
                volume_size=1024**3,
                total_volumes=5,
                created_at=1700000000,
                is_default=True,
            )
            layout = LayoutRecord.get(tag="s3")
            self.assertEqual(layout.total_volumes, 5)
            self.assertTrue(layout.is_default)

            # 3. VolumeRecord (Composite primary key: layout_tag + volume_index)
            VolumeRecord.create(
                layout_tag="s3",
                volume_index=0,
                name="backup.tar.001",
                start_offset=0,
                end_offset=1024**3,
                size=1024**3,
                md5sum="abc123md5",
            )
            vol = VolumeRecord.get(
                (VolumeRecord.layout_tag == "s3") & (VolumeRecord.volume_index == 0)
            )
            self.assertEqual(vol.name, "backup.tar.001")
            self.assertEqual(vol.md5sum, "abc123md5")

    def test_seal_database_eliminates_wal(self):
        """Verifica que seal_database convierta WAL a DELETE y elimine archivos -wal/-shm."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test_tape.db"

            # 1. Open session in WAL mode and insert records
            with DatabaseSession(db_path):
                TapeMetadata.create(key="fingerprint", value="hash123")
                LayoutRecord.create(
                    tag="default",
                    volume_size=500,
                    total_volumes=2,
                    created_at=1700000000,
                    is_default=True,
                )

            # 2. Seal the database
            seal_database(db_path)

            # 3. Verify no lingering auxiliary files exist
            wal_file = Path(tmp_dir) / "test_tape.db-wal"
            shm_file = Path(tmp_dir) / "test_tape.db-shm"
            self.assertFalse(wal_file.exists())
            self.assertFalse(shm_file.exists())

            # 4. Verify database can be reopened and read cleanly
            with DatabaseSession(db_path):
                self.assertEqual(TapeMetadata.get(key="fingerprint").value, "hash123")
                self.assertEqual(LayoutRecord.get(tag="default").volume_size, 500)


class TestZeroPollutionRecorder(unittest.TestCase):
    """Valida la generación del sidecar sin contaminar la carpeta de origen."""

    def test_zero_pollution_sidecar_creation(self):
        """Verifica que grabar una carpeta no cree archivos dentro de ella y genere el sidecar."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            source_folder = base_dir / "my_dataset"
            source_folder.mkdir()

            (source_folder / "file1.txt").write_text("hello world")
            sub = source_folder / "subdir"
            sub.mkdir()
            (sub / "file2.txt").write_text("another file")

            # Record
            catalog_file = tartape.record(source_folder)

            # 1. Verify sidecar path is adjacent to folder
            expected_sidecar = base_dir / "my_dataset.tartape"
            self.assertEqual(catalog_file, expected_sidecar)
            self.assertTrue(expected_sidecar.exists())

            # 2. ZERO POLLUTION: source folder MUST NOT contain .tartape
            self.assertFalse((source_folder / ".tartape").exists())
            source_contents = [p.name for p in source_folder.iterdir()]
            self.assertCountEqual(source_contents, ["file1.txt", "subdir"])

            # 3. Verify sidecar has no lingering WAL files
            self.assertFalse(base_dir.joinpath("my_dataset.tartape-wal").exists())
            self.assertFalse(base_dir.joinpath("my_dataset.tartape-shm").exists())

            # 4. Verify catalog contents
            with DatabaseSession(catalog_file):
                self.assertIsNotNone(TapeMetadata.get_or_none(key="fingerprint"))
                # Tracks: root folder (1), file1 (2), subdir (3), file2 (4)
                self.assertEqual(Track.select().count(), 4)

    def test_custom_catalog_path_and_read_only_folder(self):
        """Verifica que se pueda grabar una carpeta de solo lectura pasando un catalog_path externo."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            source_folder = base_dir / "ro_dataset"
            source_folder.mkdir()
            (source_folder / "data.bin").write_bytes(b"immutable")

            custom_catalog = base_dir / "custom_location" / "backup.tartape"

            # Set folder to READ-ONLY (remove write permissions)
            original_mode = source_folder.stat().st_mode
            os.chmod(source_folder, stat.S_IRUSR | stat.S_IXUSR)

            try:
                result_path = tartape.record(source_folder, catalog_path=custom_catalog)
                self.assertEqual(result_path, custom_catalog)
                self.assertTrue(custom_catalog.exists())
            finally:
                # Restore permissions for cleanup
                os.chmod(source_folder, original_mode)


class TestCatalogAndGPS(unittest.TestCase):
    """Valida la independencia del catálogo offline y la aritmética GPS sin archivos locales."""

    def test_standalone_catalog_and_gps_resolution(self):
        """Verifica que el catálogo funcione desconectado y resuelva GPS quirúrgico."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            source = base_dir / "my_data"
            source.mkdir()

            (source / "doc.txt").write_bytes(b"A" * 1024)
            (source / "big.bin").write_bytes(b"B" * 4096)

            sidecar = tartape.record(source)

            # ZERO DISK DEPENDENCY: Delete the original source folder entirely!
            import shutil

            shutil.rmtree(source)
            self.assertFalse(source.exists())

            # Open standalone catalog
            with tartape.open_catalog(sidecar) as catalog:
                # 1. Properties verification
                self.assertTrue(len(catalog.fingerprint) > 0)
                self.assertTrue(catalog.total_size > 0)
                self.assertEqual(
                    catalog.file_count, 3
                )  # root (1), doc.txt (2), big.bin (3)

                # 2. Register Layout for 2048-byte chunks
                layout = catalog.register_layout(
                    volume_size=2048, tag="small_parts", is_default=True
                )
                self.assertEqual(layout.tag, "small_parts")
                self.assertTrue(layout.total_volumes >= 3)
                self.assertEqual(len(layout.volumes), layout.total_volumes)

                # 3. Surgical GPS resolution
                gps_doc = layout.locate("my_data/doc.txt")
                self.assertEqual(gps_doc.file_size, 1024)
                self.assertEqual(len(gps_doc.fragments), 2)
                self.assertEqual(gps_doc.fragments[0].volume_index, 2)
                self.assertEqual(gps_doc.fragments[0].volume_length, 512)
                self.assertEqual(gps_doc.fragments[1].volume_index, 3)
                self.assertEqual(gps_doc.fragments[1].volume_length, 512)
                self.assertEqual(sum(f.volume_length for f in gps_doc.fragments), 1024)

                gps_big = layout.locate("my_data/big.bin")
                self.assertEqual(gps_big.file_size, 4096)
                self.assertTrue(gps_big.spans_multiple_volumes)
                self.assertEqual(sum(f.volume_length for f in gps_big.fragments), 4096)

                # 4. In a large 8192-byte layout, doc.txt fits into a SINGLE fragment
                layout_large = catalog.register_layout(
                    volume_size=8192, tag="large_parts", is_default=False
                )
                gps_doc_single = layout_large.locate("my_data/doc.txt")
                self.assertEqual(len(gps_doc_single.fragments), 1)
                self.assertEqual(gps_doc_single.fragments[0].volume_length, 1024)

                # Fetching explicitly works
                self.assertEqual(catalog.get_layout("large_parts").volume_size, 8192)
                self.assertEqual(catalog.get_layout(2048).tag, "small_parts")
                self.assertEqual(catalog.default_layout.tag, "small_parts")

    def test_volume_verification_with_md5(self):
        """Verifica que verify_volume valide hashes de volúmenes descargados correctamente."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            source = base_dir / "dataset"
            source.mkdir()
            (source / "file.txt").write_bytes(b"test data")

            sidecar = tartape.record(source)
            with tartape.open_catalog(sidecar) as catalog:
                layout = catalog.register_layout(
                    volume_size=1024, tag="test_layout", is_default=True
                )

                vol_file = base_dir / "vol_0.tar"
                vol_file.write_bytes(b"some volume data")
                import hashlib

                expected_hash = hashlib.md5(b"some volume data").hexdigest()

                # Persist MD5 directly in DB for verification test
                with catalog.db_session:
                    VolumeRecord.update(md5sum=expected_hash).where(
                        (VolumeRecord.layout_tag == "test_layout")
                        & (VolumeRecord.volume_index == 0)
                    ).execute()

                self.assertTrue(layout.verify_volume(0, vol_file))

                # Corrupted data must fail verification
                vol_file.write_bytes(b"corrupted data")
                self.assertFalse(layout.verify_volume(0, vol_file))


class TestStreamingAndFileLike(unittest.TestCase):
    """Valida la emisión de bytes puros, telemetría in-band y la interfaz as_file()."""

    def test_play_emits_pure_bytes_and_callbacks(self):
        """Verifica que tape.play emita bytes puros y despache eventos in-band."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "data"
            source.mkdir()
            (source / "a.txt").write_bytes(b"Hello World")
            (source / "b.txt").write_bytes(b"Tartape v3")

            tape = tartape.create(source)

            received_events = []

            def listener(ev):
                received_events.append(ev.type)

            byte_stream = list(tape.play(on_event=listener))
            total_bytes = b"".join(byte_stream)

            # 1. Output is raw bytes
            self.assertIsInstance(total_bytes, bytes)
            self.assertEqual(len(total_bytes), tape.total_size)

            # 2. In-band events captured synchronously
            self.assertIn("file_start", received_events)
            self.assertIn("file_end", received_events)
            self.assertIn("tape_completed", received_events)

    def test_as_file_reader(self):
        """Verifica que tape.as_file funcione como un objeto tipo archivo con seek y read."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "data"
            source.mkdir()
            (source / "doc.bin").write_bytes(b"X" * 2048)

            tape = tartape.create(source)

            with tape.as_file() as f:
                header = f.read(512)
                self.assertEqual(len(header), 512)
                self.assertEqual(f.tell(), 512)

                # Read remainder
                rest = f.read()
                self.assertEqual(len(rest), tape.total_size - 512)
                self.assertEqual(f.tell(), tape.total_size)

                # Seek back to start
                f.seek(0)
                self.assertEqual(f.tell(), 0)
                re_read_header = f.read(512)
                self.assertEqual(header, re_read_header)

    def test_inspect_dry_run(self):
        """Verifica que tape.inspect liste los tracks sin emitir bytes ni leer discos."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "data"
            source.mkdir()
            (source / "one.txt").write_text("1")
            (source / "two.txt").write_text("2")

            tape = tartape.create(source)
            tracks = list(tape.inspect())
            paths = [t.arc_path for t in tracks]

            self.assertIn("data/one.txt", paths)
            self.assertIn("data/two.txt", paths)


class TestVolumesAndConcurrency(unittest.TestCase):
    """Valida el ciclo de vida de los volúmenes, cálculo de MD5 y concurrencia multi-hilo."""

    def test_iter_volumes_registers_layout_and_persists_md5(self):
        """Verifica que iter_volumes registre el layout y guarde los hashes MD5 al leerse."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "data"
            source.mkdir()
            (source / "doc1.bin").write_bytes(b"A" * 2048)
            (source / "doc2.bin").write_bytes(b"B" * 2048)

            tape = tartape.create(source)
            sidecar_path = Path(tmp_dir) / "data.tartape"

            volumes = list(tape.iter_volumes(size=2048, tag="s3_parts"))
            self.assertTrue(len(volumes) >= 2)

            first_vol = volumes[0]
            self.assertEqual(first_vol.index, 0)
            self.assertEqual(first_vol.layout_tag, "s3_parts")

            with first_vol:
                data = first_vol.read()
                self.assertEqual(len(data), 2048)

            calculated_md5 = first_vol.md5sum
            self.assertTrue(len(calculated_md5) > 0)

            # Verify MD5 was atomically persisted to VolumeRecord
            with DatabaseSession(sidecar_path):
                rec = VolumeRecord.get(
                    (VolumeRecord.layout_tag == "s3_parts")
                    & (VolumeRecord.volume_index == 0)
                )
                self.assertEqual(rec.md5sum, calculated_md5)

    def test_get_volume_by_index_without_manual_offsets(self):
        """Verifica que tape.get_volume obtenga un volumen sin pedir offsets manuales."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "data"
            source.mkdir()
            (source / "hello.txt").write_bytes(b"Hello from Tartape v3")

            tape = tartape.create(source)

            vol = tape.get_volume(index=0, size=1024)
            self.assertEqual(vol.index, 0)
            self.assertEqual(vol.size, 1024)
            with vol:
                header_bytes = vol.read(512)
                self.assertEqual(len(header_bytes), 512)

    def test_concurrent_volume_streaming_and_md5_persisting(self):
        """Verifica que múltiples hilos puedan leer volúmenes en paralelo sin colisionar en SQLite."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "data"
            source.mkdir()
            (source / "big.bin").write_bytes(b"Z" * (1024 * 32))

            tape = tartape.create(source)
            sidecar_path = Path(tmp_dir) / "data.tartape"

            volumes = list(tape.iter_volumes(size=4096, tag="concurrent_test"))

            def process_volume(v):
                with v:
                    _ = v.read()
                return v.index, v.md5sum

            # Process 4 volumes concurrently
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(process_volume, volumes))

            self.assertEqual(len(results), len(volumes))

            # Verify all volumes got their MD5 persisted cleanly without lock errors
            with DatabaseSession(sidecar_path):
                for vol_idx, expected_md5 in results:
                    rec = VolumeRecord.get(
                        (VolumeRecord.layout_tag == "concurrent_test")
                        & (VolumeRecord.volume_index == vol_idx)
                    )
                    self.assertEqual(rec.md5sum, expected_md5)


class TestPublicFacadeAndErrorEncapsulation(unittest.TestCase):
    """Valida los puntos de entrada principales y el aislamiento de excepciones de Peewee/SQLite."""

    def test_open_context_manager(self):
        """Verifica que tartape.open funcione como context manager."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "data"
            source.mkdir()
            (source / "file.txt").write_text("Hello TarTape 3.0")

            tartape.record(source)

            with tartape.open(source) as tape:
                self.assertEqual(tape.file_count, 2)
                self.assertTrue(tape.total_size > 0)
                chunks = list(tape.play())
                self.assertTrue(len(chunks) > 0)

    def test_open_with_custom_catalog_path(self):
        """Verifica que tartape.open respete catalog_path externo."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "data"
            source.mkdir()
            (source / "doc.txt").write_text("Custom catalog location")

            custom_cat = Path(tmp_dir) / "custom" / "backup.tartape"
            tartape.record(source, catalog_path=custom_cat)

            with tartape.open(source, catalog_path=custom_cat) as tape:
                self.assertEqual(tape.catalog_path, custom_cat)
                self.assertEqual(tape.file_count, 2)

    def test_corrupted_catalog_raises_domain_exception(self):
        """Verifica que un archivo .tartape corrupto lance TapeCorruptedError (no peewee/sqlite error)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            corrupted_file = Path(tmp_dir) / "corrupt.tartape"
            corrupted_file.write_bytes(b"THIS IS NOT A VALID SQLITE DATABASE")

            with self.assertRaises(tartape.TapeCorruptedError):
                with tartape.open_catalog(corrupted_file) as cat:
                    _ = cat.total_size

    def test_missing_tape_raises_tape_not_found(self):
        """Verifica que tartape.open en carpeta inexistente lance TapeNotFoundError."""
        with self.assertRaises(tartape.TapeNotFoundError):
            _ = tartape.open("/path/that/definitely/does/not/exist")


if __name__ == "__main__":
    unittest.main()
