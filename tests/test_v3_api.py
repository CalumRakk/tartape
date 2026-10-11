"""
tests/test_v3_api.py

Comprehensive test suite for TarTape 3.0 high-level API contracts:
1. Multi-layout isolation, tags, and AmbiguousLayoutError handling.
2. Formal TarObserver telemetry lifecycle protocol during byte playback.
3. Byte-perfect resumption with start_offset and telemetry flags.
4. End-to-end zero-disk surgical reconstruction using only .tartape catalog and volumes.
5. High-concurrency volume reading and atomic MD5 persistence.
"""

import hashlib
import os
import shutil
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import tartape
from tartape.database import DatabaseSession
from tartape.exceptions import AmbiguousLayoutError, LayoutNotFoundError
from tartape.models import VolumeRecord
from tartape.schemas import (
    TarFileEndEvent,
    TarFileStartEvent,
    TarObserver,
    TarTapeCompletedEvent,
)


def compute_md5(data: bytes) -> str:
    """Helper to calculate the MD5 hash of raw bytes."""
    return hashlib.md5(data).hexdigest()


class TestMultiLayoutAndAmbiguity(unittest.TestCase):
    """Valida la gestión de múltiples layouts, selección por tag/size y control de ambigüedad."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tmp_dir.name)
        self.source = self.base_dir / "dataset"
        self.source.mkdir()

        (self.source / "file1.txt").write_bytes(b"A" * 1024)
        (self.source / "file2.txt").write_bytes(b"B" * 2048)

        self.sidecar = tartape.record(self.source)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_multi_layout_registration_and_retrieval(self):
        """Verifica que se puedan registrar múltiples layouts sin colisión."""
        with tartape.open_catalog(self.sidecar) as catalog:
            layout_s3 = catalog.register_layout(
                volume_size=1024, tag="s3_plan", is_default=False
            )
            layout_tg = catalog.register_layout(
                volume_size=2048, tag="tg_plan", is_default=False
            )

            self.assertEqual(len(catalog.layouts), 2)
            self.assertEqual(layout_s3.tag, "s3_plan")
            self.assertEqual(layout_tg.tag, "tg_plan")

            # Retrieval by tag
            self.assertEqual(catalog.get_layout("s3_plan").volume_size, 1024)
            self.assertEqual(catalog.get_layout("tg_plan").volume_size, 2048)

            # Retrieval by volume size
            self.assertEqual(catalog.get_layout(1024).tag, "s3_plan")
            self.assertEqual(catalog.get_layout(2048).tag, "tg_plan")

    def test_ambiguous_layout_error_when_no_default_exists(self):
        """Verifica que catalog.default_layout lance AmbiguousLayoutError si hay múltiples layouts sin default."""
        with tartape.open_catalog(self.sidecar) as catalog:
            catalog.register_layout(volume_size=1024, tag="plan_a", is_default=False)
            catalog.register_layout(volume_size=2048, tag="plan_b", is_default=False)

            with self.assertRaises(AmbiguousLayoutError):
                _ = catalog.default_layout

            # Non-existent layout lookup
            with self.assertRaises(LayoutNotFoundError):
                catalog.get_layout("non_existent_plan")

    def test_default_layout_resolution(self):
        """Verifica que marcar un layout como is_default resuelva la ambigüedad limpiamente."""
        with tartape.open_catalog(self.sidecar) as catalog:
            catalog.register_layout(volume_size=1024, tag="first", is_default=False)
            catalog.register_layout(volume_size=2048, tag="preferred", is_default=True)

            # default_layout should resolve to 'preferred'
            self.assertEqual(catalog.default_layout.tag, "preferred")

            # catalog.locate() shortcut without tag should use the default layout
            gps = catalog.locate("dataset/file1.txt")
            self.assertEqual(gps.arc_path, "dataset/file1.txt")
            self.assertTrue(len(gps.fragments) > 0)


class CustomTelemetryObserver(TarObserver):
    """Implementación de prueba conforme al protocolo formal TarObserver."""

    def __init__(self):
        self.started_events: list[TarFileStartEvent] = []
        self.ended_events: list[TarFileEndEvent] = []
        self.completed_event: TarTapeCompletedEvent | None = None

    def on_file_start(self, event: TarFileStartEvent) -> None:
        self.started_events.append(event)

    def on_file_end(self, event: TarFileEndEvent) -> None:
        self.ended_events.append(event)

    def on_tape_completed(self, event: TarTapeCompletedEvent) -> None:
        self.completed_event = event


class TestObserverTelemetryProtocol(unittest.TestCase):
    """Valida el protocolo de observabilidad TarObserver in-band durante la emisión de bytes."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tmp_dir.name)
        self.source = self.base_dir / "my_data"
        self.source.mkdir()

        (self.source / "doc.txt").write_bytes(b"TarTape Telemetry Test")
        (self.source / "sub").mkdir()
        (self.source / "sub" / "image.bin").write_bytes(b"XYZ" * 500)

        # Enable MD5 checksum so that TarFileEndEvent carries the expected file checksum
        self.tape = tartape.create(self.source, checksum="md5")

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_observer_receives_in_band_lifecycle_events(self):
        """Verifica que tape.play despache los eventos en tiempo y forma al objeto observer."""
        observer = CustomTelemetryObserver()

        # Stream raw bytes while dispatching to observer
        stream_generator = self.tape.play(buffer_size=512, observer=observer)
        raw_bytes = b"".join(stream_generator)

        # 1. Byte integrity: total bytes match tape size
        self.assertEqual(len(raw_bytes), self.tape.total_size)

        # 2. Tracks count: root folder (1), doc.txt (2), sub folder (3), image.bin (4)
        self.assertEqual(len(observer.started_events), 4)
        self.assertEqual(len(observer.ended_events), 4)
        self.assertIsNotNone(observer.completed_event)

        # 3. Verify event details for regular files
        doc_end_event = next(
            ev
            for ev in observer.ended_events
            if ev.entry.info.arc_path == "my_data/doc.txt"
        )
        expected_doc_hash = compute_md5(b"TarTape Telemetry Test")
        self.assertEqual(doc_end_event.metadata.md5sum, expected_doc_hash)
        self.assertTrue(doc_end_event.metadata.is_complete)


class TestByteLevelResumptionAndMetadata(unittest.TestCase):
    """Valida la reanudación byte a byte en offsets arbitrarios y la bandera 'resumed'."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tmp_dir.name)
        self.source = self.base_dir / "resume_data"
        self.source.mkdir()

        (self.source / "lead.txt").write_bytes(b"L" * 1000)
        (self.source / "big_target.bin").write_bytes(b"T" * 20000)
        (self.source / "tail.txt").write_bytes(b"Z" * 500)

        self.tape = tartape.create(self.source)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_exact_byte_level_resumption(self):
        """Verifica que reanudar en un offset arbitrario coincida exactamente con el sufijo original."""
        full_bytes = b"".join(self.tape.play())
        self.assertEqual(len(full_bytes), self.tape.total_size)

        # Pick an unaligned offset in the middle of big_target.bin content
        resume_offset = 2500

        observer = CustomTelemetryObserver()
        resumed_bytes = b"".join(
            self.tape.play(start_offset=resume_offset, observer=observer)
        )

        # Expected suffix
        expected_suffix = full_bytes[resume_offset:]

        self.assertEqual(len(resumed_bytes), len(expected_suffix))
        self.assertEqual(resumed_bytes, expected_suffix)

        # The first file touched must have resumed=True in metadata
        first_event = observer.started_events[0]
        self.assertTrue(first_event.metadata.resumed)


class TestZeroDiskSurgicalReconstruction(unittest.TestCase):
    """
    Prueba de fuego de extracción quirúrgica offline:
    Borra la carpeta de origen y reconstruye los archivos usando solo el catálogo .tartape y los volúmenes.
    """

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tmp_dir.name)
        self.source = self.base_dir / "archive_dataset"
        self.source.mkdir()

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_offline_surgical_file_reconstruction(self):
        """Reconstruye archivos de volúmenes físicos sin que exista la carpeta original en disco."""
        # 1. Create original files with diverse sizes
        file_a_data = os.urandom(7500)
        file_b_data = os.urandom(20480)
        file_c_data = os.urandom(300)

        (self.source / "file_a.bin").write_bytes(file_a_data)
        (self.source / "file_b.bin").write_bytes(file_b_data)
        (self.source / "file_c.bin").write_bytes(file_c_data)

        expected_files = {
            "archive_dataset/file_a.bin": (
                compute_md5(file_a_data),
                len(file_a_data),
            ),
            "archive_dataset/file_b.bin": (
                compute_md5(file_b_data),
                len(file_b_data),
            ),
            "archive_dataset/file_c.bin": (
                compute_md5(file_c_data),
                len(file_c_data),
            ),
        }

        # 2. Record tape and slice into physical volumes on disk
        sidecar_path = tartape.record(self.source)
        tape = tartape.open(self.source, catalog_path=sidecar_path)

        chunk_size = 4096  # 4 KB slices
        volumes_dir = self.base_dir / "physical_volumes"
        volumes_dir.mkdir()

        volume_paths = []
        for vol in tape.iter_volumes(size=chunk_size, tag="offline_layout"):
            vol_path = volumes_dir / vol.name
            volume_paths.append(vol_path)
            with vol, open(vol_path, "wb") as f_out:
                f_out.write(vol.read())

        # 3. ZERO-DISK STATE: Completely wipe out the source directory!
        shutil.rmtree(self.source)
        self.assertFalse(self.source.exists())

        # 4. Open catalog in detached/zero-disk mode
        with tartape.open_catalog(sidecar_path) as catalog:
            layout = catalog.get_layout("offline_layout")

            # 5. Surgical reconstruction for each file
            for arc_path, (expected_md5, original_size) in expected_files.items():
                gps = layout.locate(arc_path)
                self.assertEqual(gps.file_size, original_size)

                reconstructed_buffer = bytearray(original_size)

                for frag in gps.fragments:
                    vol_file = volume_paths[frag.volume_index]

                    with open(vol_file, "rb") as vf:
                        # Surgical jump to volume coordinate
                        vf.seek(frag.volume_offset)
                        chunk = vf.read(frag.volume_length)
                        self.assertEqual(len(chunk), frag.volume_length)

                        # Write at the precise source file offset
                        reconstructed_buffer[
                            frag.source_offset : frag.source_offset + frag.volume_length
                        ] = chunk

                # Validate bit-for-bit MD5 integrity
                reconstructed_md5 = compute_md5(bytes(reconstructed_buffer))
                self.assertEqual(
                    reconstructed_md5,
                    expected_md5,
                    f"Surgical integrity mismatch for '{arc_path}'",
                )


class TestConcurrentVolumeReadingAndHashing(unittest.TestCase):
    """Valida la concurrencia multi-hilo al leer volúmenes y la persistencia segura de MD5 en SQLite."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tmp_dir.name)
        self.source = self.base_dir / "concurrent_dataset"
        self.source.mkdir()

        # Generate ~64 KB of random data
        (self.source / "heavy.bin").write_bytes(os.urandom(64 * 1024))
        self.tape = tartape.create(self.source)
        self.sidecar = self.base_dir / "concurrent_dataset.tartape"

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_parallel_volume_streaming_persists_md5_without_locks(self):
        """Verifica que múltiples hilos puedan procesar volúmenes en paralelo sin SQLite lock errors."""
        chunk_size = 8192  # 8 KB chunks -> ~8-9 volumes
        volumes = list(self.tape.iter_volumes(size=chunk_size, tag="parallel_stress"))
        self.assertGreaterEqual(len(volumes), 4)

        def worker(v):
            with v:
                _ = v.read()
            return v.index, v.md5sum

        # Stream all volumes concurrently using 4 worker threads
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(worker, volumes))

        self.assertEqual(len(results), len(volumes))

        # Check that all volume MD5s were saved into SQLite VolumeRecord
        with DatabaseSession(self.sidecar):
            for vol_idx, expected_md5 in results:
                rec = VolumeRecord.get(
                    (VolumeRecord.layout_tag == "parallel_stress")
                    & (VolumeRecord.volume_index == vol_idx)
                )
                self.assertIsNotNone(rec.md5sum)
                self.assertEqual(rec.md5sum, expected_md5)


if __name__ == "__main__":
    unittest.main()
