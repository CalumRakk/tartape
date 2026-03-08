from tartape.catalog import Catalog
from tartape.chunker import TarChunker, calculate_segments
from tartape.models import TapeMetadata, Track
from tartape.schemas import ByteWindow, EntryState
from tests.base import TarTapeTestCase


class TestChunkerLogic(TarTapeTestCase):

    def test_fragmentation_states(self):
        """Verifica que el Chunker identifique correctamente HEAD, BODY, TAIL y COMPLETE."""

        with Catalog(":memory:") as catalog:
            TapeMetadata.create(key="fingerprint", value="hash123")
            TapeMetadata.create(key="total_size", value="200")

            # Nuestra cinta solo tendra un archivo de 100 bytes
            # Crea un archivo que cruza fronteras de volumen
            Track.create(
                arc_path="big_file.bin",
                rel_path="big_file.bin",
                size=100,
                start_offset=10,
                end_offset=110,
                mtime=0,
                mode=0o644,
                uid=0,
                gid=0,
                uname="root",
                gname="root",
                is_dir=False,
                is_symlink=False,
            )
            stats = catalog.get_stats()
            fingerprint = stats["fingerprint"]

            # Divimos la "cinta" en 4 partes
            segments = list(calculate_segments(stats["total_size"], 50))

            # Vol 0: Global Window [0, 50]
            start, end = segments[0]
            window = ByteWindow(start=start, end=end)
            vol0 = TarChunker.get_volume_manifest_for_range(fingerprint, 0, window)

            self.assertEqual(vol0.entries[0].state, EntryState.HEAD)
            self.assertEqual(vol0.entries[0].local_window.start, 10)
            self.assertEqual(vol0.entries[0].local_window.end, 50)
            self.assertEqual(vol0.entries[0].local_window.size, 40)

            # Vol 1: Global Window [50, 100]
            # El archivo cubre todo el volumen.
            start, end = segments[1]
            window = ByteWindow(start=start, end=end)
            vol1 = TarChunker.get_volume_manifest_for_range(fingerprint, 1, window)

            self.assertEqual(vol1.entries[0].state, EntryState.BODY)
            self.assertEqual(vol1.entries[0].local_window.start, 0)
            self.assertEqual(vol1.entries[0].local_window.end, 50)

            # Vol 2: Global Window [100, 150]
            # El archivo termina en el valor global 110. En relación con Vol Start (100), el final es 10.
            start, end = segments[2]
            window = ByteWindow(start=start, end=end)
            vol2 = TarChunker.get_volume_manifest_for_range(fingerprint, 2, window)

            self.assertEqual(vol2.entries[0].state, EntryState.TAIL)
            self.assertEqual(vol2.entries[0].local_window.start, 0)
            self.assertEqual(vol2.entries[0].local_window.end, 10)

    def test_chunk_size_alignment(self):
        """Verifica que un archivo pequeño quepa exacto como COMPLETE."""
        with Catalog(":memory:") as catalog:
            TapeMetadata.create(key="fingerprint", value="small_test")
            TapeMetadata.create(key="total_size", value="100")

            Track.create(
                arc_path="small.txt",
                rel_path="small.txt",
                size=20,
                start_offset=10,
                end_offset=30,
                mtime=0,
                mode=0o644,
                uid=0,
                gid=0,
                uname="root",
                gname="root",
            )
            stats = catalog.get_stats()
            f = stats["fingerprint"]

            # Nuestra cinta es solo un volumen de 100 bytes
            segments = list(calculate_segments(stats["total_size"], 100))
            self.assertEqual(len(segments), 1)

            start, end = segments[0]
            window = ByteWindow(start=start, end=end)

            vol = TarChunker.get_volume_manifest_for_range(f, 0, window)
            self.assertEqual(vol.entries[0].state, EntryState.COMPLETE)
            self.assertEqual(vol.entries[0].local_window.start, 10)
            self.assertEqual(vol.entries[0].local_window.end, 30)
