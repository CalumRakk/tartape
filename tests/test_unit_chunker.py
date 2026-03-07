from tartape.catalog import Catalog
from tartape.chunker import TarChunker, calculate_segments
from tartape.models import TapeMetadata, Track
from tartape.schemas import EntryState
from tests.base import TarTapeTestCase


class TestChunkerLogic(TarTapeTestCase):

    def test_fragmentation_states(self):
        """Verifica que el Chunker identifique correctamente HEAD, BODY, TAIL y COMPLETE."""

        with Catalog(":memory:") as catalog:
            TapeMetadata.create(key="fingerprint", value="hash123")
            TapeMetadata.create(key="total_size", value="200")

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
            f = stats["fingerprint"]
            segments = list(calculate_segments(stats["total_size"], 50))

            # Vol 0: El archivo empieza en 10 y termina en 110.
            start = segments[0][0]
            end = segments[0][1]
            vol0 = TarChunker.get_volume_manifest_for_range(f, 0, start, end)
            self.assertEqual(vol0.entries[0].state, EntryState.HEAD)
            self.assertEqual(vol0.entries[0].offset_in_volume, 10)
            self.assertEqual(vol0.entries[0].bytes_in_volume, 40)

            # Vol 1: El archivo ocupa todo el volumen -> BODY
            start = segments[1][0]
            end = segments[1][1]
            vol1 = TarChunker.get_volume_manifest_for_range(f, 1, start, end)
            self.assertEqual(vol1.entries[0].state, EntryState.BODY)
            self.assertEqual(vol1.entries[0].offset_in_volume, 0)
            self.assertEqual(vol1.entries[0].bytes_in_volume, 50)

            # Vol 2: El archivo termina en 110 -> TAIL
            start = segments[2][0]
            end = segments[2][1]
            vol2 = TarChunker.get_volume_manifest_for_range(f, 2, start, end)
            self.assertEqual(vol2.entries[0].state, EntryState.TAIL)
            self.assertEqual(vol2.entries[0].offset_in_volume, 0)
            self.assertEqual(vol2.entries[0].bytes_in_volume, 10)

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

            # chunker = TarChunker(chunk_size=100)
            stats = catalog.get_stats()
            f = stats["fingerprint"]
            segments = list(calculate_segments(stats["total_size"], 100))
            start = segments[0][0]
            end = segments[0][1]

            vol = TarChunker.get_volume_manifest_for_range(f, 0, start, end)
            self.assertEqual(len(segments), 1)
            self.assertEqual(vol.entries[0].state, EntryState.COMPLETE)
            self.assertEqual(vol.entries[0].offset_in_volume, 10)
            self.assertEqual(vol.entries[0].bytes_in_volume, 20)
