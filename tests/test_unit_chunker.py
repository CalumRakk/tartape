from tartape.catalog import Catalog
from tartape.chunker import TarChunker
from tartape.models import TapeMetadata, Track
from tartape.schemas import EntryState
from tests.base import TarTapeTestCase


class TestChunkerLogic(TarTapeTestCase):

    def test_fragmentation_states(self):
        """Verifica que el Chunker identifique correctamente HEAD, BODY, TAIL y COMPLETE."""

        with Catalog(":memory:") as tape:
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

            chunker = TarChunker(tape, chunk_size=50)
            plan = chunker.generate_plan()

            # Vol 0: [0-50). El archivo empieza en 10 y termina en 110.
            # El trozo en este volumen es [10, 50) -> HEAD
            vol0 = plan[0]
            self.assertEqual(vol0.entries[0].state, EntryState.HEAD)
            self.assertEqual(vol0.entries[0].offset_in_volume, 10)
            self.assertEqual(vol0.entries[0].bytes_in_volume, 40)

            # Vol 1: [50-100). El archivo ocupa todo el volumen -> BODY
            vol1 = plan[1]
            self.assertEqual(vol1.entries[0].state, EntryState.BODY)
            self.assertEqual(vol1.entries[0].offset_in_volume, 0)
            self.assertEqual(vol1.entries[0].bytes_in_volume, 50)

            # Vol 2: [100-150). El archivo termina en 110 -> TAIL
            vol2 = plan[2]
            self.assertEqual(vol2.entries[0].state, EntryState.TAIL)
            self.assertEqual(vol2.entries[0].offset_in_volume, 0)
            self.assertEqual(vol2.entries[0].bytes_in_volume, 10)

    def test_chunk_size_alignment(self):
        """Verifica que un archivo pequeño quepa exacto como COMPLETE."""
        with Catalog(":memory:") as tape:
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

            chunker = TarChunker(tape, chunk_size=100)
            plan = chunker.generate_plan()

            self.assertEqual(len(plan), 1)
            self.assertEqual(plan[0].entries[0].state, EntryState.COMPLETE)
            self.assertEqual(plan[0].entries[0].offset_in_volume, 10)
            self.assertEqual(plan[0].entries[0].bytes_in_volume, 20)
