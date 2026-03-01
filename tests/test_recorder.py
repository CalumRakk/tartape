from tartape.recorder import TapeRecorder
from tartape.tape import Tape
from tests.base import TarTapeTestCase


class TestRecorderSalvaguarda(TarTapeTestCase):
    def test_deterministic_ordering_adr001(self):
        """Garantiza que el orden en la DB sea siempre alfabético."""
        self.create_file("z.txt")
        self.create_file("a.txt")
        self.create_file("m.txt")

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()

        with Tape.discover(self.data_dir) as tape:
            tracks = [t.arc_path for t in tape.get_tracks() if t.is_file]
            # Debería ser [dataset/a.txt, dataset/m.txt, dataset/z.txt]
            self.assertEqual(tracks, sorted(tracks))

    def test_exclusion_logic(self):
        """Verifica que los archivos excluidos no lleguen a la cinta."""
        self.create_file("keep.txt")
        self.create_file("ignore.log")

        recorder = TapeRecorder(self.data_dir, exclude="*.log")
        recorder.commit()

        with Tape.discover(self.data_dir) as tape:
            paths = [t.arc_path for t in tape.get_tracks()]
            self.assertTrue(any("keep.txt" in p for p in paths))
            self.assertFalse(any("ignore.log" in p for p in paths))
