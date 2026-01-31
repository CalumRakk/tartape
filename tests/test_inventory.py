import tempfile
import unittest
from pathlib import Path

from tartape import TarTape
from tartape.enums import TarEventType


class TestSqlInventory(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp_dir.name)

    def test_deterministic_ordering(self):
        """
        Crea archivos en orden no alfabético y verifica que el TAR
        los procesa siempre en orden A-Z.
        """
        (self.root / "z.txt").touch()
        (self.root / "a.txt").touch()
        (self.root / "m.txt").touch()

        tape = TarTape()
        tape.add_folder(self.root)

        # Recolectamos los nombres de los archivos según aparecen en el stream
        processed_files = []
        for event in tape.stream():
            if event.type == TarEventType.FILE_START:
                processed_files.append(Path(event.entry.arc_path).name)

        # El resultado DEBE estar ordenado alfabéticamente
        expected = ["a.txt", "m.txt", "z.txt"]

        files_only = [f for f in processed_files if f.endswith(".txt")]

        self.assertEqual(files_only, expected)

    def test_stream_resumption(self):
        """
        Simula un fallo a mitad de un stream y verifica que podemos
        reanudar desde el último archivo completado.
        """

        for i in range(10):
            (self.root / f"file_{i:02d}.txt").write_text(f"content {i}")

        # Primer intento: Procesamos hasta el archivo 04 y "fallamos"
        tape = TarTape()
        tape.add_folder(self.root)

        all_events = list(tape.stream())

        # Simular reanudación desde 'file_04.txt'
        resume_path = f"{self.root.name}/file_04.txt"

        resumed_events = list(tape.stream(resume_from=resume_path))

        resumed_files = [
            e.entry.arc_path
            for e in resumed_events
            if e.type == TarEventType.FILE_START
        ]

        # No debería estar el 04 (porque reanudamos DESPUÉS de ese)
        self.assertNotIn(resume_path, resumed_files)
        # Debería empezar en el 05
        self.assertEqual(resumed_files[0], f"{self.root.name}/file_05.txt")
        # Debería terminar en el 09
        self.assertEqual(resumed_files[-1], f"{self.root.name}/file_09.txt")

    def test_inventory_persistence(self):
        db_file = self.root / "index.db"

        tape1 = TarTape(index_path=str(db_file))
        (self.root / "persist.txt").touch()
        tape1.add_file(self.root / "persist.txt", arcname="p.txt")

        tape2 = TarTape(index_path=str(db_file))
        events = list(tape2.stream())

        filenames = [
            e.entry.arc_path for e in events if e.type == TarEventType.FILE_START
        ]
        self.assertIn("p.txt", filenames)
