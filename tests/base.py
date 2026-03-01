import shutil
import tempfile
import unittest
from pathlib import Path


class TarTapeTestCase(unittest.TestCase):
    """Clase base para todos los tests de TarTape."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.data_dir = self.tmp / "dataset"
        self.data_dir.mkdir()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def create_file(self, rel_path: str, content: str = "data"):
        p = self.data_dir / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        return p
