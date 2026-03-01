import shutil
import tempfile
import unittest
from pathlib import Path

from tartape.models import Track


class TarTapeTestCase(unittest.TestCase):
    """Clase base para todos los tests de TarTape."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.data_dir = self.tmp / "dataset"
        self.data_dir.mkdir()

    def tearDown(self):

        if self.tmp.exists():
            shutil.rmtree(self.tmp)

    def create_file(self, rel_path: str, content: str = "data"):
        p = self.data_dir / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        return p

    def create_minimal_track(self, **kwargs) -> Track:
        """Helper para crear una instancia de Track válida para pruebas de header."""
        defaults = {
            "arc_path": "file.txt",
            "rel_path": "path/to/file.txt",
            "size": 100,
            "mtime": 1700000000,
            "mode": 0o644,
            "uid": 0,
            "gid": 0,
            "uname": "root",
            "gname": "root",
            "is_dir": False,
            "is_symlink": False,
            "linkname": "",
        }
        defaults.update(kwargs)
        return Track(**defaults)
