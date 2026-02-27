import io
import os
import tarfile
import tempfile
import time
import unittest
from pathlib import Path

from tartape.player import TapePlayer
from tartape.recorder import TapeRecorder
from tartape.schemas import TarFileDataEvent
from tartape.tape import Tape


class TestTarIntegrity(unittest.TestCase):
    """
    Valida la robustez del motor ante cambios en el sistema de archivos
    durante la lectura.
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base_path = Path(self.temp.name) / "data"

    def tearDown(self):
        self.temp.cleanup()

    def _create_test_file(self, name: str, content: str = "hello world"):
        p = self.base_path / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        return p

    def test_full_workflow_and_compatibility(self):
        """
        Graba una carpeta, la abre con el Player y
        verifica que 'tarfile' pueda leer el stream resultante.
        """
        self._create_test_file("hola.txt", "Contenido 1")
        self._create_test_file("sub/mundo.txt", "Contenido 2")

        recorder = TapeRecorder(self.base_path)
        recorder.commit()

        with Tape.discover(self.base_path) as tape:
            self.assertGreater(tape.total_size, 0)

            player = TapePlayer(tape, directory=self.base_path)

            buffer = io.BytesIO()
            for event in player.play(fast_verify=False):
                if isinstance(event, TarFileDataEvent):
                    buffer.write(event.data)

            buffer.seek(0)

            with tarfile.open(fileobj=buffer, mode="r:") as tf:
                names = tf.getnames()
                self.assertIn("data/hola.txt", names)
                self.assertIn("data/sub/mundo.txt", names)

                content = tf.extractfile("data/hola.txt").read().decode()  # type: ignore
                self.assertEqual(content, "Contenido 1")

    def test_integrity_failure_size_changed(self):
        """ADR-002: Si el archivo cambia de tamaño tras la grabación, el Player debe abortar."""
        f = self._create_test_file("mutante.txt", "original")

        recorder = TapeRecorder(self.base_path)
        recorder.commit()

        # Modificamos el archivo en disco DESPUÉS de grabar la cinta
        f.write_text("contenido mucho mas largo")

        with Tape.discover(self.base_path) as tape:
            player = TapePlayer(tape, directory=self.base_path)

            with self.assertRaisesRegex(RuntimeError, "File size changed"):
                for _ in player.play(fast_verify=False):
                    pass

    def test_integrity_failure_mtime_changed(self):
        """ADR-002: Si el mtime cambia (aunque el tamaño sea igual), el Player debe abortar."""
        f = self._create_test_file("stale.txt", "mismo_tamano")

        recorder = TapeRecorder(self.base_path)
        recorder.commit()

        future_time = time.time() + 100

        os.utime(f, (future_time, future_time))

        with Tape.discover(self.base_path) as tape:
            player = TapePlayer(tape, directory=self.base_path)

            with self.assertRaisesRegex(RuntimeError, "File modified"):
                for _ in player.play(fast_verify=False):
                    pass

    def test_resume_at_specific_offset(self):
        """
        Prueba que el Player puede saltar bytes y el stream sigue siendo válido
        para la librería estándar (usando recortes).
        """
        self._create_test_file("a.txt", "AAAAA")  # 5 bytes + header + padding
        self._create_test_file("b.txt", "BBBBB")

        recorder = TapeRecorder(self.base_path)
        recorder.commit()

        with Tape.discover(self.base_path) as tape:

            # Obtenemos el offset de inicio del segundo archivo (b.txt)
            tracks = list(tape.get_tracks())
            # track[0]=dir, [1]=a.txt, [2]=b.txt
            target_track = tracks[2]
            start_at = target_track.start_offset

            player = TapePlayer(tape, directory=self.base_path)

            buffer = io.BytesIO()
            for event in player.play(start_offset=start_at, fast_verify=False):
                if isinstance(event, TarFileDataEvent):
                    buffer.write(event.data)

            buffer.seek(0)

            # El buffer resultante solo debería contener a b.txt y el footer
            with tarfile.open(fileobj=buffer, mode="r:") as tf:
                names = tf.getnames()
                self.assertEqual(len(names), 1)
                self.assertEqual(names[0], "data/b.txt")

    def test_identity_anonymization(self):
        """ADR-003: Verifica que por defecto se anonimicen UID/GID y nombres."""
        self._create_test_file("secret.txt")

        recorder = TapeRecorder(self.base_path, anonymize=True)
        recorder.commit()

        with Tape.discover(self.base_path) as tape:
            track = list(tape.get_tracks())[1]

            self.assertEqual(track.uid, 0)
            self.assertEqual(track.uname, "root")
            self.assertEqual(track.gid, 0)
            self.assertEqual(track.gname, "root")


if __name__ == "__main__":
    unittest.main()
