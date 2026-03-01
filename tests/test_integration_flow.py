import io
import os
import tarfile
import time

from tartape.exceptions import TarIntegrityError
from tartape.player import TapePlayer
from tartape.recorder import TapeRecorder
from tartape.schemas import TarFileDataEvent
from tartape.tape import Tape
from tests.base import TarTapeTestCase


class TestFlow(TarTapeTestCase):
    def test_end_to_end_pipeline(self):
        self.create_file("root_file.txt", "contenido raiz")
        self.create_file("sub/folder/nested.txt", "contenido anidado")
        self.create_file("empty.txt", "")  # Caso de borde: archivo vacío

        # Grabación
        recorder = TapeRecorder(self.data_dir)
        fingerprint = recorder.commit()
        self.assertIsNotNone(fingerprint)

        # Reproducción y Captura del Stream
        with Tape.discover(self.data_dir) as tape:
            player = TapePlayer(tape, directory=self.data_dir)

            tar_buffer = io.BytesIO()
            for event in player.play(fast_verify=False):
                if isinstance(event, TarFileDataEvent):
                    tar_buffer.write(event.data)

        tar_buffer.seek(0)
        with tarfile.open(fileobj=tar_buffer, mode="r:") as tf:
            members = tf.getnames()

            # Verificamos que los nombres en el TAR sigan nuestra estructura
            # TarTape incluye el nombre de la carpeta raíz por diseño
            root_name = self.data_dir.name
            expected_files = [
                f"{root_name}/root_file.txt",
                f"{root_name}/sub/folder/nested.txt",
                f"{root_name}/empty.txt",
            ]

            for expected in expected_files:
                self.assertIn(expected, members)

            f = tf.extractfile(f"{root_name}/sub/folder/nested.txt")
            assert f is not None, "El archivo no se pudo extraer"
            self.assertEqual(f.read().decode(), "contenido anidado")

    def test_tape_discovery_fails_if_no_tape(self):
        """Asegura que el sistema falle correctamente si no hay grabación previa."""
        with self.assertRaises(FileNotFoundError):
            Tape.discover(self.data_dir)

    def test_integrity_ignore_root_mtime_mutation(self):
        """ADR-002: El cambio de mtime en el directorio raíz NO debe abortar el stream."""
        self.create_file("data.txt", "content")

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()

        # Mutamos el mtime del ROOT (directorio dataset/)
        past_time = time.time() - 10000
        os.utime(self.data_dir, (past_time, past_time))

        with Tape.discover(self.data_dir) as tape:
            player = TapePlayer(tape, self.data_dir)
            # Esto NO debe lanzar RuntimeError
            events = list(player.play(fast_verify=False))
            self.assertTrue(any(e.type == "tape_completed" for e in events))

    def test_integrity_subdirectory_mtime_aborts(self):
        """ADR-002: El mtime de un SUB-directorio SÍ es crítico y debe abortar."""
        sub = self.data_dir / "subdir"
        sub.mkdir()
        self.create_file("subdir/file.txt", "content")

        TapeRecorder(self.data_dir).commit()

        # Mutamos el mtime del SUB-directorio
        os.utime(sub, (time.time() + 500, time.time() + 500))

        with Tape.discover(self.data_dir) as tape:
            player = TapePlayer(tape, self.data_dir)
            with self.assertRaisesRegex(
                TarIntegrityError, "Directory structure changed"
            ):
                for _ in player.play(fast_verify=False):
                    pass
