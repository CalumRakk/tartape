import os
import time

from tartape.exceptions import TarIntegrityError
from tartape.player import TapePlayer
from tartape.recorder import TapeRecorder
from tartape.tape import Tape
from tests.base import TarTapeTestCase


class TestIntegritySafeguards(TarTapeTestCase):
    def test_fail_fast_on_mutation(self):
        """Si un archivo cambia su mtime, el player debe explotar inmediatamente."""
        f = self.create_file("critical.db", "original_state")

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()

        # Mutación T1
        os.utime(f, (time.time() + 1000, time.time() + 1000))

        with Tape.discover(self.data_dir) as tape:
            player = TapePlayer(tape, self.data_dir)
            with self.assertRaisesRegex(TarIntegrityError, "File modified"):
                for _ in player.play(fast_verify=False):
                    pass

    def test_mtime_mutation_aborts_stream(self):
        """ADR-002: Si el mtime cambia después del T0, el stream debe fallar."""

        f = self.data_dir / "file.txt"
        f.write_text("content")

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()

        # Mutación: Cambiamos mtime al futuro
        os.utime(f, (time.time() + 100, time.time() + 100))

        tape = Tape.discover(self.data_dir)
        player = TapePlayer(tape, directory=self.data_dir)

        with self.assertRaisesRegex(TarIntegrityError, "File modified"):
            for _ in player.play(fast_verify=False):
                pass

    def test_size_mutation_aborts_stream(self):
        """Si el archivo crece, se aborta para no corromper el alineamiento de bloques."""

        f = self.data_dir / "grow.bin"
        f.write_text("original")
        recorder = TapeRecorder(self.data_dir).commit()

        f.write_text("original plus more")

        tape = Tape.discover(self.data_dir)
        with self.assertRaisesRegex(TarIntegrityError, "File size changed"):
            for _ in TapePlayer(tape, self.data_dir).play(fast_verify=False):
                pass

    def test_identity_anonymization(self):
        """ADR-003: Verifica que por defecto se anonimicen UID/GID y nombres."""
        self.create_file("secret.txt")

        recorder = TapeRecorder(self.data_dir, anonymize=True)
        recorder.commit()

        with Tape.discover(self.data_dir) as tape:
            track = list(tape.get_tracks())[1]

            self.assertEqual(track.uid, 0)
            self.assertEqual(track.uname, "root")
            self.assertEqual(track.gid, 0)
            self.assertEqual(track.gname, "root")
