from tartape.header import TarHeader
from tartape.models import Track
from tartape.recorder import TapeRecorder
from tartape.tape import Tape
from tests.base import TarTapeTestCase


class TestTarProtocol(TarTapeTestCase):
    """Garantiza que el flujo de bytes cumpla con USTAR y extensiones GNU."""

    def test_large_file_base256(self):
        """Verifica que archivos > 8GiB mantienen el bloque de 512 bytes (GNU extension)."""
        giant_size = 10 * 1024 * 1024 * 1024
        track = Track(
            arc_path="giant.bin",
            size=giant_size,
            mtime=123,
            mode=0o644,
            uname="root",
            gname="root",
            uid=0,
            gid=0,
        )

        header = TarHeader(track).build()

        self.assertEqual(len(header), 512, "El bloque de header debe ser de 512 bytes")
        self.assertEqual(header[124], 0x80, "Debe tener el flag binario Base-256")

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
