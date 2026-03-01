import io
import tarfile
import unittest
from typing import cast

from tartape.database import DatabaseSession
from tartape.header import TarHeader
from tartape.models import Track


class TestHeaderCompliance(unittest.TestCase):
    """
    Pruebas quirúrgicas para el contrato de 512 bytes y ADR-004.
    """

    @classmethod
    def setUpClass(cls):
        cls.db_session = DatabaseSession(":memory:")
        cls.db = cls.db_session.connect()

    @classmethod
    def tearDownClass(cls):
        cls.db_session.close()

    def _create_minimal_track(self, **kwargs) -> Track:
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

    def test_standard_header_size(self):
        """Verifica que un archivo normal genera exactamente 512 bytes."""
        entry = self._create_minimal_track()
        header = TarHeader(entry)
        self.assertEqual(len(header.build()), 512)

    def test_symlink_target_too_long(self):
        # TODO ¿ESTO YA ESTA VERIFICADO?
        """Verifica el límite de 100 bytes para el destino de symlinks."""
        long_target = "b" * 110
        entry = self._create_minimal_track(is_symlink=True, linkname=long_target)

        with self.assertRaises(ValueError) as cm:
            entry = TarHeader(entry).build()

        self.assertIn("too long for field", str(cm.exception))

    def test_binary_identity_determinism(self):
        """
        Prueba reina: Dos entradas idénticas deben generar
        exactamente los mismos bytes de header.
        """
        e1 = self._create_minimal_track(size=10**10)  # 10GB
        e2 = self._create_minimal_track(size=10**10)

        info1 = tarfile.TarInfo(name=e1.arc_path)
        info1.size = e1.size
        info1.uid, info1.gid = e1.uid, e1.gid
        info1.uname, info1.gname = e1.uname, e1.gname
        info1.mtime = e1.mtime

        info2 = tarfile.TarInfo(name=e2.arc_path)
        info2.size = e2.size
        info2.uid, info2.gid = e2.uid, e2.gid
        info2.uname, info2.gname = e2.uname, e2.gname
        info2.mtime = e2.mtime

        h1 = info1.tobuf(format=tarfile.GNU_FORMAT)
        h2 = info2.tobuf(format=tarfile.GNU_FORMAT)

        self.assertEqual(h1, h2, "Los headers no son idénticos bit a bit")

    def test_binary_identity(self):
        """Garantiza que el header es idéntico bit a bit sin importar el entorno, siempre que los datos de entrada sean los mismos."""

        params = {
            "arc_path": "test/path/file.txt",
            "size": 5000,
            "mtime": 123456789.0,
            "uname": "root",
            "gname": "root",
        }

        e1 = self._create_minimal_track(**params)
        e2 = self._create_minimal_track(**params)

        h1 = TarHeader(e1).build()
        h2 = TarHeader(e2).build()

        self.assertEqual(h1, h2, "Los headers generados no son idénticos bit a bit")
        # Verificamos la firma USTAR en la posición correcta (offset 257)
        self.assertEqual(h1[257:262], b"ustar", "Falta el magic string 'ustar'")

    def test_base256_roundtrip_with_standard_library(self):

        # Creamos una entrada de 10 GiB
        giant_size = 10 * 1024 * 1024 * 1024
        entry = self._create_minimal_track(size=giant_size, arc_path="giant.bin")

        header_bytes = TarHeader(entry).build()
        self.assertEqual(
            len(header_bytes), 512, "El header debe medir exactamente 512 bytes"
        )

        # Usamos tf.next() para leer el header sin saltar al contenido
        full_tar = header_bytes + (b"\0" * 1024)
        with tarfile.open(fileobj=io.BytesIO(full_tar), mode="r") as tf:
            member = cast(tarfile.TarInfo, tf.next())

            self.assertIsNotNone(member, "No se pudo leer el miembro del TAR")
            self.assertEqual(member.name, "giant.bin")
            self.assertEqual(
                member.size,
                giant_size,
                "La librería estándar no reconoció el tamaño Base-256",
            )

    def test_header_fields_precision_alignment(self):
        """
        Test de estrés estructural: Verifica que todos los campos estén en su
        posición exacta y que 'tarfile' pueda recuperarlos sin errores.
        """
        params = {
            "arc_path": "deep/folder/structure/file.txt",
            "size": 123456,
            "mtime": 1600000000.0,
            "mode": 0o755,
            "uid": 4321,
            "gid": 8765,
            "uname": "tartape-user",
            "gname": "tartape-group",
        }

        entry = self._create_minimal_track(**params)
        header_bytes = TarHeader(entry).build()

        full_tar = header_bytes + (b"\0" * 1024)

        with tarfile.open(fileobj=io.BytesIO(full_tar), mode="r") as tf:
            member = cast(tarfile.TarInfo, tf.next())
            self.assertIsNotNone(member)

            self.assertEqual(member.name, params["arc_path"], "Ruta corrupta")
            self.assertEqual(member.size, params["size"], "Tamaño corrupto o solapado")
            self.assertEqual(member.mode, params["mode"], "Permisos (mode) corruptos")
            self.assertEqual(member.uid, params["uid"], "UID corrupto")
            self.assertEqual(member.gid, params["gid"], "GID corrupto")
            self.assertEqual(member.mtime, int(params["mtime"]), "mtime corrupto")
            self.assertEqual(member.uname, params["uname"], "Username corrupto")
            self.assertEqual(member.gname, params["gname"], "Groupname corrupto")

            self.assertTrue(member.chksum > 0)


if __name__ == "__main__":
    unittest.main()
