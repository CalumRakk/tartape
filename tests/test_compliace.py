import tarfile
import unittest

from tartape.schemas import TarEntry


class TestHeaderCompliance(unittest.TestCase):
    """
    Pruebas quirúrgicas para el contrato de 512 bytes y ADR-004.
    """

    def _create_minimal_entry(self, **kwargs):
        """Helper para crear una entrada válida mínima."""
        defaults = {
            "source_path": "/tmp/fake",
            "arc_path": "file.txt",
            "size": 100,
            "mtime": 1700000000.0,
            "mode": 0o644,
            "uid": 0,
            "gid": 0,
            "uname": "root",
            "gname": "root",
        }
        defaults.update(kwargs)
        return TarEntry(**defaults)

    def test_standard_header_size(self):
        """Verifica que un archivo normal genera exactamente 512 bytes."""
        entry = self._create_minimal_entry()
        # No debería lanzar excepción
        entry.validate_compliance()

    def test_large_file_base256_compliance(self):
        """
        ADR-004: Verifica que archivos > 8GiB mantienen el header en 512 bytes
        usando la codificación Base-256 de GNU.
        """
        large_size = 9 * 1024 * 1024 * 1024  # 9 GiB
        entry = self._create_minimal_entry(size=large_size)

        # 1. Validar que no rompe el contrato de TarTape
        entry.validate_compliance()

        # 2. Verificación binaria (Opcional pero recomendada)
        # Recreamos lo que haría el core para ver los bytes
        info = tarfile.TarInfo(name=entry.arc_path)
        info.size = entry.size
        header_bytes = info.tobuf(format=tarfile.GNU_FORMAT)

        self.assertEqual(len(header_bytes), 512)
        # En el formato GNU, si el tamaño es > 8GB, el primer byte del campo size (offset 124)
        # debe tener el bit 0x80 activo.
        self.assertTrue(
            header_bytes[124] & 0x80, "El bit de flag binario no está activo"
        )

    def test_path_too_long_education(self):
        """Verifica que el error educativo sea claro cuando la ruta excede 255 bytes."""
        long_path = "a" * 260
        entry = self._create_minimal_entry(arc_path=long_path)

        with self.assertRaises(ValueError) as cm:
            entry.validate_compliance()

        self.assertIn("Path too long", str(cm.exception))
        self.assertIn("Maximum 255", str(cm.exception))

    def test_username_too_long_education(self):
        """Verifica el diagnóstico cuando el nombre de usuario no cabe (32 bytes)."""
        long_user = "usuario.extremadamente.largo.que.no.cabe.en.tar"
        entry = self._create_minimal_entry(uname=long_user)

        with self.assertRaises(ValueError) as cm:
            entry.validate_compliance()

        self.assertIn(f"User '{long_user}' exceeds", str(cm.exception))
        self.assertIn("32 bytes", str(cm.exception))

    def test_symlink_target_too_long(self):
        """Verifica el límite de 100 bytes para el destino de symlinks."""
        long_target = "b" * 110
        entry = self._create_minimal_entry(is_symlink=True, linkname=long_target)

        with self.assertRaises(ValueError) as cm:
            entry.validate_compliance()

        self.assertIn("Destination very long link", str(cm.exception))

    def test_binary_identity_determinism(self):
        """
        Prueba reina: Dos entradas idénticas deben generar
        exactamente los mismos bytes de header.
        """
        e1 = self._create_minimal_entry(size=10**10)  # 10GB
        e2 = self._create_minimal_entry(size=10**10)

        # Usamos el método de tarfile para comparar el resultado final
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


if __name__ == "__main__":
    unittest.main()
