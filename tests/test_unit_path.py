import io
import tarfile
import unittest

from tartape.exceptions import PathConstraintError
from tartape.factory import TarEntryFactory, validate_ustar_path
from tartape.header import TarHeader
from tartape.models import Track


class TestPathLogic(unittest.TestCase):
    """
    Validación exhaustiva de ADR-005, compatibilidad USTAR y auto_truncate.
    """

    def _get_header(self, path: str, is_dir: bool = False):
        track = Track(
            arc_path=path,
            size=0,
            mtime=0,
            mode=0o755 if is_dir else 0o644,
            uid=0,
            gid=0,
            uname="root",
            gname="root",
            is_dir=is_dir,
        )
        return TarHeader(track)

    def test_component_limit_strict_adr005(self):
        """ADR-005: Ningún componente puede medir más de 100 bytes sin auto_truncate."""
        long_component = "a" * 101
        path = f"folder/{long_component}"

        with self.assertRaisesRegex(PathConstraintError, "exceeds 100 bytes"):
            TarEntryFactory.resolve_arcname(path, auto_truncate=False)

    def test_total_path_limit_ustar(self):
        """USTAR: La ruta total no puede exceder los 255 bytes sin auto_truncate."""
        path = "a" * 80 + "/" + "b" * 80 + "/" + "c" * 80 + "/" + "d" * 20
        self.assertGreater(len(path.encode()), 255)

        with self.assertRaisesRegex(
            PathConstraintError, "exceeds USTAR 255 byte limit"
        ):
            TarEntryFactory.resolve_arcname(path, auto_truncate=False)

    def test_the_dead_zone_case(self):
        """
        CASO BORDE: Ruta legal en longitud total (< 255) pero indivisible en USTAR (prefijo > 155 o nombre > 100).
        """
        path = ("a" * 90) + "/" + ("b" * 90) + "/" + ("c" * 70)

        # Sin auto_truncate, debe fallar con error explicativo
        with self.assertRaisesRegex(
            PathConstraintError, "cannot be split into USTAR prefix"
        ):
            TarEntryFactory.resolve_arcname(path, auto_truncate=False)

        # Con auto_truncate, debe resolverlo deterministamente a una ruta compatible
        resolved = TarEntryFactory.resolve_arcname(path, auto_truncate=True)
        valid, reason = validate_ustar_path(resolved)
        self.assertTrue(valid, f"No es válida: {reason}")
        self.assertLessEqual(len(resolved.encode("utf-8")), 255)

        # La cabecera TAR debe construirse sin errores en 512 bytes
        header_bytes = self._get_header(resolved).build()
        self.assertEqual(len(header_bytes), 512)

    def test_auto_truncate_deep_path_over_255_bytes(self):
        """
        Una ruta profunda de 10 niveles que supera ampliamente 255 bytes
        debe resolverse a una ruta USTAR válida preservando la raíz y el nombre del archivo.
        """
        # Generamos una ruta de ~320 bytes (> 255)
        deep_folder_chain = "/".join([f"sub_directorio_nivel_{i}" for i in range(12)])
        deep_path = f"DISCO C/{deep_folder_chain}/test_documento_importante.docx"

        self.assertGreater(len(deep_path.encode("utf-8")), 255)

        # Sin auto_truncate debe fallar
        with self.assertRaises(PathConstraintError):
            TarEntryFactory.resolve_arcname(deep_path, auto_truncate=False)

        # Con auto_truncate debe acortarse deterministamente
        resolved = TarEntryFactory.resolve_arcname(deep_path, auto_truncate=True)

        self.assertLessEqual(len(resolved.encode("utf-8")), 255)
        self.assertTrue(resolved.startswith("DISCO C/"))
        self.assertTrue(resolved.endswith("test_documento_importante.docx"))
        self.assertIn("~", resolved)  # Contiene el marcador hash determinista

        # Verifica que el TAR Header lo acepte y mida exactamente 512 bytes
        header = self._get_header(resolved).build()
        self.assertEqual(len(header), 512)

    def test_auto_truncate_determinism(self):
        """Misma ruta profunda debe producir exactamente el mismo string resuelto."""
        deep_path = (
            "root/" + "/".join(["subfolder_" + str(i) for i in range(15)]) + "/file.txt"
        )

        resolved_1 = TarEntryFactory.resolve_arcname(deep_path, auto_truncate=True)
        resolved_2 = TarEntryFactory.resolve_arcname(deep_path, auto_truncate=True)

        self.assertEqual(resolved_1, resolved_2)

    def test_auto_truncate_collision_resistance(self):
        """Dos rutas profundas distintas deben generar nombres resueltos distintos."""
        path_a = (
            "root/carpeta_a/"
            + "/".join(["folder_" + str(i) for i in range(10)])
            + "/file.txt"
        )
        path_b = (
            "root/carpeta_b/"
            + "/".join(["folder_" + str(i) for i in range(10)])
            + "/file.txt"
        )

        resolved_a = TarEntryFactory.resolve_arcname(path_a, auto_truncate=True)
        resolved_b = TarEntryFactory.resolve_arcname(path_b, auto_truncate=True)

        self.assertNotEqual(resolved_a, resolved_b)

    def test_auto_truncate_directory_trailing_slash(self):
        """Las carpetas truncadas deben contemplar el trailing slash sin pasar de 255 bytes."""
        deep_dir = "DISCO C/" + "/".join(
            ["directorio_largo_" + str(i) for i in range(12)]
        )

        resolved = TarEntryFactory.resolve_arcname(
            deep_dir, auto_truncate=True, is_dir=True
        )
        valid, reason = validate_ustar_path(resolved, is_dir=True)
        self.assertTrue(valid, f"Directorio inválido: {reason}")

        # Comprobar con la cabecera real
        header = self._get_header(resolved, is_dir=True).build()
        self.assertEqual(len(header), 512)

        # En tarfile, debe leerse como directorio (type '5')
        full_tar = header + (b"\0" * 1024)
        with tarfile.open(fileobj=io.BytesIO(full_tar), mode="r") as tf:
            member = tf.next()
            self.assertIsNotNone(member)
            self.assertTrue(member.isdir())


if __name__ == "__main__":
    unittest.main()
