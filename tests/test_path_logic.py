import unittest

from tartape.header import TarHeader
from tartape.models import Track


class TestPathLogic(unittest.TestCase):
    """
    Validación exhaustiva de ADR-005 y compatibilidad USTAR.
    Centrado en la lógica de nombrado y límites de bytes.
    """

    def _get_header(self, path: str):
        # Helper para no repetir boilerplate
        track = Track(
            arc_path=path, size=0, mtime=0, mode=0o644,
            uid=0, gid=0, uname="root", gname="root"
        )
        return TarHeader(track)

    def test_component_limit_strict_adr005(self):
        """ADR-005: Ningún componente puede medir más de 100 bytes."""
        # Ruta corta (110 bytes) pero componente ilegal
        long_component = "a" * 101
        path = f"folder/{long_component}"

        with self.assertRaisesRegex(ValueError, "ADR-005 Violation"):
            self._get_header(path).build()

    def test_total_path_limit_ustar(self):
        """USTAR: La ruta total no puede exceder los 255 bytes."""
        # 3 carpetas de 80 = 240 + slashes... llegamos a 260.
        path = "a"*80 + "/" + "b"*80 + "/" + "c"*80 + "/" + "d"*20
        self.assertGreater(len(path.encode()), 255)

        with self.assertRaisesRegex(ValueError, "Path is too long"):
            self._get_header(path).build()

    def test_the_dead_zone_case(self):
        """
        CASO BORDE: Ruta legal en TarTape pero indivisible en USTAR.
        Demuestra por qué el límite de 100 bytes/componente no garantiza que exista split.
        """
        # A(90) / B(90) / C(70) = 252 bytes totales.
        # Todos los componentes < 100.
        path = ("a" * 90) + "/" + ("b" * 90) + "/" + ("c" * 70)

        with self.assertRaisesRegex(ValueError, "cannot be split into USTAR prefix/name"):
            self._get_header(path).build()

    def test_perfect_ustar_split_with_adr005_compliance(self):
        """
        Limite USTAR y ADDR-005:

        - USTAR ruta con 255 bytes y split legal
        - Limite ADR-005: cumpliendo que ningún componente exceda los 100 bytes.

        """
        c1 = "a" * 100
        c2 = "b" * 54
        name = "c" * 99
        # Total: 100 + 1 (/) + 54 + 1 (/) + 99 = 255 bytes
        path = f"{c1}/{c2}/{name}"

        self.assertEqual(len(path.encode('utf-8')), 255)

        # Esto NO debe lanzar ValueError
        header_bytes = self._get_header(path).build()

        # Verificamos que el split se hizo en el último '/'
        # Name: "c"*99
        name_in_header = header_bytes[0:100].decode().rstrip('\0')
        self.assertEqual(name_in_header, name)

        # Prefix: "a"*100 + "/" + "b"*54 (155 bytes)
        prefix_in_header = header_bytes[345:500].decode().rstrip('\0')
        self.assertEqual(prefix_in_header, f"{c1}/{c2}")
        self.assertEqual(len(prefix_in_header.encode('utf-8')), 155)

    def test_unicode_byte_counting(self):
        """Garantiza que los límites se apliquen a BYTES UTF-8, no a caracteres."""

        # El emoji 🔥 ocupa 4 bytes.
        # 25 emojis = 100 bytes exactos.
        safe_component = "🔥" * 25
        self.assertEqual(len(safe_component.encode('utf-8')), 100)

        # Debería pasar
        self._get_header(safe_component).build()

        # 26 emojis = 104 bytes.
        # Debería fallar por ADR-005
        unsafe_component = "🔥" * 26
        with self.assertRaises(ValueError):
            self._get_header(unsafe_component).build()

    def test_directory_trailing_slash(self):
        """Garantiza que los directorios siempre terminen en / para cumplimiento USTAR."""
        track = Track(
            arc_path="my_folder", is_dir=True, # Sin slash manual
            size=0, mtime=0, mode=0o755, uid=0, gid=0, uname="r", gname="r"
        )
        header_bytes = TarHeader(track).build()

        # El nombre en el header (offset 0) debe tener el slash
        name_in_header = header_bytes[0:100].decode().rstrip('\0')
        self.assertEqual(name_in_header, "my_folder/")

if __name__ == "__main__":
    unittest.main()
