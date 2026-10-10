import tempfile
import unittest
from pathlib import Path

import tartape
from tartape.catalog import Catalog
from tartape.constants import TAR_BLOCK_SIZE
from tartape.exceptions import LayoutNotFoundError
from tartape.units import format_size, parse_size


class TestUnitsParsing(unittest.TestCase):
    """Pruebas unitarias para la función parse_size."""

    def test_parse_valid_integers(self):
        """Valida enteros válidos alineados a 512 bytes."""
        self.assertEqual(parse_size(512), 512)
        self.assertEqual(parse_size(1024), 1024)
        self.assertEqual(parse_size(1024 * 1024 * 100), 104857600)

    def test_parse_invalid_integers(self):
        """Rechaza enteros menores o iguales a cero o no alineados a 512."""
        with self.assertRaises(ValueError):
            parse_size(0)

        with self.assertRaises(ValueError):
            parse_size(-512)

        with self.assertRaises(ValueError) as ctx:
            parse_size(500)
        self.assertIn("multiple of TAR block size", str(ctx.exception))

    def test_parse_valid_strings_case_insensitive(self):
        """Valida strings con varias unidades y tolerancia a mayúsculas/minúsculas."""
        self.assertEqual(parse_size("512B"), 512)
        self.assertEqual(parse_size("512b"), 512)
        self.assertEqual(parse_size("1KB"), 1024)
        self.assertEqual(parse_size("1kb"), 1024)
        self.assertEqual(parse_size("1K"), 1024)
        self.assertEqual(parse_size("1k"), 1024)
        self.assertEqual(parse_size("100MB"), 100 * 1024 * 1024)
        self.assertEqual(parse_size("100mb"), 100 * 1024 * 1024)
        self.assertEqual(parse_size("1GB"), 1024 * 1024 * 1024)
        self.assertEqual(parse_size("1gb"), 1024 * 1024 * 1024)
        self.assertEqual(parse_size("1G"), 1024 * 1024 * 1024)
        self.assertEqual(parse_size("1TB"), 1024**4)
        self.assertEqual(parse_size("1tb"), 1024**4)

    def test_parse_clean_decimals(self):
        """Valida que los decimales alineados a 512 sean interpretados correctamente."""
        # 1.5 GB = 1.5 * 1024^3 = 1,610,612,736 bytes (múltiplo de 512)
        expected_1_5_gb = int(1.5 * 1024 * 1024 * 1024)
        self.assertEqual(parse_size("1.5GB"), expected_1_5_gb)
        self.assertEqual(parse_size("1.5gb"), expected_1_5_gb)

        # 2.5 MB = 2.5 * 1024^2 = 2,621,440 bytes (múltiplo de 512)
        expected_2_5_mb = int(2.5 * 1024 * 1024)
        self.assertEqual(parse_size("2.5MB"), expected_2_5_mb)

    def test_parse_spaces_raises_educational_error(self):
        """Verifica que los espacios sean rechazados con el mensaje didáctico y sugerencia."""
        with self.assertRaises(ValueError) as ctx1:
            parse_size("1 GB")
        self.assertIn(
            "Spaces are not allowed in TarTape size strings", str(ctx1.exception)
        )
        self.assertIn("Did you mean '1GB'?", str(ctx1.exception))

        with self.assertRaises(ValueError) as ctx2:
            parse_size(" 500MB ")
        self.assertIn("Did you mean '500MB'?", str(ctx2.exception))

        with self.assertRaises(ValueError) as ctx3:
            parse_size("1.5  gb")
        self.assertIn("Did you mean '1.5gb'?", str(ctx3.exception))

    def test_parse_invalid_types_and_formats(self):
        """Verifica tipos no permitidos o unidades desconocidas."""
        with self.assertRaises(TypeError):
            parse_size(None)  # type: ignore

        with self.assertRaises(TypeError):
            parse_size([1024])  # type: ignore

        with self.assertRaises(ValueError) as ctx:
            parse_size("100FOO")
        self.assertIn("Unknown size unit 'FOO'", str(ctx.exception))

        with self.assertRaises(ValueError):
            parse_size("invalid")

    def test_parse_tar_block_unaligned_fails(self):
        """Rechaza tamaños que no sean divisibles por 512 bytes."""
        with self.assertRaises(ValueError) as ctx:
            parse_size("1000B")
        self.assertIn("multiple of TAR block size", str(ctx.exception))


class TestUnitsFormatting(unittest.TestCase):
    """Pruebas unitarias para la función format_size."""

    def test_format_exact_units(self):
        self.assertEqual(format_size(512), "512B")
        self.assertEqual(format_size(1024), "1KB")
        self.assertEqual(format_size(1024 * 1024), "1MB")
        self.assertEqual(format_size(100 * 1024 * 1024), "100MB")
        self.assertEqual(format_size(1024 * 1024 * 1024), "1GB")
        self.assertEqual(format_size(2 * 1024 * 1024 * 1024), "2GB")
        self.assertEqual(format_size(1024**4), "1TB")

    def test_format_clean_decimals(self):
        # 1.5 GB
        self.assertEqual(format_size(int(1.5 * 1024**3)), "1.5GB")
        # 2.5 MB
        self.assertEqual(format_size(int(2.5 * 1024**2)), "2.5MB")

    def test_format_edge_values(self):
        self.assertEqual(format_size(0), "0B")
        self.assertEqual(format_size(-100), "-100B")


class TestUnitsLayoutIntegration(unittest.TestCase):
    """Pruebas de integración con Catalog y Tape utilizando tamaños humanos."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root_path = Path(self.temp_dir.name) / "test_data"
        self.root_path.mkdir()

        # Creamos un archivo de prueba
        (self.root_path / "hello.txt").write_text("Hello TarTape with human sizes!")
        self.tape = tartape.record(self.root_path)

    def tearDown(self):
        self.tape.close()
        self.temp_dir.cleanup()

    def test_iter_volumes_registers_canonical_human_tag(self):
        """Verifica que iter_volumes con '100MB' registre un layout con tag '100MB'."""
        volumes = list(self.tape.iter_volumes(size="100MB"))
        self.assertGreater(len(volumes), 0)

        # El tag asignado al layout debe ser exactamente '100MB'
        self.assertEqual(volumes[0].layout_tag, "100MB")

        cat = self.tape._get_catalog()
        # Debe poder consultarse por coincidencia exacta de tag
        layout = cat.get_layout("100MB")
        self.assertEqual(layout.tag, "100MB")
        self.assertEqual(layout.volume_size, 100 * 1024 * 1024)

    def test_get_layout_case_and_size_resolutions(self):
        """Verifica que get_layout resuelva por string insensible a mayúsculas y por entero."""
        list(
            self.tape.iter_volumes(size="50MB")
        )  # Consumir el generador para registrar el layout
        cat = self.tape._get_catalog()

        # Búsqueda por minúsculas '50mb'
        layout_lower = cat.get_layout("50mb")
        self.assertEqual(layout_lower.tag, "50MB")

        # Búsqueda por entero exacto en bytes
        layout_int = cat.get_layout(50 * 1024 * 1024)
        self.assertEqual(layout_int.tag, "50MB")

        # Layout inexistente debe lanzar LayoutNotFoundError
        with self.assertRaises(LayoutNotFoundError):
            cat.get_layout("999GB")

    def test_iter_volumes_with_integer_generates_canonical_tag(self):
        """Verifica que pasar enteros (ej. 1024^3) genere automáticamente el tag canónico '1GB'."""
        volumes = list(self.tape.iter_volumes(size=1024 * 1024 * 1024))
        self.assertEqual(volumes[0].layout_tag, "1GB")

        cat = self.tape._get_catalog()
        layout = cat.get_layout("1GB")
        self.assertEqual(layout.volume_size, 1024 * 1024 * 1024)

    def test_locate_accepts_human_size(self):
        """Verifica que catalog.locate funcione pasando un tamaño legible como '100MB'."""
        list(self.tape.iter_volumes(size="100MB"))
        cat = self.tape._get_catalog()

        gps = cat.locate(f"{self.root_path.name}/hello.txt", tag_or_size="100mb")
        self.assertEqual(gps.arc_path, f"{self.root_path.name}/hello.txt")
        self.assertGreater(len(gps.fragments), 0)


if __name__ == "__main__":
    unittest.main()
