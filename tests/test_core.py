import io
import tarfile
import tempfile
import unittest
from pathlib import Path

from tartape import TarEntryFactory, TarTape
from tartape.core import TarStreamGenerator
from tartape.enums import TarEventType


class TestTarIntegrity(unittest.TestCase):
    """
    Valida la robustez del motor ante cambios en el sistema de archivos
    durante la lectura.
    """

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp_dir.name)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_file_grows_during_streaming(self):
        """
        Simula que un archivo cambia de tamaño DESPUÉS de ser analizado
        pero ANTES de terminar de ser leído. Debe lanzar RuntimeError.
        """
        file_path = self.root / "crecimiento.txt"

        with open(file_path, "w") as f:
            f.write("HELLO")  # 5 bytes

        entry = TarEntryFactory.create(file_path, "crecimiento.txt")
        assert entry is not None, "Error al crear la entrada"
        self.assertEqual(entry.size, 5)

        # Modificar el archivo en disco
        with open(file_path, "a") as f:
            f.write("WORLD")

        generator = TarStreamGenerator([entry])
        with self.assertRaises(RuntimeError) as cm:
            for _ in generator.stream():
                pass

        error_msg = str(cm.exception)
        is_integrity_error = (
            "File modified (mtime)" in error_msg or "File size changed" in error_msg
        )
        self.assertTrue(is_integrity_error, f"Mensaje de error inesperado: {error_msg}")


class TestTarOutputCompatibility(unittest.TestCase):
    """
    Prueba de integración: Generamos un TAR en memoria y usamos
    la librería estándar 'tarfile' de Python para intentar leerlo.
    Si 'tarfile' lo lee, es compatible.
    """

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp_dir.name)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_compatibility_with_standard_library(self):
        (self.root / "carpeta").mkdir()
        (self.root / "carpeta" / "hola.txt").write_text("Contenido del archivo")

        tape = TarTape()
        tape.add_folder(self.root / "carpeta")

        # Strimeamos y guardamos los datos en memoria (buffer)
        buffer = io.BytesIO()
        for event in tape.stream():
            if event.type == TarEventType.FILE_DATA:
                buffer.write(event.data)

        buffer.seek(0)

        # Validamos con la libreria estandar de python tarfile
        with tarfile.open(fileobj=buffer, mode="r:") as tf:
            names = tf.getnames()

            self.assertIn("carpeta", names)
            self.assertIn("carpeta/hola.txt", names)

            member = tf.getmember("carpeta/hola.txt")
            self.assertEqual(member.size, len("Contenido del archivo"))

            extracted_f = tf.extractfile(member)
            assert extracted_f is not None, "Error al extraer el archivo"
            content = extracted_f.read().decode("utf-8")
            self.assertEqual(content, "Contenido del archivo")

    def test_identity_anonymization(self):

        test_file = self.root / "secret.txt"
        test_file.touch()

        # Por defecto debe anonimizar
        tape = TarTape()
        tape.add_file(test_file, arcname="secret.txt")

        event = next(tape.stream())
        assert event.type == TarEventType.FILE_START
        entry = event.entry

        self.assertEqual(entry.uid, 0)
        self.assertEqual(entry.uname, "root")
        self.assertEqual(entry.gid, 0)
        self.assertEqual(entry.gname, "root")


class TestPathSanitization(unittest.TestCase):
    """
    Intenta 'romper' el motor inyectando rutas estilo Windows (con backslashes).
    El motor DEBE normalizarlas a estilo UNIX para cumplir el estándar.
    """

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp_dir.name)

        self.dummy_file = self.root / "dummy.txt"
        self.dummy_file.touch()

    def tearDown(self):
        self.tmp_dir.cleanup()

    def _get_tar_names(self, tape: TarTape) -> list[str]:
        """Helper para generar el TAR en memoria y devolver los nombres internos."""
        buffer = io.BytesIO()
        for event in tape.stream():
            if event.type == TarEventType.FILE_DATA:
                buffer.write(event.data)
        buffer.seek(0)

        with tarfile.open(fileobj=buffer, mode="r:") as tf:
            return tf.getnames()

    def test_manual_injection_of_windows_path(self):
        """
        Inyecta manualmente una ruta con backslashes en arcname.

        ESPERADO: El sistema debería reemplazar '\\' por '/' automáticamente.
        """
        tape = TarTape()
        dirty_path = "carpeta\\subcarpeta\\archivo_win.txt"

        tape.add_file(self.dummy_file, arcname=dirty_path)

        names = self._get_tar_names(tape)

        # Verificamos que no haya backslashes
        self.assertNotIn(dirty_path, names, "¡FALLO! El TAR contiene backslashes.")

        # Verificamos que se haya normalizado
        clean_path = "carpeta/subcarpeta/archivo_win.txt"
        self.assertIn(clean_path, names, "La ruta no fue normalizada a UNIX format.")

    def test_mixed_separators(self):
        """
        Inyecta una mezcla horrible de separadores.
        """
        tape = TarTape()
        mixed_path = "data/win\\logs/error.log"

        tape.add_file(self.dummy_file, arcname=mixed_path)

        names = self._get_tar_names(tape)

        expected = "data/win/logs/error.log"
        self.assertIn(expected, names)


if __name__ == "__main__":
    unittest.main()
