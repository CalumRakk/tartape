import io
import tarfile

from tartape.exceptions import PathConstraintReportError
from tartape.recorder import TapeRecorder
from tartape.tape import Tape
from tests.base import TarTapeTestCase


class TestRecorder(TarTapeTestCase):
    def test_deterministic_ordering_adr001(self):
        """Garantiza que el orden en la DB sea siempre alfabético."""
        self.create_file("z.txt")
        self.create_file("a.txt")
        self.create_file("m.txt")

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()

        tape = Tape(self.data_dir)
        tracks = [t.arc_path for t in tape.get_tracks() if t.is_file]
        self.assertEqual(tracks, sorted(tracks))

    def test_exclusion_logic(self):
        """Verifica que los archivos excluidos no lleguen a la cinta."""
        self.create_file("keep.txt")
        self.create_file("ignore.log")

        recorder = TapeRecorder(self.data_dir, exclude="*.log")
        recorder.commit()

        tape = Tape(self.data_dir)
        paths = [t.arc_path for t in tape.get_tracks()]
        self.assertTrue(any("keep.txt" in p for p in paths))
        self.assertFalse(any("ignore.log" in p for p in paths))

    def test_deep_paths_without_auto_truncate_aborts_with_helpful_report(self):
        """
        Sin auto_truncate, una ruta profunda (>255 bytes) debe abortar la grabación
        mostrando la razón específica y sugiriendo 'auto_truncate=True'.
        """
        deep_folder_chain = "/".join(
            [f"sub_nivel_con_nombre_largo_{i}" for i in range(10)]
        )
        self.create_file(f"{deep_folder_chain}/archivo_profundo.txt", "contenido")

        recorder = TapeRecorder(self.data_dir, auto_truncate=False)

        with self.assertRaises(PathConstraintReportError) as ctx:
            recorder.commit()

        error_msg = str(ctx.exception)
        self.assertIn("exceeds USTAR 255 byte limit", error_msg)
        self.assertIn("auto_truncate=True", error_msg)

    def test_deep_paths_with_auto_truncate_succeeds_and_extracts(self):
        """
        Con auto_truncate=True, el TapeRecorder debe procesar exitosamente
        rutas de más de 255 bytes y el archivo TAR generado debe ser 100%
        legible y extraíble por la librería estándar tarfile.
        """
        deep_folder_chain = "/".join([f"nivel_{i}_con_nombre_largo" for i in range(12)])
        expected_content = "Contenido ultra secreto en ruta profunda"
        self.create_file(f"{deep_folder_chain}/mi_archivo.txt", expected_content)

        recorder = TapeRecorder(self.data_dir, auto_truncate=True)
        fingerprint = recorder.commit()
        self.assertIsNotNone(fingerprint)

        tape = Tape(self.data_dir)
        tar_buffer = io.BytesIO()
        for chunk in tape.play(fast_verify=False):
            tar_buffer.write(chunk)

        tar_buffer.seek(0)
        with tarfile.open(fileobj=tar_buffer, mode="r") as tf:
            members = tf.getmembers()
            self.assertGreater(len(members), 1)

            file_member = next(
                (m for m in members if m.name.endswith("mi_archivo.txt")), None
            )
            self.assertIsNotNone(file_member, "No se encontró el archivo en el TAR")
            self.assertLessEqual(len(file_member.name.encode("utf-8")), 255)

            f = tf.extractfile(file_member)
            self.assertIsNotNone(f)
            self.assertEqual(f.read().decode("utf-8"), expected_content)
