import io
import tarfile

import tartape
from tartape.chunker import TarChunker
from tartape.player import TapePlayer
from tartape.recorder import TapeRecorder
from tests.base import TarTapeTestCase


class TestChunkingIntegration(TarTapeTestCase):
    def test_reconstruction_from_volumes(self):
        """
        Graba una carpeta, la divide en volúmenes pequeños,
        los une binariamente y verifica la integridad del TAR resultante.
        """
        self.create_file("file1.txt", "Contenido del archivo 1")
        self.create_file("sub/file2.txt", "X" * 5000)
        self.create_file("file3.txt", "Final")

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()

        cat = tartape.get_catalog(self.data_dir)
        with cat:
            player = TapePlayer(self.data_dir)
            # Planifica trozos pequeños 2048 bytes
            chunker = TarChunker(chunk_size=2048)

            full_reconstructed_content = bytearray()

            # "Transmite" los volúmenes y guardamos los bytes en memoria
            for volume, manifest in chunker.iter_volumes(player):
                with volume:
                    content = volume.read()
                    self.assertTrue(volume.is_completed)
                    self.assertIsNotNone(volume.md5sum)

                    full_reconstructed_content.extend(content)

            # Validamos que los datos transmitidos sean igual al peso total calculado.
            data = cat.get_metadata_snapshot()
            self.assertEqual(len(full_reconstructed_content), int(data["total_size"]))

            # Abrimos nuestros datos transmitidos como un archivo usando la libreria estandar de python.
            tar_fileobj = io.BytesIO(full_reconstructed_content)
            with tarfile.open(fileobj=tar_fileobj, mode="r") as tf:
                members = tf.getnames()

                # Verifica que los archivos están ahí
                self.assertTrue(any("file1.txt" in m for m in members))
                self.assertTrue(any("file2.txt" in m for m in members))

                # Verifica que contenido de un archivo que fue troceado
                f2 = tf.extractfile(next(m for m in members if "file2.txt" in m))
                self.assertEqual(f2.read(), b"X" * 5000)  # type: ignore

    def test_deterministic_resumption(self):
        """Verifica que si generamos el plan dos veces, los offsets son idénticos."""
        self.create_file("data.bin", "A" * 10000)
        TapeRecorder(self.data_dir).commit()

        cat = tartape.get_catalog(self.data_dir)
        with cat:
            chunker = TarChunker(chunk_size=1024)
            plan1 = chunker.generate_plan()
            plan2 = chunker.generate_plan()

            self.assertEqual(len(plan1), len(plan2))
            for i in range(len(plan1)):
                self.assertEqual(plan1[i].start_offset, plan2[i].start_offset)
                self.assertEqual(plan1[i].tape_fingerprint, plan2[i].tape_fingerprint)
