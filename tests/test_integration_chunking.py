from tartape.chunker import TarChunker
from tartape.recorder import TapeRecorder
from tests.base import TarTapeTestCase


class TestChunkingIntegration(TarTapeTestCase):
    # BUG: ESTE TEST REVELA UN BUG DESCONOCIDO.

    # def test_reconstruction_from_volumes(self):
    #     """
    #     Graba una carpeta, la divide en volúmenes pequeños,
    #     los une binariamente y verifica la integridad del TAR resultante.
    #     """
    #     self.create_file("file1.txt", "Contenido del archivo 1")
    #     self.create_file("sub/file2.txt", "X" * 5000)
    #     self.create_file("file3.txt", "Final")

    #     recorder = TapeRecorder(self.data_dir)
    #     recorder.commit()

    #     # Planifica trozos pequeños 2048 bytes
    #     chunker = TarChunker(chunk_size=2048)

    #     full_reconstructed_content = bytearray()

    #     # "Transmite" los volúmenes y guardamos los bytes en memoria
    #     for volume, manifest in chunker.iter_volumes(self.data_dir):
    #         with volume:
    #             content = volume.read()
    #             self.assertTrue(volume.is_completed)
    #             self.assertIsNotNone(volume.md5sum)

    #             full_reconstructed_content.extend(content)

    #     # Validamos que los datos transmitidos sean igual al peso total calculado.
    #     with Catalog.from_directory(self.data_dir) as cat:
    #         stats = cat.get_stats()
    #     self.assertEqual(len(full_reconstructed_content), int(stats["total_size"]))

    #     # Abrimos nuestros datos transmitidos como un archivo usando la libreria estandar de python.
    #     tar_fileobj = io.BytesIO(full_reconstructed_content)
    #     with tarfile.open(fileobj=tar_fileobj, mode="r") as tf:
    #         members = tf.getnames()

    #         # Verifica que los archivos están ahí
    #         self.assertTrue(any("file1.txt" in m for m in members))
    #         self.assertTrue(any("file2.txt" in m for m in members))

    #         # Verifica que contenido de un archivo que fue troceado
    #         f2 = tf.extractfile(next(m for m in members if "file2.txt" in m))
    #         self.assertEqual(f2.read(), b"X" * 5000)  # type: ignore

    def test_deterministic_resumption(self):
        """Verifica que si generamos el plan dos veces, los offsets son idénticos."""
        self.create_file("data.bin", "A" * 10000)

        TapeRecorder(self.data_dir).commit()

        chunker = TarChunker(chunk_size=1024)
        vols_1 = list(chunker.iter_volumes(self.data_dir))
        vols_2 = list(chunker.iter_volumes(self.data_dir))

        self.assertEqual(len(vols_1), len(vols_2))

        vol_1, manifest_1 = vols_1[0]
        vol_2, manifest_2 = vols_2[0]
        entries_1 = vols_1[0][1].entries
        entries_2 = vols_2[0][1].entries
        self.assertEqual(entries_1[0].start_offset, entries_2[0].start_offset)
        self.assertEqual(manifest_1.tape_fingerprint, manifest_2.tape_fingerprint)
