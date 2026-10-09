import hashlib
import os
import tempfile
import unittest
from pathlib import Path

import tartape
from tartape.chunker import TarChunker
from tartape.constants import TAR_BLOCK_SIZE
from tartape.schemas import FileSlice


def calculate_md5(data: bytes) -> str:
    """Helper to calculate MD5 of a byte array."""
    return hashlib.md5(data).hexdigest()


class TestSurgicalSlices(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root_path = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_chunk_size_alignment_validation(self):
        """Verifica que TarChunker rechace tamaños de volumen no alineados a 512 bytes."""
        # Multiples of 512 must succeed
        chunker = TarChunker(chunk_size=512)
        self.assertEqual(chunker.chunk_size, 512)

        chunker_large = TarChunker(chunk_size=1024 * 1024)
        self.assertEqual(chunker_large.chunk_size, 1024 * 1024)

        # Non-multiples of 512 must fail
        with self.assertRaises(ValueError):
            TarChunker(chunk_size=513)

        with self.assertRaises(ValueError):
            TarChunker(chunk_size=1000)

        with self.assertRaises(ValueError):
            TarChunker(chunk_size=0)

    def test_single_volume_complete_file_slice(self):
        """Verifica que un archivo dentro de un solo volumen tenga el offset correcto y longitud exacta."""
        test_file = self.root_path / "single.bin"
        file_content = os.urandom(2500)  # 2500 bytes
        test_file.write_bytes(file_content)

        tape = tartape.create(self.root_path)

        # Chunk size grande para que todo quepa en el volumen 0
        chunk_size = 64 * 1024
        arc_path = f"{self.root_path.name}/single.bin"
        slices = tape.get_file_slices(arc_path, chunk_size=chunk_size)

        self.assertEqual(len(slices), 1)
        s = slices[0]

        self.assertEqual(s.volume_index, 0)
        # Offset esperado:
        # 0..512: Header de la carpeta raíz
        # 512..1024: Header de single.bin
        # 1024+: Datos reales de single.bin
        expected_offset = 2 * TAR_BLOCK_SIZE  # 1024 bytes
        self.assertEqual(s.volume_offset, expected_offset)
        self.assertEqual(s.volume_length, 2500)
        self.assertEqual(s.source_offset, 0)

    def test_edge_cases_empty_file_and_directories(self):
        """Verifica que archivos de 0 bytes y carpetas no produzcan FileSlice (slice is None)."""
        empty_file = self.root_path / "empty.txt"
        empty_file.write_bytes(b"")

        sub_dir = self.root_path / "subfolder"
        sub_dir.mkdir()

        tape = tartape.create(self.root_path)
        chunk_size = 10 * 1024

        empty_arc_path = f"{self.root_path.name}/empty.txt"
        dir_arc_path = f"{self.root_path.name}/subfolder"

        # Archivo vacío no debe tener slices que descargar
        empty_slices = tape.get_file_slices(empty_arc_path, chunk_size=chunk_size)
        self.assertEqual(len(empty_slices), 0)

        # Directorio no debe tener slices que descargar
        dir_slices = tape.get_file_slices(dir_arc_path, chunk_size=chunk_size)
        self.assertEqual(len(dir_slices), 0)

        # En el mapa global de slices solo deben figurar archivos con contenido
        slices_map = tape.get_file_slices_map(chunk_size=chunk_size)
        self.assertNotIn(empty_arc_path, slices_map)
        self.assertNotIn(dir_arc_path, slices_map)

    def test_multi_volume_file_straddling(self):
        """
        Verifica un archivo fragmentado en múltiples volúmenes pequeños.
        Comprueba la continuidad matemática de source_offset y volume_offset.
        """
        file_size = 15 * 1024  # 15 KB
        content = os.urandom(file_size)
        test_file = self.root_path / "large.bin"
        test_file.write_bytes(content)

        tape = tartape.create(self.root_path)

        # Volúmenes pequeños de 4 KB (múltiplo de 512)
        chunk_size = 4 * 1024
        arc_path = f"{self.root_path.name}/large.bin"
        slices = tape.get_file_slices(arc_path, chunk_size=chunk_size)

        # Debe repartirse en múltiples volúmenes
        self.assertGreater(len(slices), 1)

        # 1. La suma de todas las longitudes debe ser exactamente el tamaño del archivo
        total_extracted_length = sum(s.volume_length for s in slices)
        self.assertEqual(total_extracted_length, file_size)

        # 2. Continuidad estricta de source_offset
        expected_source_offset = 0
        for idx, s in enumerate(slices):
            self.assertEqual(s.source_offset, expected_source_offset)
            expected_source_offset += s.volume_length

            if idx == 0:
                # El primer volumen contiene el header de la carpeta raíz y el de este archivo
                # Pero en todos los casos s.volume_offset >= 512
                self.assertGreaterEqual(s.volume_offset, TAR_BLOCK_SIZE)
            else:
                # En volúmenes intermedios/finales (BODY/TAIL) los datos empiezan en el byte 0
                self.assertEqual(s.volume_offset, 0)

    def test_end_to_end_bit_for_bit_surgical_reconstruction(self):
        """
        PRUEBA DE FUEGO (End-to-End):
        Genera físicamente los volúmenes en disco.
        Luego reconstruye el archivo quirúrgicamente usando solo FileSlice (seek + read).
        Verifica que el archivo resultante sea bit a bit idéntico al original mediante MD5.
        """
        # Creamos varios archivos con diferentes tamaños
        file1_data = os.urandom(12345)
        file2_data = os.urandom(40960)
        file3_data = os.urandom(800)

        (self.root_path / "data1.bin").write_bytes(file1_data)
        (self.root_path / "data2.bin").write_bytes(file2_data)
        (self.root_path / "data3.bin").write_bytes(file3_data)

        expected_md5_map = {
            f"{self.root_path.name}/data1.bin": (
                calculate_md5(file1_data),
                len(file1_data),
            ),
            f"{self.root_path.name}/data2.bin": (
                calculate_md5(file2_data),
                len(file2_data),
            ),
            f"{self.root_path.name}/data3.bin": (
                calculate_md5(file3_data),
                len(file3_data),
            ),
        }

        tape = tartape.create(self.root_path)

        # Volúmenes pequeños de 8 KB para forzar cortes arbitrarios
        chunk_size = 8 * 1024
        chunker = TarChunker(chunk_size=chunk_size)

        # 1. Escribimos los volúmenes reales a disco
        volumes_dir = self.root_path / "_volumes"
        volumes_dir.mkdir()
        volume_files = []

        for vol, manifest in chunker.iter_volumes(self.root_path):
            vol_path = volumes_dir / vol.name
            volume_files.append(vol_path)
            with vol, open(vol_path, "wb") as f_out:
                while chunk := vol.read(64 * 1024):
                    f_out.write(chunk)

        # 2. Obtenemos el mapa completo de slices quirúrgicos
        slices_map = tape.get_file_slices_map(chunk_size=chunk_size)

        # 3. Reconstruimos quirúrgicamente cada archivo SIN usar tarfile ni headers
        for arc_path, (expected_md5, original_size) in expected_md5_map.items():
            file_slices = slices_map[arc_path]
            reconstructed_buffer = bytearray(original_size)

            for s in file_slices:
                vol_path = volume_files[s.volume_index]

                with open(vol_path, "rb") as vf:
                    # SALTO QUIRÚRGICO AL VOLUMEN
                    vf.seek(s.volume_offset)
                    chunk = vf.read(s.volume_length)
                    self.assertEqual(len(chunk), s.volume_length)

                    # ESCRITURA EN LA COORDENADA EXACTA DEL ARCHIVO
                    reconstructed_buffer[
                        s.source_offset : s.source_offset + s.volume_length
                    ] = chunk

            # 4. Verificación de Integridad Bit a Bit
            reconstructed_md5 = calculate_md5(bytes(reconstructed_buffer))
            self.assertEqual(
                reconstructed_md5,
                expected_md5,
                f"Fallo de integridad quirúrgica en: {arc_path}",
            )


if __name__ == "__main__":
    unittest.main()
