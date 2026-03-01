import io
import os
import tarfile
import time

from tartape.models import Track
from tartape.player import TapePlayer
from tartape.recorder import TapeRecorder
from tartape.schemas import TarFileDataEvent
from tartape.tape import Tape
from tests.base import TarTapeTestCase


class TestStreamingEngine(TarTapeTestCase):

    def test_byte_perfect_resume(self):
        """
        Verifica que reanudar en un offset arbitrario produce bytes idénticos
        al sufijo del stream original (Garantía de determinismo ADR-001).
        """

        # Prepara un set de datos variado: archivos pequeños, grandes y carpetas
        self.create_file("a_small.txt", "Contenido pequeño")
        self.create_file("b_large.bin", "X" * 10000)
        self.create_file("sub/c_nested.txt", "Archivo en subcarpeta")

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()

        with Tape.discover(self.data_dir) as tape:
            player = TapePlayer(tape, self.data_dir)

            full_buffer = io.BytesIO()
            for event in player.play(start_offset=0, fast_verify=False):
                if isinstance(event, TarFileDataEvent):
                    full_buffer.write(event.data)

            full_bytes = full_buffer.getvalue()
            total_len = len(full_bytes)

            # Elegimos un offset de reanudación "difícil"
            # Buscamos el track del archivo grande para reanudar EN MEDIO de sus datos
            track_large = Track.get(Track.arc_path.contains("b_large.bin"))  # type: ignore

            # Offset = Inicio del archivo + Header (512) + 123 bytes de sus datos
            resume_offset = track_large.start_offset + 512 + 123

            # Asegurarnos de que no estamos alineados a 512 para forzar la lógica de "windowing"
            self.assertNotEqual(resume_offset % 512, 0)

            # Genera el stream RESUMIDO desde ese punto
            resumed_buffer = io.BytesIO()
            for event in player.play(start_offset=resume_offset, fast_verify=False):
                if isinstance(event, TarFileDataEvent):
                    resumed_buffer.write(event.data)

            resumed_bytes = resumed_buffer.getvalue()

            # --- VALIDACIÓN MAESTRA ---

            # El stream reanudado debe ser EXACTAMENTE igual al recorte del original
            expected_suffix = full_bytes[resume_offset:]

            self.assertEqual(
                len(resumed_bytes),
                len(expected_suffix),
                "El tamaño del stream reanudado no coincide con el remanente esperado",
            )

            self.assertEqual(
                resumed_bytes,
                expected_suffix,
                "Los bytes del stream reanudado no son idénticos al stream original (Bit-Perfect Fail)",
            )

            # Verififacion extra: si intentamos reanudar más allá del tamaño total, debería fallar.
            with self.assertRaises(ValueError):
                list(player.play(start_offset=total_len + 100))

    def test_player_tar_block_padding_alignment(self):
        """Verifica que el padding rellene hasta el múltiplo de 512 (ADR-002/004)."""

        # Creamos un archivo de 1 solo byte.
        # 512 (Header) + 1 (Data) + 511 (Padding) = 1024 bytes.
        filename = "single_byte.txt"
        content = b"A"
        self.create_file(filename, content.decode())

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()

        with Tape.discover(self.data_dir) as tape:

            # --- Solo calculos de offsets y tamaños, sin leer el stream aún ---
            track = next(t for t in tape.get_tracks() if filename in t.arc_path)
            self.assertEqual(track.size, 1)
            self.assertEqual(track.padding_size, 511)
            self.assertEqual(track.total_block_size, 1024)

            player = TapePlayer(tape, self.data_dir)
            full_stream = io.BytesIO()
            for event in player.play(fast_verify=False):
                if isinstance(event, TarFileDataEvent):
                    full_stream.write(event.data)

            stream_bytes = full_stream.getvalue()

            # --- Verificaciones reales de contenido (bytes) ---

            # Localizamos la sección del archivo en el stream usando los offsets de la DB
            # [Header(512)] + [Data(1)] + [Padding(511)]
            file_section = stream_bytes[track.start_offset : track.end_offset]
            self.assertEqual(len(file_section), 1024)

            header = file_section[:512]
            data_byte = file_section[512:513]
            padding = file_section[513:]

            self.assertEqual(
                data_byte, content, "El contenido del archivo es incorrecto"
            )
            self.assertEqual(len(padding), 511, "El tamaño del padding es incorrecto")
            self.assertEqual(
                padding, b"\0" * 511, "El padding debe contener solo bytes nulos"
            )

            # El siguiente byte en el stream global (si existe otro archivo o el footer)
            # debe estar alineado a 512
            self.assertEqual(track.end_offset % 512, 0)

    def test_resume_at_specific_file_offset(self):
        self.create_file("a.txt", "AAAAA")
        self.create_file("b.txt", "BBBBB")

        TapeRecorder(self.data_dir).commit()

        with Tape.discover(self.data_dir) as tape:
            # Buscamos el offset exacto donde empieza b.txt en el stream
            track_b = [t for t in tape.get_tracks() if "b.txt" in t.arc_path][0]
            start_offset = track_b.start_offset

            player = TapePlayer(tape, self.data_dir)

            buffer = io.BytesIO()
            for event in player.play(start_offset=start_offset):
                if isinstance(event, TarFileDataEvent):
                    buffer.write(event.data)

            buffer.seek(0)
            with tarfile.open(fileobj=buffer, mode="r:") as tf:
                names = tf.getnames()

                # Solo debería existir b.txt, nada de a.txt
                self.assertEqual(len(names), 1)
                self.assertTrue(names[0].endswith("b.txt"))

    def test_player_spot_check_detection(self):
        """Verifica que el muestreo aleatorio (Spot Check) detecte mutaciones."""

        for i in range(20):
            self.create_file(f"file_{i}.txt", "content")

        TapeRecorder(self.data_dir).commit()

        # Corrompemos un archivo específico
        corrupt_file = self.data_dir / "file_10.txt"
        os.utime(corrupt_file, (time.time() + 1000, time.time() + 1000))

        with Tape.discover(self.data_dir) as tape:
            player = TapePlayer(tape, self.data_dir)

            found_error = False
            for _ in range(3):
                try:
                    # El spot_check se ejecuta dentro de play() si fast_verify=True
                    list(player.play(fast_verify=True))
                except RuntimeError:
                    found_error = True
                    break

            self.assertTrue(
                found_error, "El spot check no detectó la mutación del archivo"
            )
