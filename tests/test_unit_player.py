import io
import os
import tarfile
import time

from tartape.exceptions import TarIntegrityError
from tartape.models import Track
from tartape.recorder import TapeRecorder
from tartape.tape import Tape
from tests.base import TarTapeTestCase


class TestStreamingEngine(TarTapeTestCase):
    def test_byte_perfect_resume(self):
        """
        Verifica que reanudar en un offset arbitrario produce bytes idénticos
        al sufijo del stream original (Garantía de determinismo ADR-001).
        """
        self.create_file("a_small.txt", "Contenido pequeño")
        self.create_file("b_large.bin", "X" * 10000)
        self.create_file("sub/c_nested.txt", "Archivo en subcarpeta")

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()

        tape = Tape(self.data_dir)
        full_buffer = io.BytesIO()
        for chunk in tape.play(start_offset=0, fast_verify=False):
            full_buffer.write(chunk)

        full_bytes = full_buffer.getvalue()
        total_len = len(full_bytes)

        # Buscamos el track del archivo grande para reanudar EN MEDIO de sus datos
        track_large = Track.get(Track.arc_path.contains("b_large.bin"))  # type: ignore
        resume_offset = track_large.start_offset + 512 + 123
        self.assertNotEqual(resume_offset % 512, 0)

        # Genera el stream RESUMIDO desde ese punto
        resumed_buffer = io.BytesIO()
        for chunk in tape.play(start_offset=resume_offset, fast_verify=False):
            resumed_buffer.write(chunk)

        resumed_bytes = resumed_buffer.getvalue()
        expected_suffix = full_bytes[resume_offset:]

        self.assertEqual(len(resumed_bytes), len(expected_suffix))
        self.assertEqual(resumed_bytes, expected_suffix)

        # Verificación extra: reanudar más allá del tamaño total debe fallar
        with self.assertRaises(ValueError):
            list(tape.play(start_offset=total_len + 100))

    def test_player_tar_block_padding_alignment(self):
        """Verifica que el padding rellene hasta el múltiplo de 512 (ADR-002/004)."""
        filename = "single_byte.txt"
        content = b"A"
        self.create_file(filename, content.decode())

        recorder = TapeRecorder(self.data_dir)
        recorder.commit()

        tape = Tape(self.data_dir)
        track = next(t for t in tape.get_tracks() if filename in t.arc_path)
        self.assertEqual(track.size, 1)
        self.assertEqual(track.padding_size, 511)
        self.assertEqual(track.total_block_size, 1024)

        full_stream = io.BytesIO()
        for chunk in tape.play(fast_verify=False):
            full_stream.write(chunk)

        stream_bytes = full_stream.getvalue()
        file_section = stream_bytes[track.start_offset : track.end_offset]
        self.assertEqual(len(file_section), 1024)

        data_byte = file_section[512:513]
        padding = file_section[513:]

        self.assertEqual(data_byte, content)
        self.assertEqual(len(padding), 511)
        self.assertEqual(padding, b"\0" * 511)
        self.assertEqual(track.end_offset % 512, 0)

    def test_resume_at_specific_file_offset(self):
        self.create_file("a.txt", "AAAAA")
        self.create_file("b.txt", "BBBBB")

        TapeRecorder(self.data_dir).commit()

        tape = Tape(self.data_dir)
        track_b = [t for t in tape.get_tracks() if "b.txt" in t.arc_path][0]
        start_offset = track_b.start_offset

        buffer = io.BytesIO()
        for chunk in tape.play(start_offset=start_offset):
            buffer.write(chunk)

        buffer.seek(0)
        with tarfile.open(fileobj=buffer, mode="r:") as tf:
            names = tf.getnames()
            self.assertEqual(len(names), 1)
            self.assertTrue(names[0].endswith("b.txt"))

    def test_player_spot_check_detection(self):
        """Verifica que el muestreo aleatorio (Spot Check) detecte mutaciones."""
        for i in range(20):
            self.create_file(f"file_{i}.txt", "content")

        TapeRecorder(self.data_dir).commit()

        corrupt_file = self.data_dir / "file_10.txt"
        os.utime(corrupt_file, (time.time() + 1000, time.time() + 1000))

        tape = Tape(self.data_dir)
        found_error = False
        for _ in range(3):
            try:
                list(tape.play(fast_verify=True))
            except TarIntegrityError:
                found_error = True
                break

        self.assertTrue(found_error, "El spot check no detectó la mutación del archivo")
