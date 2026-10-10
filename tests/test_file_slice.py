import unittest

from tartape.schemas import FileSlice


class TestFileSliceHttpRange(unittest.TestCase):
    def test_http_range_calculation(self):
        """Verifica que el rango HTTP sea inclusivo según el estándar RFC 9110."""
        # Un fragmento de 100 bytes que empieza en el byte 0 debe ser bytes=0-99
        fragment = FileSlice(
            volume_index=0,
            volume_offset=0,
            volume_length=100,
            source_offset=0,
        )
        self.assertEqual(fragment.http_range, "bytes=0-99")
        self.assertEqual(fragment.range_header, {"Range": "bytes=0-99"})

    def test_http_range_arbitrary_offset(self):
        """Verifica el cálculo con offsets arbitrarios en volúmenes."""
        # Un fragmento de 1024 bytes que empieza en el byte 512 debe ser 512 a 1535
        fragment = FileSlice(
            volume_index=2,
            volume_offset=512,
            volume_length=1024,
            source_offset=2048,
        )
        self.assertEqual(fragment.http_range, "bytes=512-1535")
        self.assertEqual(fragment.range_header, {"Range": "bytes=512-1535"})

    def test_http_range_single_byte(self):
        """Verifica que un fragmento de 1 solo byte tenga inicio y fin iguales."""
        fragment = FileSlice(
            volume_index=0,
            volume_offset=50,
            volume_length=1,
            source_offset=0,
        )
        self.assertEqual(fragment.http_range, "bytes=50-50")
        self.assertEqual(fragment.range_header, {"Range": "bytes=50-50"})

    def test_http_range_empty_length(self):
        """Verifica comportamiento cuando la longitud es 0."""
        fragment = FileSlice(
            volume_index=0,
            volume_offset=100,
            volume_length=0,
            source_offset=0,
        )
        self.assertEqual(fragment.http_range, "")
        self.assertEqual(fragment.range_header, {"Range": ""})


if __name__ == "__main__":
    unittest.main()
