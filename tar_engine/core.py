import hashlib
from typing import Generator

from .constants import CHUNK_SIZE_DEFAULT, TAR_BLOCK_SIZE, TAR_FOOTER_SIZE
from .enums import TarEventType
from .schemas import TarEntry, TarEvent


class TarHeader:
    def __init__(self):
        self.buffer = bytearray(512)

    def set_string(self, offset: int, field_width: int, value: str):
        """Escribe una cadena codificada en UTF-8 y truncada."""
        data = value.encode("utf-8")[:field_width]
        self.buffer[offset : offset + len(data)] = data

    def set_octal(self, offset: int, field_width: int, value: int):
        """
        Escribe un número en formato octal siguiendo el estándar TAR:
        1. Convierte el número a octal.
        2. Rellena con ceros a la izquierda.
        3. Deja espacio para el terminador nulo (NULL) al final.
        """
        # Convertir el entero a una cadena octal (ej: 511 -> '777')
        # oct() devuelve algo como '0o777', así que quitamos los dos primeros caracteres
        octal_string = oct(int(value))[2:]

        # El estándar TAR espera que el campo termine en un byte nulo (\0)
        # Por lo tanto, el espacio disponible para los dígitos es field_width - 1
        max_digits = field_width - 1

        # Rellena con ceros a la izquierda hasta alcanzar ese tamaño máximo
        # Si el número es '777' y field_width es 12, necesitamos 8 ceros delante
        padded_octal = octal_string.zfill(max_digits)

        # Aseguramos de el valor no exceda a field_width
        if len(padded_octal) > field_width:
            raise ValueError(
                f"Number {value} is too large for field width {field_width}"
            )

        final_string = padded_octal + "\0"
        data_bytes = final_string.encode("ascii")

        self.buffer[offset : offset + field_width] = data_bytes

    def set_bytes(self, offset: int, value: bytes):
        """Escribe bytes crudos en una posición."""
        self.buffer[offset : offset + len(value)] = value

    def calculate_checksum(self):
        """
        Calcula y escribe el checksum del header TAR (formato USTAR).

        El checksum es una suma simple de los valores numéricos de los 512 bytes del header.
        Se utiliza únicamente como verificación básica de integridad del header (no es un
        mecanismo criptográfico ni valida el contenido de los archivos).

        Reglas del estándar TAR:
        - El campo del checksum (offset 148, longitud 8 bytes) debe tratarse como si
        contuviera espacios ASCII (valor 32) durante el cálculo, independientemente de
        su contenido real (ceros, basura o un checksum previo).
        - El valor final se almacena como 6 dígitos en octal, seguidos de un byte nulo
        y un espacio.

        Esto permite detectar errores comunes de lectura o escritura en el header,
        aunque no garantiza que los datos del archivo estén intactos.
        """

        # reemplaza temporalmente sus 8 bytes por espacios (ASCII 32) como se indica en el estándar.
        self.buffer[148:156] = b" " * 8

        # Calcula la suma de los 512 bytes del header
        # Cada posición del buffer contiene un número entre 0 y 255.
        # El checksum es simplemente la suma de todos esos valores.
        suma_total = 0
        for byte_valor in self.buffer:
            suma_total += byte_valor

        # - Convertimos a octal (base 8) y quitamos el prefijo "0o"
        # - Rellenamos con ceros a la izquierda hasta tener 6 caracteres
        # - Construimos el campo final de 8 bytes: [oct][oct][oct][doct][oct][oct][\0][ ]
        octal_sum = oct(suma_total)[2:]
        octal_filled = octal_sum.zfill(6)
        final_string = octal_filled + "\0" + " "

        self.buffer[148:156] = final_string.encode("ascii")

    def build(self) -> bytes:
        self.calculate_checksum()
        return bytes(self.buffer)


class TarStreamGenerator:
    def __init__(self, entries: list[TarEntry], chunk_size: int = CHUNK_SIZE_DEFAULT):
        self.entries = entries
        self.chunk_size = chunk_size
        self._emitted_bytes = 0

    def stream(self) -> Generator[TarEvent, None, None]:
        for entry in self.entries:
            # Inicio de archivo
            yield TarEvent(
                type=TarEventType.FILE_START,
                entry=entry,
                metadata={"start_offset": self._emitted_bytes},
            )

            # Header
            header = self._build_header(entry)
            self._emitted_bytes += len(header)
            yield TarEvent(type=TarEventType.FILE_DATA, data=header, entry=entry)

            # Data
            md5 = hashlib.md5()
            with open(entry.source_path, "rb") as f:
                while chunk := f.read(self.chunk_size):
                    md5.update(chunk)
                    self._emitted_bytes += len(chunk)
                    yield TarEvent(type=TarEventType.FILE_DATA, data=chunk, entry=entry)

            # Padding
            padding_size = (
                TAR_BLOCK_SIZE - (entry.size % TAR_BLOCK_SIZE)
            ) % TAR_BLOCK_SIZE
            if padding_size > 0:
                padding = b"\0" * padding_size
                self._emitted_bytes += padding_size
                yield TarEvent(type=TarEventType.FILE_DATA, data=padding, entry=entry)

            # Fin de archivo
            yield TarEvent(
                type=TarEventType.FILE_END,
                entry=entry,
                metadata={"md5sum": md5.hexdigest(), "end_offset": self._emitted_bytes},
            )

        # Footer
        footer = b"\0" * TAR_FOOTER_SIZE
        self._emitted_bytes += len(footer)
        yield TarEvent(type=TarEventType.FILE_DATA, data=footer)
        yield TarEvent(type=TarEventType.TAPE_COMPLETED)

    def _build_header(self, item: TarEntry) -> bytes:
        """Construye un header para un archivo.

        - https://www.ibm.com/docs/en/zos/2.4.0?topic=formats-tar-format-tar-archives#taf__outar
        """
        # 1. Preparación de rutas (Lógica de Prefijo/Nombre)
        name, prefix = self._split_path(item.arc_path)

        h = TarHeader()
        h.set_string(0, 100, name)  # name
        h.set_octal(100, 8, 0o644)  # mode # TODO: Agregar soporte para otros modos.
        h.set_octal(108, 8, 0)  # uid
        h.set_octal(116, 8, 0)  # gid

        h.set_octal(124, 12, item.size)  # size
        h.set_octal(136, 12, int(item.mtime))  # mtime
        h.set_bytes(156, b"0")  # typeflag (0 = normal file)

        # Indica que el archivo sigue un estandar moderno:
        h.set_bytes(257, b"ustar\0")  # magic (ustar + null)
        h.set_bytes(263, b"00")  # version

        h.set_string(345, 155, prefix)  # prefix
        return h.build()

    @staticmethod
    def _split_path(path: str) -> tuple[str, str]:
        """Divide una ruta para cumplir con el límite de 100/155 bytes del prefix."""
        if len(path) <= 100:
            return path, ""

        # Buscamos el punto de corte ideal en una barra '/'
        # El prefijo puede tener hasta 155 caracteres
        split_at = path.rfind("/", 0, 156)
        if split_at == -1:
            raise ValueError(f"Path is too long ({len(path)}): {path}")

        prefix = path[:split_at]
        name = path[split_at + 1 :]

        if len(name) > 155:
            raise ValueError(f"Name is too long ({len(name)}): {name}")

        return name, prefix
