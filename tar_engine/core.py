import hashlib
from typing import Generator

from .constants import CHUNK_SIZE_DEFAULT, TAR_BLOCK_SIZE, TAR_FOOTER_SIZE
from .enums import TarEventType
from .schemas import (
    FileEndMetadata,
    FileStartMetadata,
    TarEntry,
    TarEvent,
    TarFileDataEvent,
    TarFileEndEvent,
    TarFileStartEvent,
    TarTapeCompletedEvent,
)


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
        # - Construimos el campo final de 8 bytes: [oct][oct][oct][oct][oct][oct][\0][ ]
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
            yield TarFileStartEvent(
                type=TarEventType.FILE_START,
                entry=entry,
                metadata=FileStartMetadata(start_offset=self._emitted_bytes),
            )

            # HEADER: Siempre son 512 bytes
            header = self._build_header(entry)
            yield TarFileDataEvent(
                type=TarEventType.FILE_DATA, data=header, entry=entry
            )
            self._emitted_bytes += len(header)

            md5 = hashlib.md5()
            bytes_written = 0

            # DATA (Solo archivos normales, ni dirs ni symlinks tienen body)
            if not entry.is_dir and not entry.is_symlink:
                with open(entry.source_path, "rb") as f:
                    while chunk := f.read(self.chunk_size):
                        md5.update(chunk)
                        yield TarFileDataEvent(
                            type=TarEventType.FILE_DATA, data=chunk, entry=entry
                        )
                        self._emitted_bytes += len(chunk)
                        bytes_written += len(chunk)

                # Si el archivo cambió de tamaño mientras leíamos, abortamos.
                if bytes_written != entry.size:
                    raise RuntimeError(
                        f"File integrity compromised: '{entry.source_path}'. "
                        f"Expected {entry.size} bytes, read {bytes_written} bytes. "
                        "Aborting to prevent archive corruption."
                    )

                # PADDING
                padding_size = (
                    TAR_BLOCK_SIZE - (entry.size % TAR_BLOCK_SIZE)
                ) % TAR_BLOCK_SIZE

                if padding_size > 0:
                    padding = b"\0" * padding_size
                    yield TarFileDataEvent(
                        type=TarEventType.FILE_DATA, data=padding, entry=entry
                    )
                    self._emitted_bytes += padding_size

            # Fin de ítem (Con MD5 si aplica)
            md5sum = (
                md5.hexdigest() if (not entry.is_dir and not entry.is_symlink) else None
            )
            yield TarFileEndEvent(
                type=TarEventType.FILE_END,
                entry=entry,
                metadata=FileEndMetadata(
                    md5sum=md5sum,
                    end_offset=self._emitted_bytes,
                ),
            )

        # FOOTER 1024 bytes nulos al final de la cinta
        footer = b"\0" * TAR_FOOTER_SIZE
        yield TarFileDataEvent(type=TarEventType.FILE_DATA, data=footer)
        self._emitted_bytes += len(footer)

        yield TarTapeCompletedEvent(type=TarEventType.TAPE_COMPLETED)

    def _build_header(self, item: TarEntry) -> bytes:
        """Construye un header para un archivo.

        - https://www.ibm.com/docs/en/zos/2.4.0?topic=formats-tar-format-tar-archives#taf__outar
        """
        name, prefix = self._split_path(item.arc_path)
        if item.is_dir and not name.endswith("/"):
            name += "/"

        h = TarHeader()
        h.set_string(0, 100, name)  # name: Nombre del archivo
        h.set_octal(100, 8, item.mode)  # mode: Permisos (ej: 0644)
        h.set_octal(108, 8, item.uid)  # uid: ID del propietario
        h.set_octal(116, 8, item.gid)  # gid: ID del grupo
        h.set_octal(124, 12, item.size)  # size: Tamaño en bytes
        h.set_octal(136, 12, int(item.mtime))  # mtime: Fecha de modificación

        # TYPE FLAG
        # '0' = File, '5' = Dir, '2' = Symlink
        if item.is_symlink:
            type_flag = b"2"
        elif item.is_dir:
            type_flag = b"5"
        else:
            type_flag = b"0"

        h.set_bytes(156, type_flag)

        # Si es symlink, aquí va el destino
        if item.is_symlink:
            h.set_string(157, 100, item.linkname)

        # Firma USTAR (Identifica que usamos la extensión moderna)
        h.set_bytes(257, b"ustar\0")
        h.set_bytes(263, b"00")

        # User/Group Names
        h.set_string(265, 32, item.uname)
        h.set_string(297, 32, item.gname)

        # Permite que la ruta completa llegue a 255 caracteres (155 prefix + 100 name)
        h.set_string(345, 155, prefix)
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
