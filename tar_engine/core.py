import hashlib
from typing import Generator

from .constants import CHUNK_SIZE_DEFAULT, TAR_BLOCK_SIZE, TAR_FOOTER_SIZE
from .enums import TarEventType
from .schemas import TarEntry, TarEvent


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
        """Construcción del bloque ustar de 512 bytes."""
        h = bytearray(TAR_BLOCK_SIZE)
        name = item.arc_path.encode("utf-8")[:99]
        h[0 : len(name)] = name
        h[100:107] = b"0000644"
        h[124:135] = f"{item.size:011o}".encode("ascii")
        h[136:147] = f"{int(item.mtime):011o}".encode("ascii")
        h[156] = ord("0")
        h[257:262] = b"ustar"
        h[263:265] = b"00"
        h[148:156] = b"        "
        chksum = sum(h)
        h[148:156] = f"{chksum:06o}\0 ".encode("ascii")
        return bytes(h)
