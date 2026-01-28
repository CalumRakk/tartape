from pathlib import Path
from typing import Generator, List

from .core import TarStreamGenerator
from .schemas import TarEntry, TarEvent


class TarTape:
    """Interfaz amigable para grabar una cinta TAR."""

    def __init__(self):
        self._entries: List[TarEntry] = []

    def add_file(self, source_path: str | Path, arcname: str | None = None):
        """Añade un archivo individual a la cinta."""
        p = Path(source_path)
        stat = p.stat()
        entry = TarEntry(
            source_path=str(p.absolute()),
            arc_path=arcname or p.name,
            size=stat.st_size,
            mtime=stat.st_mtime,
        )
        self._entries.append(entry)

    def add_folder(self, folder_path: str | Path, recursive: bool = True):
        """Añade una carpeta completa (escaneo automático)."""
        root = Path(folder_path)
        pattern = "**/*" if recursive else "*"
        for p in root.glob(pattern):
            if p.is_file():
                rel_path = p.relative_to(root.parent)
                self.add_file(p, arcname=str(rel_path))

    def stream(self, chunk_size: int = 64 * 1024) -> Generator[TarEvent, None, None]:
        """Inicia la grabación y emite el flujo de eventos/bytes."""
        engine = TarStreamGenerator(self._entries, chunk_size=chunk_size)
        yield from engine.stream()
