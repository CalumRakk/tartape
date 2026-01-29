from pathlib import Path
from typing import Generator, List

from .core import TarStreamGenerator
from .schemas import TarEntry, TarEvent


class TarTape:
    """Interfaz amigable para grabar una cinta TAR."""

    def __init__(self):
        self._entries: List[TarEntry] = []

    def add_folder(self, folder_path: str | Path, recursive: bool = True):
        root = Path(folder_path)

        self.add_file(root, arcname=root.name, is_dir=True)

        pattern = "**/*" if recursive else "*"
        for p in root.glob(pattern):
            rel_path = p.relative_to(root.parent)
            if p.is_file():
                self.add_file(p, arcname=str(rel_path), is_dir=False)
            elif p.is_dir():
                self.add_file(p, arcname=str(rel_path), is_dir=True)

    def add_file(
        self, source_path: str | Path, arcname: str | None = None, is_dir: bool = False
    ):
        p = Path(source_path)
        stat = p.stat()
        entry = TarEntry(
            source_path=str(p.absolute()),
            arc_path=str(arcname or p.name),
            size=0 if is_dir else stat.st_size,
            mtime=stat.st_mtime,
            is_dir=is_dir,
        )
        self._entries.append(entry)

    def stream(self, chunk_size: int = 64 * 1024) -> Generator[TarEvent, None, None]:
        """Inicia la grabación y emite el flujo de eventos/bytes."""
        engine = TarStreamGenerator(self._entries, chunk_size=chunk_size)
        yield from engine.stream()
