import os
import stat as stat_module
from pathlib import Path
from typing import Generator, List

try:
    import grp
    import pwd
except ImportError:
    pwd = None
    grp = None

from .core import TarStreamGenerator
from .schemas import TarEntry, TarEvent


class TarTape:
    """Interfaz amigable para grabar una cinta TAR."""

    def __init__(self):
        self._entries: List[TarEntry] = []

    def add_folder(self, folder_path: str | Path, recursive: bool = True):
        """Escanea una carpeta y agrega sus contenidos."""
        root = Path(folder_path)

        # carpeta raíz
        self.add_file(root, arcname=root.name, is_dir=True)

        pattern = "**/*" if recursive else "*"
        for p in root.glob(pattern):
            rel_path = p.relative_to(root.parent)

            is_d = p.is_dir()
            self.add_file(p, arcname=str(rel_path), is_dir=is_d)

    def _get_os_metadata(self, st: os.stat_result) -> tuple[int, int, int, str, str]:
        """
        Extrae metadatos de un objeto stat_result.
        Retorna: (mode, uid, gid, uname, gname)
        """

        mode = stat_module.S_IMODE(st.st_mode)

        uid = st.st_uid
        gid = st.st_gid

        uname = ""
        gname = ""

        if pwd:
            try:
                uname = pwd.getpwuid(uid).pw_name  # type: ignore
            except (KeyError, AttributeError):
                uname = str(uid)

        if grp:
            try:
                gname = grp.getgrgid(gid).gr_name  # type: ignore
            except (KeyError, AttributeError):
                gname = str(gid)

        return mode, uid, gid, uname, gname

    def add_file(
        self, source_path: str | Path, arcname: str | None = None, is_dir: bool = False
    ):
        p = Path(source_path)
        st = p.lstat()
        mode, uid, gid, uname, gname = self._get_os_metadata(st)

        entry = TarEntry(
            source_path=str(p.absolute()),
            arc_path=str(arcname or p.name),
            size=0 if is_dir else st.st_size,
            mtime=st.st_mtime,
            is_dir=is_dir,
            mode=mode,
            uid=uid,
            gid=gid,
            uname=uname,
            gname=gname,
        )
        self._entries.append(entry)

    def stream(self, chunk_size: int = 64 * 1024) -> Generator[TarEvent, None, None]:
        """Inicia la grabación y emite el flujo de eventos/bytes."""
        engine = TarStreamGenerator(self._entries, chunk_size=chunk_size)
        yield from engine.stream()
