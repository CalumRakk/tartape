import os
import stat as stat_module
from pathlib import Path
from typing import Generator, List, Optional, Tuple

try:
    import grp
    import pwd
except ImportError:
    pwd = None
    grp = None

from .core import TarStreamGenerator
from .schemas import TarEntry, TarEvent


class TarEntryFactory:
    """
    Responsable exclusivamente de inspeccionar el sistema de archivos
    y fabricar objetos TarEntry válidos.

    Centraliza:
    1. El uso de lstat (evitar seguir symlinks).
    2. El filtrado de tipos (Solo File, Dir, Link).
    3. La extracción de metadatos (Usuarios, Grupos, Modos).
    """

    @classmethod
    def create(cls, source_path: Path, arcname: str) -> Optional[TarEntry]:
        """
        Analiza una ruta y crea un TarEntry.
        Retorna None si el archivo es de un tipo no soportado (Socket, Pipe, etc).
        Lanza OSError/FileNotFoundError si hay problemas de acceso.
        """
        st = source_path.lstat()
        mode = st.st_mode

        is_dir, is_file, is_symlink = cls._diagnose_type(mode)

        if not (is_dir or is_file or is_symlink):
            return None

        file_mode, uid, gid, uname, gname = cls._extract_metadata(st)

        linkname = ""
        size = st.st_size

        if is_symlink:
            linkname = os.readlink(source_path)
            size = 0  # En TAR, size de los symlinks es 0
        elif is_dir:
            size = 0  # Los directorios pesan 0 en el header TAR

        return TarEntry(
            source_path=str(source_path.absolute()),
            arc_path=arcname,
            size=size,
            mtime=st.st_mtime,
            is_dir=is_dir,
            is_symlink=is_symlink,
            linkname=linkname,
            mode=file_mode,
            uid=uid,
            gid=gid,
            uname=uname,
            gname=gname,
        )

    @staticmethod
    def _diagnose_type(mode: int) -> Tuple[bool, bool, bool]:
        """Retorna (is_dir, is_reg, is_symlink) basado en el modo."""
        return (
            stat_module.S_ISDIR(mode),
            stat_module.S_ISREG(mode),
            stat_module.S_ISLNK(mode),
        )

    @staticmethod
    def _extract_metadata(st: os.stat_result) -> Tuple[int, int, int, str, str]:
        """Obtiene mode, uid, gid, uname, gname de forma segura."""
        # S_IMODE limpia los bits de tipo (ej: quita el bit que dice "soy directorio")
        # quedándose solo con los permisos (0o755).
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


class TarTape:
    """Interfaz amigable para grabar una cinta TAR."""

    def __init__(self):
        self._entries: List[TarEntry] = []

    def add_folder(self, folder_path: str | Path, recursive: bool = True):
        """Escanea una carpeta y agrega sus contenidos."""
        root = Path(folder_path)

        # carpeta raíz
        self.add_file(root, arcname=root.name)

        pattern = "**/*" if recursive else "*"
        for p in root.glob(pattern):
            try:
                rel_path = p.relative_to(root.parent)
                self.add_file(p, arcname=rel_path.as_posix())
            except (ValueError, OSError):
                # si glob lista algo al que no se puede acceder. No hay fallo total, simplemente lo saltamos.
                continue

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

    def add_file(self, source_path: str | Path, arcname: str | None = None):
        """Agrega un archivo a la cinta.

        Args:
            source_path: Ruta al archivo.
            arcname: Nombre del archivo en la cinta.

        Returns:
            None
        """
        p = Path(source_path)
        name = arcname or p.name
        name_unix = name.replace("\\", "/")

        entry = TarEntryFactory.create(p, name_unix)
        if entry:
            self._entries.append(entry)
        # Si entry es None, se ignoró silenciosamente (Socket/Pipe/etc)

    def stream(self, chunk_size: int = 64 * 1024) -> Generator[TarEvent, None, None]:
        """Inicia la grabación y emite el flujo de eventos/bytes."""
        engine = TarStreamGenerator(self._entries, chunk_size=chunk_size)
        yield from engine.stream()
