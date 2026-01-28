from enum import Enum


class TarEventType(Enum):
    FILE_START = "file_start"  # Antes de emitir el header
    FILE_DATA = "file_data"  # Trozos de bytes (Header, Contenido, Padding)
    FILE_END = "file_end"  # Después del padding, incluye MD5
    TAPE_COMPLETED = "tape_completed"  # Fin de la cinta (Footer emitido)
