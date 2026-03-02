__version__ = "2.1.0b"
__copyright__ = "Copyright (C) 2026-present CalumRakk <https://github.com/CalumRakk>"

from .chunker import TarChunker
from .player import TapePlayer
from .recorder import TapeRecorder
from .schemas import VolumeManifest
from .stream import TarStreamGenerator
from .tape import Tape

__all__ = [
    "Tape",
    "TapeRecorder",
    "TapePlayer",
    "TarStreamGenerator",
    "TarChunker",
    "VolumeManifest",
]
