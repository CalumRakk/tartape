__version__ = "2.0.0"
__copyright__ = "Copyright (C) 2026-present CalumRakk <https://github.com/CalumRakk>"

from .player import TapePlayer
from .recorder import TapeRecorder
from .stream import TarStreamGenerator
from .tape import Tape

__all__ = ["Tape", "TapeRecorder", "TapePlayer", "TarStreamGenerator"]
