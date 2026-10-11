class TarTapeError(Exception):
    """Base exception for all TarTape errors."""


class TarIntegrityError(TarTapeError):
    """Exception raised when physical disk state does not match the T0 inventory snapshot."""


class TapeNotFoundError(TarTapeError):
    """Exception raised when a tape index or metadata directory cannot be found."""


class TapeCorruptedError(TarTapeError):
    """Exception raised when a .tartape catalog file is invalid, unreadable, or corrupted."""


class VolumeChecksumMismatchError(TarTapeError):
    """Exception raised when a volume checksum does not match the recorded hash."""


class PathConstraintError(TarTapeError, ValueError):
    """Exception raised when a path violates USTAR or TarTape ADR-005 constraints."""


class PathConstraintReportError(TarTapeError, ValueError):
    """Exception raised at the end of discovery with a full report of path violations."""


class InvalidOffsetError(TarTapeError, ValueError):
    """Exception raised when an invalid byte offset or byte window is requested."""


class VolumeStateError(TarTapeError, IOError):
    """Exception raised for invalid I/O operations on a TapeVolume (e.g., closed file)."""


class TapeVerificationError(TarTapeError):
    """Exception raised when an unexpected system error occurs during tape verification."""


class LayoutNotFoundError(TarTapeError, KeyError):
    """Exception raised when a requested layout tag or size does not exist."""


class AmbiguousLayoutError(TarTapeError, ValueError):
    """Exception raised when multiple layouts exist and none was explicitly specified."""


class VolumeNotFoundError(TarTapeError, KeyError):
    """Exception raised when a volume index does not exist in a layout."""


class SourceNotFoundError(TarTapeError, FileNotFoundError):
    """Exception raised when source files are required for an operation but not found on disk."""


class ReadOnlyCatalogError(TarTapeError, PermissionError):
    """Exception raised when attempting to modify a catalog opened in read-only mode."""
