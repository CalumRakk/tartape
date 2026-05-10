class TarTapeError(Exception):
    """Base exception for all TarTape errors."""
    pass

class TarIntegrityError(TarTapeError):
    """Exception raised when physical disk state does not match the T0 inventory snapshot."""
    pass

class TapeNotFoundError(TarTapeError):
    """Exception raised when a tape index or metadata directory cannot be found."""
    pass

class PathConstraintError(TarTapeError, ValueError):
    """Exception raised when a path violates USTAR or TarTape ADR-005 constraints."""
    pass

class InvalidOffsetError(TarTapeError, ValueError):
    """Exception raised when an invalid byte offset or byte window is requested."""
    pass

class VolumeStateError(TarTapeError, IOError):
    """Exception raised for invalid I/O operations on a TapeVolume (e.g., closed file)."""
    pass

class TapeVerificationError(TarTapeError):
    """Exception raised when an unexpected system error occurs during tape verification."""
    pass
