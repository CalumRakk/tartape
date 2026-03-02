import hashlib
import io
import logging
from typing import Optional

from tartape.player import TapePlayer

logger = logging.getLogger(__name__)


class TarVolume(io.BufferedIOBase):
    def __init__(
        self,
        player: TapePlayer,
        start_offset: int,
        end_offset: int,
        name: Optional[str] = None,
    ):
        """
        Args:
            player: Instance of TapePlayer.
            start_offset: Starting byte (inclusive).
            end_offset: Ending byte (exclusive).
            name: Optional name for the file (e.g., 'backup_part_1.tar').
        """

        self.player = player
        self.start_offset = start_offset
        self.end_offset = end_offset
        self.total_tape_size = self.player.tape.total_size

        # Range integrity validations
        if start_offset < 0:
            raise ValueError("The initial offset cannot be negative.")

        if end_offset <= start_offset:
            raise ValueError(
                "The final offset must be greater than the initial offset."
            )

        if end_offset > self.total_tape_size:
            raise ValueError(
                f"Limit exceeded: The end_offset ({end_offset}) is greater than "
                f"the total tape size ({self.total_tape_size})."
            )

        self.size = end_offset - start_offset
        self.name = name or f"vol_{start_offset}.tar"

        # Reading status
        self._position = 0
        self._buffer = bytearray()
        self._stream_gen = None

        # Hash Status
        self._md5 = hashlib.md5()
        self._hash_cursor = 0
        self._md5_invalid = False  # Jump flag (seek) detected

        self._init_stream(offset_in_volume=0)

    def _init_stream(self, offset_in_volume: int):
        """
        Inicializa el generador en un punto específico del volumen.
        Si el offset es 0, reiniciamos el cálculo del MD5.
        """
        self._position = offset_in_volume
        self._buffer.clear()

        if offset_in_volume == 0:
            self._md5 = hashlib.md5()
            self._hash_cursor = 0
            self._md5_invalid = False
        else:
            # If we jump to the middle of the file, the MD5 hash can no longer be guaranteed.
            # Unless the jump is exactly to where the hash left off.
            if offset_in_volume != self._hash_cursor:
                self._md5_invalid = True

        global_target = self.start_offset + offset_in_volume
        self._stream_gen = self.player.play(start_offset=global_target)

    @property
    def md5sum(self) -> str:
        if self._md5_invalid:
            raise RuntimeError(
                f"MD5 not available for '{self.name}': Non-linear read detected"
                "(jumps with seek). To obtain the MD5 hash, the file must be read sequentially."
            )
        if self._hash_cursor < self.size:
            raise RuntimeError(
                f"MD5 not available: Incomplete read ({self._hash_cursor}/{self.size})."
            )
        return self._md5.hexdigest()

    @property
    def is_completed(self) -> bool:
        return self._position == self.size

    def read(self, size: int = -1) -> bytes:  # type: ignore
        if self._position >= self.size:
            return b""

        remaining = self.size - self._position
        bytes_to_read = (
            remaining if (size is None or size < 0) else min(size, remaining)
        )

        while len(self._buffer) < bytes_to_read:
            try:
                assert self._stream_gen is not None, "Stream generator not initialized"
                event = next(self._stream_gen)
                if event.type == "file_data":
                    self._buffer.extend(event.data)
            except StopIteration:
                break

        chunk_size = min(bytes_to_read, len(self._buffer))
        chunk = bytes(self._buffer[:chunk_size])
        self._buffer = self._buffer[chunk_size:]

        if not self._md5_invalid:
            # If the read cursor matches the hash cursor, we continue processing
            if self._position == self._hash_cursor:
                self._md5.update(chunk)
                self._hash_cursor += len(chunk)
            else:
                # The developer read something out of linear order
                self._md5_invalid = True

        self._position += len(chunk)
        return chunk

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """
        Supports jumping to the beginning (rewind) and the end (so that libraries like requests can calculate the Content-Length).
        """
        target = 0
        if whence == io.SEEK_SET:
            target = offset
        elif whence == io.SEEK_CUR:
            target = self._position + offset
        elif whence == io.SEEK_END:
            target = self.size + offset
        else:
            raise ValueError("whence inválido")

        target = max(0, min(target, self.size))

        if target == self._position:
            return self._position

        if target == 0:
            self._init_stream(0)
        else:
            # Arbitrary jump: restart the engine at the new point
            # The MD5 hash will be marked as invalid in the next read() if it's not linear
            self._init_stream(target)
            if target != self._hash_cursor:
                self._md5_invalid = True

        return self._position

    def tell(self) -> int:
        return self._position

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True
