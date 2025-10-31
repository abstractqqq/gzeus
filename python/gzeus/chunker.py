from __future__ import annotations

import os
import sys
import logging

if sys.version_info >= (3, 11):
    from typing import Self
else:  # 3.10, 3.9, 3.8
    from typing_extensions import Self

from pathlib import Path
from typing import Iterator
from .utils import (
    CompressionMethod,
    get_compression_method_local
)
from gzeus._gzeus import (
    PyGzChunker,
)

__all__ = ["Chunker"]
logger = logging.getLogger(__name__)

class Chunker:
    
    def __init__(self, buffer_size:int = 1_000_000, new_line_symbol:str = "\n"):
        """
        Creates a Chunker.

        Parameters
        ----------
        buffer_size
            Buffer size in bytes. 1_000_000 is 1MB. A min of 1MB is used for this value.
            Note: in fact, the actual buffer that will get allocated has size slightly
            greater than the given value. Actual chunk size will vary depending on the compression, 
            but this buffer will be used repeatedly in the process, saving overall memory.
        new_line_symbol
            The symbol for new line. The last such symbol indicates the end of the 
            chunk. And the rest of the bytes will be appended to the front of the next
            chunk of bytes read.
        """    

        self.buffer_size = max(1_000_000, buffer_size)
        if len(new_line_symbol.encode('utf-8')) != 1:
            raise ValueError("The new line symbol must be one byte only.")

        self.symbol:str = new_line_symbol
        self.compression:CompressionMethod = CompressionMethod.UNK
        # Type of _reader is Option[ChunkReader], where ChunkReader represents types implementing the following interface
        # 1. is_finished(self) -> bool
        # 2. read_chunk(self) -> bytes
        # 3. read_full(self) -> bytes
        # 4. n_reads(self) -> usize
        # 5. bytes_decompressed(self) -> int
        # We use duck typing to make it work.
        self._reader = None
        self._description = ""

    def __repr__(self) -> str:
        desc = "Gz File Chunker:\n"
        if self._reader is None:
            desc += "Target file not set."
        else:
            desc += (
                f"Allocated internal buffer size: {self.buffer_size} bytes\n"
                f"New line symbol: {repr(self.symbol)}\n"
                f"{self._description}\n"
                "Read Status:\n"
                f"- # reads: {self._reader.n_reads()}\n"
                f"- Bytes decompressed: {self._reader.bytes_decompressed()}\n"
                f"- Job is finished: {self._reader.is_finished()}"
            )

        return desc

    def _check_reader(self):
        """
        Checks whether the internal reader is set.
        """
        # Always run this when a read is called
        if self._reader is None:
            raise ValueError("Target file is not set yet. Please run `with_*_file` first.")

        if self._reader.is_finished():
            raise ValueError("The reader has finished reading. To begin a new read, please run ``with_*_file` again.")

    def with_buffer_size(self, buffer_size:int) -> Self:
        """
        Resets the buffer size of the Chunker.

        Parameters
        ----------
        buffer_size
            The internal buffer size. It should be at least 1_000_000, which is 1MB. Note that
            if this is too high, your network might complain and you may have have disconnect issues
            when reading from cloud storage like s3.
        """
        if buffer_size <= 1_000_000:
            self.buffer_size = 1_000_000
        else:
            self.buffer_size = buffer_size

        return self

    def with_new_line(self, new_line_symbol:str) -> Self:
        """
        Resets the new line symbol.

        Parameters
        ----------
        new_line_symbol
            The new line symbol for the underlying text file. The most common one is '\n'.
        """

        if len(new_line_symbol) != 1:
            raise ValueError("The new line symbol must be one byte only.")

        self.symbol = new_line_symbol
        return self

    def with_local_file(self, file_path: str | Path, verbose:bool=False) -> Self:
        """
        Prepares the chunker by letting it know the file to be read.

        Parameters
        ----------
        file_path
            The file path
        verbose
            Whether to print more info about the job
        """
        self.compression = get_compression_method_local(file_path)
        if self.compression == CompressionMethod.GZ:
            self._reader = PyGzChunker(str(file_path), self.buffer_size, self.symbol)
        else:
            raise ValueError(
                "The underlying file is not compressed or cannot be properly identified as a .gz file."
            )

        self._description = f"Target file is a local file at path: {file_path}"
        if verbose:
            with open(file_path, "rb") as f:
                # place the cursor in the right position
                f.seek(-4, os.SEEK_END)
                # get the size of uncompressed input from last 4 bytes
                size = int.from_bytes(f.read(), "little" )
                logger.info(f"Estimated uncompressed size is: {size} bytes, estimated # chunks needed: {int(size/self.buffer_size)}. (This estimate can be wrong if the file is a Multi-file gz instead of a single-file gz.)")

        return self

    def n_reads(self) -> int:
        """
        Return the number of chunks read.
        """
        if self._reader is None:
            return 0
        return self._reader.n_reads()

    def is_finished(self) -> bool:
        """
        Whether the current reader has finished reading.
        """
        if self._reader is None:
            return False
        return self._reader.is_finished()

    def bytes_decompressed(self) -> int:
        """
        Returns the number of decompressed bytes so far by the reader.
        """
        if self._reader is None:
            return 0
        return self._reader.bytes_decompressed()

    def read_full(self) -> bytes:
        """
        Read as much as the internal buffer allows, and after this read, declare the read to be finished. 
        This will read at most self.buffer_size number of bytes, and should only be used when you know that 
        the decompressed file is small enough to fit in the buffer. This should be used for convenience only.
        """
        _ = self._check_reader()
        return self._reader.read_full()

    def read_one(self) -> bytes:
        """
        Read one chunk.
        """
        _ = self._check_reader()
        return self._reader.read_chunk()

    def chunks(self) -> Iterator[bytes]:

        _ = self._check_reader()
        while not self._reader.is_finished():
            bytes_read = self._reader.read_chunk()
            if len(bytes_read) > 0:
                yield bytes_read