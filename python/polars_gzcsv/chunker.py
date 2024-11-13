from __future__ import annotations

from pathlib import Path
from typing import Iterator
from .utils import (
    CompressionMethod,
    check_compression
)
from polars_gzcsv._gzcsv import (
    PyGzCsvChunker
    , PyCsvChunker
)

class Chunker:
    
    def __init__(self, buffer_size:int = 1_000_000, separator:str = "\n"):
        """
        Creates a Chunker.

        Parameters
        ----------
        buffer_size
            Buffer size in bytes. 1_000_000 is 1MB. A min of 1MB is used for this value.
        """    

        if buffer_size <= 1_000_000:
            self.buffer_size = 1_000_000
        else:
            self.buffer_size = buffer_size

        sep = separator.encode()
        if len(sep) != 1:
            raise ValueError("The separator must be one byte only.")

        self.sep:bytes = sep
        self.compression:CompressionMethod = CompressionMethod.UNK
        # Type of _reader is Option[ChunkReader], where ChunkReader represents types implementing the following interface
        # 1. is_finished(self) -> bool
        # 2. read_chunk(self) -> bytes
        # 3. n_reads(self) -> usize
        self._reader = None

    def _check_reader(self):
        if self._reader is None:
            raise ValueError("The file is not set yet. Please run `set_file` first.")

    def set_file(self, file_path: str | Path):
        """
        Prepares the chunker by letting it know the file to be read.
        """
        self.compression = check_compression(file_path)
        if self.compression == CompressionMethod.GZ:
            self._reader = PyGzCsvChunker(str(file_path), self.buffer_size, self.sep)
        else:
            self._reader = PyCsvChunker(str(file_path), self.buffer_size, self.sep)

    def show_status(self):
        """
        Prints a message about the status of the reader.
        """
        if self._reader is None:
            print("Target file is not set yet. Please run `set_file` first.")
        else:
            if self._reader.is_finished():
                print(f"Read process has finished. Total number of chunks read: {self._reader.n_reads()}.")
            else:
                if self._reader.has_started():
                    print(f"Read process has started. {self._reader.n_reads()}-chunks have been read.")
                else:
                    print("Read process has not been started.")

    def read_one(self) -> bytes:
        """
        Manually read one chunk.
        """
        _ = self._check_reader()
        return self._reader.read_chunk()

    def chunks(self) -> Iterator[bytes]:

        _ = self._check_reader()

        while not self._reader.is_finished():
            data_bytes = self._reader.read_chunk()
            if len(data_bytes) > 0:
                yield data_bytes
