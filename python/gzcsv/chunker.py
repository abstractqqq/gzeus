from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator
from .utils import (
    CompressionMethod,
    get_compression_method_local
)
from polars_gzcsv._gzcsv import (
    PyGzCsvChunker,
    PyS3GzCsvChunker
)

__all__ = ["Chunker"]

class Chunker:
    
    def __init__(self, buffer_size:int = 1_000_000, line_change_symbol:str = "\n"):
        """
        Creates a Chunker.

        Parameters
        ----------
        buffer_size
            Buffer size in bytes. 1_000_000 is 1MB. A min of 1MB is used for this value.
        line_change_symbol
            The symbol for "line change". The last such symbol indicates the end of the 
            chunk. And the rest of the bytes will be appended to the front of the next
            chunk of bytes read.
        """    

        if buffer_size <= 1_000_000:
            self.buffer_size = 1_000_000
        else:
            self.buffer_size = buffer_size

        symbol = line_change_symbol.encode()
        if len(symbol) != 1:
            raise ValueError("The line change symbol must be one byte only.")

        self.symbol:bytes = symbol
        self.compression:CompressionMethod = CompressionMethod.UNK
        # Type of _reader is Option[ChunkReader], where ChunkReader represents types implementing the following interface
        # 1. is_finished(self) -> bool
        # 2. read_chunk(self) -> bytes
        # 3. n_reads(self) -> usize
        self._reader = None

    def _check_reader(self):
        # Always run this when a read is called
        if self._reader is None:
            raise ValueError("The file is not set yet. Please run `set_file` first.")

        if self._reader.is_finished():
            raise ValueError("The reader has finished reading. To being a new read, please run `set_file` again.")

    def set_file(self, file_path: str | Path):
        """
        Prepares the chunker by letting it know the file to be read.
        """
        self.compression = get_compression_method_local(file_path)
        if self.compression == CompressionMethod.GZ:
            self._reader = PyGzCsvChunker(str(file_path), self.buffer_size, self.symbol)
        else:
            raise ValueError("The underlying file is not compressed, and you should probably use a Polars lazy scan `pl.scan_csv` or `pl.read_csv_batched`.")

    def set_s3_file(self, bucket: str, path: str):
        """
        """
        self._reader = PyS3GzCsvChunker(str(bucket), str(path), self.buffer_size, self.symbol)    

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
                    print(f"Read process has started. {self._reader.n_reads()}-chunks have been read.\n")
                else:
                    print(f"Read process has not been started.")

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