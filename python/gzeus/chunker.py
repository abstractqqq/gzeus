from __future__ import annotations

import os
import sys

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
    # PyCloudGzChunker
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
            Note: in fact, the actual buffer that will get allocated has size slightly
            greater than the given value. Actual chunk size will vary depending on the compression, 
            but this buffer will be used repeatedly in the process, saving overall memory.
        line_change_symbol
            The symbol for "line change". The last such symbol indicates the end of the 
            chunk. And the rest of the bytes will be appended to the front of the next
            chunk of bytes read.
        """    

        if buffer_size <= 1_000_000:
            self.buffer_size = 1_000_000
        else:
            self.buffer_size = buffer_size

        if len(line_change_symbol.encode('utf-8')) != 1:
            raise ValueError("The line change symbol must be one byte only.")

        self.symbol:str = line_change_symbol
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
                f"Line change symbol: {repr(self.symbol)}\n"
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
            raise ValueError("The reader has finished reading. To being a new read, please run `set_file` again.")

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

    def with_line_change(self, line_change_symbol:str) -> Self:
        """
        Resets the line change byte symbol.

        Parameters
        ----------
        line_change_symbol
            The line change symbol for the underlying text file. The most common one is '\n'.
        """

        if len(line_change_symbol) != 1:
            raise ValueError("The line change symbol must be one byte only.")

        self.symbol = line_change_symbol
        return self

    def with_local_file(self, file_path: str | Path) -> Self:
        """
        Prepares the chunker by letting it know the file to be read.

        Parameters
        ----------
        file_path
            The file path
        """
        self.compression = get_compression_method_local(file_path)
        if self.compression == CompressionMethod.GZ:
            self._reader = PyGzChunker(str(file_path), self.buffer_size, self.symbol)
        else:
            raise ValueError(
                "The underlying file is not compressed, "
                "and you should probably use a Polars lazy scan `pl.scan_csv` or `pl.read_csv_batched`."
            )

        self._description = f"Target file is a local file at path: {file_path}"
        return self

    # def with_s3_file(self, bucket: str, path: str, region:str) -> Self:
    #     """
    #     Set the file target to a file from aws s3. The file must be gz compressed for the subsequent read to work.
    #     """
    #     self._reader = PyCloudGzChunker(str(bucket), str(path), "aws", region, self.buffer_size, self.symbol)
    #     self._description = f"Target file is a S3 file in bucket: {bucket}, path: {path}, region: {region}"
    #     return self

    # def with_gcs_file(self, bucket: str, path: str) -> Self:
    #     """
    #     Set the file target to a file from google cloud storage. The file must be gz compressed for the subsequent 
    #     read to work.
    #     """
    #     import warnings
    #     warnings.warn(
    #         "This code is untested.",
    #         stacklevel=2
    #     )

    #     self._reader = PyCloudGzChunker(str(bucket), str(path), "gcp", "", self.buffer_size, self.symbol)
    #     self._description = f"Target file is a Google Cloud Storage file in bucket: {bucket}, path: {path}"
    #     return self

    # def with_azure_file(self, container: str, path: str) -> Self:
    #     """
    #     Set the file target to a file from azure storage. The file must be gz compressed for the subsequent read to work.
    #     """
    #     import warnings
    #     warnings.warn(
    #         "This code is untested.",
    #         stacklevel=2
    #     )
    #     self._reader = PyCloudGzChunker(str(container), str(path), "azure", "", self.buffer_size, self.symbol)
    #     self._description = f"Target file is a Azure Storage file in container: {container}, path: {path}"
    #     return self

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

    def show_status(self):
        """
        Prints a message about the status of the reader.
        """
        if self._reader is None:
            print("Target file is not set yet. Please run `with_*_file` first.")
        else:
            if self._reader.is_finished():
                print(f"Read process has finished. Total number of chunks read: {self._reader.n_reads()}.")
            else:
                if self._reader.has_started():
                    print(f"Read process has started. {self._reader.n_reads()}-chunks have been read.\n")
                else:
                    print(f"Read process has not been started.")

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