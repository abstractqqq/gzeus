from __future__ import annotations

__version__ = "0.1.0"

from pathlib import Path
from .chunker import Chunker
from collections.abc import Iterable, Callable
from typing import Any

def stream_polars_csv_gz(
    file_path: str | Path
    , buffer_size: int = 10_000_000
    , new_line_symbol: str = "\n"
    , func: Callable | None = None
    , schema: "pl.Schema" | None = None
    , **kwargs
) -> Iterable[Any]:
    """
    Helper function that reads .csv.gz files in chunks. This requires Polars
    to be installed and should be version >= 1. This doesn't check Polars version
    for the user. The data schema, if not provided, will be inferred on the first chunk.
    The output type of this is an iterable of the output of `func`. If `func` 
    is None, then output will be an Iterable[pl.DataFrame]. If `func` returns (int, pl.DataFrame),
    then output will be Iterable[(int, pl.DataFrame)], etc.

    Please make sure that your system has > 10mb of RAM.

    Parameters
    ----------
    file_path
        Local file path or a temp file's name
    buffer_size
        Buffer size for the underlying chunker
    new_line_symbol
        The new line symbol for the .csv.gz file
    func
        An optional processor function that processes each chunk. The function signature
        should be func(df: pl.LazyFrame) -> Any. Notice the function input should
        be a lazy frame, because we can maximally optimize our operation on the chunk when it 
        is only scanned, not fully read into memory.
    schema
        Schema of the dataset, if known. If none, this will be inferred on the first chunk.
    **kwargs
        Kwargs passed to Polars's scan_csv. Kwargs should not contain `has_header`, 
        since it is used internally.
    """
    import polars as pl

    if 'has_header' in kwargs:
        raise ValueError("Input `has_header` should not be a kwarg.")

    ck = Chunker(buffer_size=buffer_size, new_line_symbol=new_line_symbol).with_local_file(file_path)

    df_temp = pl.scan_csv(ck.read_one(), **kwargs) # first chunk
    if schema is None:
        use_schema = df_temp.collect_schema() # Infer schema from first chunk
    else:
        use_schema = schema

    if func is None:
        yield df_temp.collect()
    else:
        yield func(df_temp)

    for byte_chunk in ck.chunks():
        if func is None:
            yield pl.read_csv(byte_chunk, has_header=False, schema=use_schema, **kwargs)
        else:
            yield func(
                pl.scan_csv(byte_chunk, has_header=False, schema=use_schema, **kwargs)
            )



