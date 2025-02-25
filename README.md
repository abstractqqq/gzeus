# GZeus 

is a package that chunk-reads *GZipped* text files *LIGHTENING* fast. 

## What is this package for?

This package is designed for workloads that 

1. Need to read data from a very large .csv.gz file

2. You have additional rules that you want to apply while reading, and you want to work in a chunk by chunk fashion that saves you memory.

3. You know what you are doing and prefer a more customizable experience than a package like [polars_streaming_csv_decompression](https://github.com/ghuls/polars_streaming_csv_decompression)

This package provides a Chunker class that will read gz compressed text file by chunks. In the case of csv files, each chunk will represent a proper decompressed csv file and only the first chunk will have header info, if headers are present. The Chunker will produce these chunks in a streaming fashion, thus minimizing memory load.

**This package can potentially be used to stream large gzipped text files as well. But is not capable of semantic chunking, which is often needed for text processing for LLMs. This package only chunks by identifying the last needle (new line character) in the haystack (text string) in the current buffer.**

## Assumptions

The new_line_symbol provided by the user only shows up in the underlying text file as new line symbol.

## We get decompressed bytes by chunks, then what? 

Most of the times, we only need to extract partial data from large .csv.gz files. This is where the combination of GZeus and Polars really shines. 

If you have Polars installed already:
```python
from gzeus import stream_polars_csv_gz

for output_of_your_func in stream_polars_csv_gz("PATH TO YOUR DATA", func = your_func):
    # do work on the output of your func
```
where your_func should be `pl.LazyFrame -> Any`. If you need more control over the iteration and data bytes, you can structure you code as below:

```python
from gzeus import Chunker
import polars as pl

# Turn portion of the produced bytes into a DataFrame.
def bytes_into_df(df:pl.LazyFrame) -> pl.DataFrame:
    return df.filter(
        pl.col("City_Category") == 'A'
    ).select("City_Category", "Primary_Bank_Type", "Source").collect()

ck = (
    Chunker(buffer_size=1_000_000, new_line_symbol='\n')
    .with_local_file("../data/test.csv.gz")
)

df_temp = pl.scan_csv(ck.read_one()) # first chunk
schema = df_temp.collect_schema() # Infer schema from first chunk
dfs = [bytes_into_df(df_temp)]

dfs.extend(
    bytes_into_df(
        pl.scan_csv(byte_chunk, has_header=False, schema=schema)
    )
    for byte_chunk in ck.chunks()
)

df = pl.concat(dfs)
df.head()
```

## Performance vs. Pandas

See [here](./benches/bench.ipynb).

It is extremely hard to have an apples-to-apples comparison with other tools. Here I will focus on the comparison with pandas.read_csv, which has an iterator option. Note: GZeus chunks are defined by byte-sizes, while pandas.read_csv iterator has a fixed number of rows per chunk.

However, generally speaking, I find that for .csv.gz files:

1. GZeus + Polars is at least a 50% reduction in time than pd.read_csv with zero additional work on each chunk
2. If you set higher buffer size, GZeus + Polars can take only 1/5 of the time of pandas.read_csv.
3. Even faster with more workload per chunk (mostly because of Polars).

## Cloud Files

To support "chunk read" from any major cloud provider is no easy task. Not only will it require an async interface in Rust, which is much harder to write and maintain, but there are also performance issues related to getting only a small chunk of data each time. To name a few:

1. Increase the number of calls to the storage
2. Repeatedly opening the file and seeking to the last read position. 
3. Rate limits issues, especially with VPN. E.g. to get better performance, gzeus needs to read 10MB+ per chunk, but this will increase "packets per second" significantly.

A workaround is to use temp files. For example, for AWS s3, one can do the following:

```python
import tempfile
import boto3

s3 = boto3.client('s3')

tmp = tempfile.NamedTemporaryFile()
s3.download_fileobj('amzn-s3-demo-bucket', 'OBJECT_NAME', tmp)
df = chunk_load_data_using_gzeus(tmp.name) # a wrapper function for the code shown above.
tmp.close()
```

Almost always, the machine should have enough disk space. In `chunk_load_data_using_gzeus`, data is read by chunks and therefore won't lead to OOM errors. It can be any wrapper around `stream_polars_csv_gz` provided by the package.

## Road Maps
1. To be decided

## Other Projects to Check Out
1. Dataframe-friendly data analysis package [polars_ds](https://github.com/abstractqqq/polars_ds_extension)
2. For a more sophisticated but more feature-complete package for streaming csv.gz, see [polars_streaming_csv_decompression](https://github.com/ghuls/polars_streaming_csv_decompression)