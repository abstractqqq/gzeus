# Name not decided yet

## What is this package for?

This package is designed for workloads that 

(1) Need to read data from a very large .csv file or a .csv.gz file
(2) Only some of the data, usually less than 50% of the original, is actually needed

This package provides a Chunker class that will read the csv by chunks of given size (in # of bytes). Each chunk will represent a proper csv file (if input file is a proper csv file). Only the first chunk will have header info, if headers are present. The user is supposed to use this to "stream" the csv data chunk by chunk and perform whatever processes the user wants. 

If you intend to filter on the data and return a dataframe, Polars's `scan_csv` is enough for the non-compressed case. For the compressed case, the story is different. Although Polars can also do a lazy .csv.gz scan, it will still try to decompress the entire file into memory first, which is infeasible in many cases. This package will provide an easy way to decompress and output the valid csv data stream.

**Since csv files are just text files, this package can potentially be used to stream large text files as well.**

## Assumptions

The separator provided by the user only shows up in the underlying text file as separators.

## Other Projects to Check Out

1. Dataframe-friendly data analysis package [polars_ds](https://github.com/abstractqqq/polars_ds_extension)