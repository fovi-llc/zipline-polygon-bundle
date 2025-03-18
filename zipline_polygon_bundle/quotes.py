from .config import PolygonConfig
from .trades import cast_strings_to_list

import os

import pyarrow as pa
from pyarrow import dataset as pa_ds
from pyarrow import compute as pa_compute
from pyarrow import fs as pa_fs
from fsspec.implementations.arrow import ArrowFSWrapper
from pyarrow import csv as pa_csv


def quotes_schema(raw: bool = False) -> pa.Schema:
    # There is some problem reading the timestamps as timestamps so we have to read as integer then change the schema.
    # I make the timestamp timezone-aware because that's how Unix timestamps work and it may help avoid mistakes.
    # timestamp_type = pa.timestamp("ns", tz="UTC")
    timestamp_type = pa.int64() if raw else pa.timestamp("ns", tz="UTC")

    # Polygon price scale is 4 decimal places (i.e. hundredths of a penny), but we'll use 10 because we have precision to spare.
    # price_type = pa.decimal128(precision=38, scale=10)
    # 64bit float a little overkill but avoids any plausible truncation error.
    price_type = pa.float64()

    # ticker: string
    # ask_exchange: int64
    # ask_price: double
    # ask_size: int64
    # bid_exchange: int64
    # bid_price: double
    # bid_size: int64
    # conditions: string
    # indicators: int64
    # participant_timestamp: int64
    # sequence_number: int64
    # sip_timestamp: int64
    # tape: int64
    # trf_timestamp: int64

    return pa.schema(
            [
                pa.field("ticker", pa.string(), nullable=False),
                pa.field("ask_exchange", pa.int8(), nullable=False),
                pa.field("ask_price", price_type, nullable=False),
                pa.field("ask_size", pa.int64(), nullable=False),
                pa.field("bid_exchange", pa.int8(), nullable=False),
                pa.field("bid_price", price_type, nullable=False),
                pa.field("bid_size", pa.int64(), nullable=False),
                pa.field("conditions", pa.string(), nullable=False),
                pa.field("indicators", pa.string(), nullable=False),
                pa.field("participant_timestamp", timestamp_type, nullable=False),
                pa.field("sequence_number", pa.int64(), nullable=False),
                pa.field("sip_timestamp", timestamp_type, nullable=False),
                pa.field("tape", pa.int8(), nullable=False),
                pa.field("trf_timestamp", timestamp_type, nullable=False),
            ]
        )


def quotes_dataset(config: PolygonConfig) -> pa_ds.Dataset:
    """
    Create a pyarrow dataset from the quotes files.
    """

    # https://arrow.apache.org/docs/python/filesystems.html#using-arrow-filesystems-with-fsspec
    # https://filesystem-spec.readthedocs.io/en/latest/_modules/fsspec/spec.html#AbstractFileSystem.glob.
    fsspec = ArrowFSWrapper(config.filesystem)

    # We sort by path because they have the year and month in the dir names and the date in the filename.
    paths = sorted(
        fsspec.glob(os.path.join(config.quotes_dir, config.csv_paths_pattern))
    )

    return pa_ds.FileSystemDataset.from_paths(paths,
                                              format=pa_ds.CsvFileFormat(),
                                              schema=quotes_schema(raw=True),
                                              filesystem=config.filesystem)


def cast_strings_to_list(string_array, separator=",", default="0", value_type=pa.uint8()):
    """Cast a PyArrow StringArray of comma-separated numbers to a ListArray of values."""

    # Create a mask to identify empty strings
    is_empty = pa_compute.equal(pa_compute.utf8_trim_whitespace(string_array), "")

    # Use replace_with_mask to replace empty strings with the default ("0")
    filled_column = pa_compute.replace_with_mask(string_array, is_empty, pa.scalar(default))

    # Split the strings by comma
    split_array = pa_compute.split_pattern(filled_column, pattern=separator)

    # Cast each element in the resulting lists to integers
    int_list_array = pa_compute.cast(split_array, pa.list_(value_type))

    return int_list_array


def cast_quotes(quotes):
    quotes = quotes.cast(quotes_schema())
    condition_values = cast_strings_to_list(quotes.column("conditions").combine_chunks())
    return quotes.append_column('condition_values', condition_values)
