from .config import PolygonConfig

from typing import Iterator

import argparse
import glob
import os

import pyarrow as pa
from pyarrow import dataset as pa_ds
from pyarrow import csv as pa_csv

PARTITION_COLUMN_NAME = "part"


# To work across all reasonable filesystems, we need to escape the characters in partition keys that are treated weirdly in filenames.
def partition_key_escape(c: str) -> str:
    return ("^" + c.upper()) if c.islower() else ("%" + "%02X" % ord(c))


def to_partition_key(s: str) -> str:
    if s.isalnum() and s.isupper():
        return s
    return "".join(
        [f"{c if (c.isupper() or c.isdigit()) else partition_key_escape(c)}" for c in s]
    )


def read_csv_table(path, timestamp_type: pa.TimestampType, convert_options):
    table = pa.csv.read_csv(path, convert_options=convert_options)
    table = table.set_column(
        table.column_names.index("window_start"),
        "window_start",
        table.column("window_start").cast(timestamp_type),
    )
    return table


def csv_agg_scanner(
    paths: list, schema: pa.Schema, timestamp_type: pa.TimestampType
) -> Iterator[pa.RecordBatch]:
    for path in paths:
        convert_options = pa_csv.ConvertOptions(
            column_types=schema,
            strings_can_be_null=False,
            quoted_strings_can_be_null=False,
        )

        print(f"{path=}")
        table = read_csv_table(
            path=path, timestamp_type=timestamp_type, convert_options=convert_options
        )

        table = table.append_column(
            PARTITION_COLUMN_NAME,
            pa.array(
                [to_partition_key(ticker) for ticker in table.column("ticker").to_pylist()]
            ),
        )

        for batch in table.to_batches():
            yield batch


def concat_all_aggs_from_csv(
    config: PolygonConfig,
    aggs_pattern: str = "**/*.csv.gz",
) -> list:
    """zipline does bundle ingestion one ticker at a time."""

    # We sort by path because they have the year and month in the dir names and the date in the filename.
    paths = sorted(
        list(
            glob.glob(
                os.path.join(config.aggs_dir, aggs_pattern),
                recursive="**" in aggs_pattern,
            )
        )
    )

    # Polygon Aggregate flatfile timestamps are in nanoseconds (like trades), not milliseconds as the docs say.
    # I make the timestamp timezone-aware because that's how Unix timestamps work and it may help avoid mistakes.
    timestamp_type = pa.timestamp("ns", tz="UTC")

    # But we can't use the timestamp type in the schema here because it's not supported by the CSV reader.
    # So we'll use int64 and cast it after reading the CSV file.
    # https://github.com/apache/arrow/issues/44030

    # strptime(3) (used by CSV reader for timestamps in ConvertOptions.timestamp_parsers) supports Unix timestamps (%s) and milliseconds (%f) but not nanoseconds.
    # https://www.geeksforgeeks.org/how-to-use-strptime-with-milliseconds-in-python/
    # Actually that's the wrong strptime (it's Python's).  C++ strptime(3) doesn't even support %f.
    # https://github.com/apache/arrow/issues/39839#issuecomment-1915981816
    # Also I don't think you can use those in a format string without a separator.

    # Polygon price scale is 4 decimal places (i.e. hundredths of a penny), but we'll use 10 because we have precision to spare.
    price_type = pa.decimal128(precision=38, scale=10)

    polygon_aggs_schema = pa.schema(
        [
            pa.field("ticker", pa.string(), nullable=False),
            pa.field("volume", pa.int64(), nullable=False),
            pa.field("open", price_type, nullable=False),
            pa.field("close", price_type, nullable=False),
            pa.field("high", price_type, nullable=False),
            pa.field("low", price_type, nullable=False),
            pa.field("window_start", pa.int64(), nullable=False),
            pa.field("transactions", pa.int64(), nullable=False),
        ]
    )

    partitioned_schema = polygon_aggs_schema.append(
        pa.field(PARTITION_COLUMN_NAME, pa.string(), nullable=False)
    )
    agg_scanner = pa_ds.Scanner.from_batches(
        csv_agg_scanner(paths=paths, schema=polygon_aggs_schema, timestamp_type=timestamp_type),
        schema=partitioned_schema
    )

    by_ticker_base_dir = os.path.join(
        config.by_ticker_dir,
        f"{config.agg_time}_{config.start_timestamp.date().isoformat()}_{config.end_timestamp.date().isoformat()}.hive",
    )
    partition_by_ticker = pa.dataset.partitioning(
        pa.schema([(PARTITION_COLUMN_NAME, pa.string())]), flavor="hive"
    )
    pa_ds.write_dataset(
        agg_scanner,
        base_dir=by_ticker_base_dir,
        format="parquet",
        partitioning=partition_by_ticker,
        existing_data_behavior="overwrite_or_ignore",
        max_partitions=config.max_partitions,
        max_open_files=config.max_open_files,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--calendar_name", default="XNYS")

    parser.add_argument("--start_session", default="2014-06-16")
    parser.add_argument("--end_session", default="2024-09-06")
    # parser.add_argument("--start_session", default="2020-10-07")
    # parser.add_argument("--end_session", default="2020-10-15")
    # parser.add_argument("--aggs_pattern", default="2020/10/**/*.csv.gz")
    parser.add_argument("--aggs_pattern", default="**/*.csv.gz")

    parser.add_argument("--overwrite", action="store_true")

    # TODO: These defaults should be None but for dev convenience they are set for my local config.
    parser.add_argument("--agg_time", default="day")
    parser.add_argument("--data_dir", default="/Volumes/Oahu/Mirror/files.polygon.io")

    args = parser.parse_args()

    # Maybe the way to do this is to use the os.environ as the argparser defaults.
    environ = dict(os.environ.items())
    if args.data_dir:
        environ["POLYGON_DATA_DIR"] = args.data_dir
    if args.agg_time:
        environ["POLYGON_AGG_TIME"] = args.agg_time

    config = PolygonConfig(
        environ=os.environ,
        calendar_name=args.calendar_name,
        start_session=args.start_session,
        end_session=args.end_session,
    )

    concat_all_aggs_from_csv(
        config=config,
        aggs_pattern=args.aggs_pattern,
    )
