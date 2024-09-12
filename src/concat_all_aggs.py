import shutil
from typing import Iterator
from config import PolygonConfig

import argparse
import glob
import os

import pyarrow as pa
from pyarrow import dataset as pa_ds
from pyarrow import csv as pa_csv

import pandas as pd


def csv_agg_scanner(
    paths: list,
    schema: pa.Schema,
) -> Iterator[pa.RecordBatch]:
    empty_table = schema.empty_table()
    # TODO: Find which column(s) need to be cast to int64 from the schema.
    empty_table = empty_table.set_column(
        empty_table.column_names.index("window_start"),
        "window_start",
        empty_table.column("window_start").cast(pa.int64()),
    )
    csv_schema = empty_table.schema

    skipped_tables = 0
    for path in paths:
        convert_options = pa_csv.ConvertOptions(
            column_types=csv_schema,
            strings_can_be_null=False,
            quoted_strings_can_be_null=False,
        )

        table = pa.csv.read_csv(path, convert_options=convert_options)
        table = table.set_column(
            table.column_names.index("window_start"),
            "window_start",
            table.column("window_start").cast(schema.field("window_start").type),
        )
        expr = (
            pa.compute.field("window_start")
            >= pa.scalar(config.start_timestamp, type=schema.field("window_start").type)
        ) & (
            pa.compute.field("window_start")
            < pa.scalar(
                config.end_timestamp + pd.to_timedelta(1, unit="day"),
                type=schema.field("window_start").type,
            )
        )
        table = table.filter(expr)

        # TODO: Also check that these rows are within range for this file's date (not just the whole session).
        # And if we're doing that (figuring date for each file), we can just skip reading the file.
        # Might able to do a single comparison using compute.days_between.
        # https://arrow.apache.org/docs/python/generated/pyarrow.compute.days_between.html

        if table.num_rows == 0:
            skipped_tables += 1
            continue

        print(f"{path=}")
        for batch in table.to_batches():
            yield batch
    print(f"{skipped_tables=}")


def concat_all_aggs_from_csv(
    config: PolygonConfig,
    aggs_pattern: str = "**/*.csv.gz",
    overwrite: bool = False,
) -> list:
    """zipline does bundle ingestion one ticker at a time."""
    if os.path.exists(config.by_ticker_hive_dir):
        if overwrite:
            print(f"Removing {config.by_ticker_hive_dir=}")
            shutil.rmtree(config.by_ticker_hive_dir)
        else:
            raise FileExistsError(f"{config.by_ticker_hive_dir=} exists and overwrite is False.")

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
            pa.field("window_start", timestamp_type, nullable=False),
            pa.field("transactions", pa.int64(), nullable=False),
        ]
    )

    agg_scanner = pa_ds.Scanner.from_batches(
        csv_agg_scanner(paths=paths, schema=polygon_aggs_schema),
        schema=polygon_aggs_schema,
    )

    pa_ds.write_dataset(
        agg_scanner,
        base_dir=config.by_ticker_hive_dir,
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
    )
    print(f"Concatenated aggregates to {config.by_ticker_hive_dir=}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--calendar_name", default="XNYS")

    parser.add_argument("--start_session", default="2014-06-16")
    parser.add_argument("--end_session", default="2024-09-06")
    # parser.add_argument("--start_session", default="2020-01-01")
    # parser.add_argument("--end_session", default="2020-12-31")
    # parser.add_argument("--aggs_pattern", default="2020/10/**/*.csv.gz")
    parser.add_argument("--aggs_pattern", default="**/*.csv.gz")

    parser.add_argument("--overwrite", action="store_true")

    # TODO: These defaults should be None but for dev convenience they are set for my local config.
    parser.add_argument("--agg_time", default="day")
    parser.add_argument("--data_dir", default="/Volumes/Oahu/Mirror/files.polygon.io")

    args = parser.parse_args()

    # Maybe the way to do this is to use the os.environ as the argparser defaults.
    if args.data_dir:
        os.environ["POLYGON_DATA_DIR"] = args.data_dir
    if args.agg_time:
        os.environ["POLYGON_AGG_TIME"] = args.agg_time

    config = PolygonConfig(
        environ=os.environ,
        calendar_name=args.calendar_name,
        start_session=args.start_session,
        end_session=args.end_session,
    )

    concat_all_aggs_from_csv(
        config=config,
        aggs_pattern=args.aggs_pattern,
        overwrite=args.overwrite,
    )
