from .config import PolygonConfig, PARTITION_COLUMN_NAME, to_partition_key

import shutil
from typing import Iterator, Tuple, Union

import argparse
import os
import datetime

import pyarrow as pa
import pyarrow.compute as pa_compute
import pyarrow.csv as pa_csv
import pyarrow.dataset as pa_ds
import pyarrow.fs as pa_fs

import pandas as pd


# def get_by_ticker_dates(config: PolygonConfig, schema) -> set[datetime.date]:
#     file_info = config.filesystem.get_file_info(config.by_ticker_dir)
#     if file_info.type == pa_fs.FileType.NotFound:
#         return set()
#     partitioning = None
#     if PARTITION_COLUMN_NAME in schema.names:
#         partitioning = pa_ds.partitioning(
#             pa.schema([(PARTITION_COLUMN_NAME, pa.string())]), flavor="hive"
#         )
#     by_ticker_aggs_ds = pa_ds.dataset(config.by_ticker_aggs_arrow_dir, schema=schema, partitioning=partitioning)
#     return set(
#         [
#             pa_ds.get_partition_keys(fragment.partition_expression).get("date")
#             for fragment in by_ticker_aggs_ds.get_fragments()
#         ]
#     )


def generate_tables_from_csv_files(
    config: PolygonConfig,
    schema: pa.Schema,
    overwrite: bool = False,
) -> Iterator[pa.Table]:
    empty_table = schema.empty_table()
    # TODO: Find which column(s) need to be cast to int64 from the schema.
    empty_table = empty_table.set_column(
        empty_table.column_names.index("window_start"),
        "window_start",
        empty_table.column("window_start").cast(pa.int64()),
    )
    csv_schema = empty_table.schema

    existing_by_ticker_dates = set()
    if not overwrite:
        # print("Getting existing by_ticker_dates")
        # existing_by_ticker_dates = get_by_ticker_dates(config, schema)
        print(f"{len(existing_by_ticker_dates)=}")

    schedule = config.calendar.trading_index(
        start=config.start_timestamp, end=config.end_timestamp, period="1D"
    )
    start_timestamp = config.start_timestamp.tz_localize(config.calendar.tz.key)
    limit_timestamp = (config.end_timestamp + pd.Timedelta(days=1)).tz_localize(
        config.calendar.tz.key)
    # print(f"{start_timestamp=} {limit_timestamp=} {config.calendar.tz=} {schedule[:2]=} {schedule[-2:]=}")

    tables_read_count = 0
    skipped_table_count = 0
    for timestamp in schedule:
        date: datetime.date = timestamp.tz_localize(config.calendar.tz.key).to_pydatetime().date()
        # print(f"{date=} {timestamp=}")
        if date in existing_by_ticker_dates:
            continue
        csv_path = config.date_to_csv_file_path(date)
        convert_options = pa_csv.ConvertOptions(
            column_types=csv_schema,
            strings_can_be_null=False,
            quoted_strings_can_be_null=False,
        )

        table = pa_csv.read_csv(csv_path, convert_options=convert_options)
        tables_read_count += 1
        table = table.set_column(
            table.column_names.index("window_start"),
            "window_start",
            table.column("window_start").cast(schema.field("window_start").type),
        )
        if PARTITION_COLUMN_NAME in schema.names:
            table = table.append_column(
                PARTITION_COLUMN_NAME,
                pa.array(
                    [
                        to_partition_key(ticker)
                        for ticker in table.column("ticker").to_pylist()
                    ]
                ),
            )
        expr = (
            pa_compute.field("window_start")
            >= pa.scalar(start_timestamp, type=schema.field("window_start").type)
        ) & (
            pa_compute.field("window_start")
            < pa.scalar(
                limit_timestamp,
                type=schema.field("window_start").type,
            )
        )
        table = table.filter(expr)

        # TODO: Also check that these rows are within range for this file's date (not just the whole session).
        # And if we're doing that (figuring date for each file), we can just skip reading the file.
        # Might able to do a single comparison using compute.days_between.
        # https://arrow.apache.org/docs/python/generated/pyarrow.compute.days_between.html

        if table.num_rows == 0:
            skipped_table_count += 1
            continue

        if PARTITION_COLUMN_NAME in schema.names:
            print(f"{date=}")
        yield table
    print(f"{tables_read_count=} {skipped_table_count=}")


# def generate_tables_from_csv_files(
#     paths: Iterator[Union[str, os.PathLike]],
#     schema: pa.Schema,
#     start_timestamp: pd.Timestamp,
#     limit_timestamp: pd.Timestamp,
# ) -> Iterator[pa.Table]:
#     empty_table = schema.empty_table()
#     # TODO: Find which column(s) need to be cast to int64 from the schema.
#     empty_table = empty_table.set_column(
#         empty_table.column_names.index("window_start"),
#         "window_start",
#         empty_table.column("window_start").cast(pa.int64()),
#     )
#     csv_schema = empty_table.schema

#     tables_read_count = 0
#     skipped_table_count = 0
#     for path in paths:
#         convert_options = pa_csv.ConvertOptions(
#             column_types=csv_schema,
#             strings_can_be_null=False,
#             quoted_strings_can_be_null=False,
#         )

#         table = pa_csv.read_csv(path, convert_options=convert_options)
#         tables_read_count += 1
#         table = table.set_column(
#             table.column_names.index("window_start"),
#             "window_start",
#             table.column("window_start").cast(schema.field("window_start").type),
#         )
#         if PARTITION_COLUMN_NAME in schema.names:
#             table = table.append_column(
#                 PARTITION_COLUMN_NAME,
#                 pa.array(
#                     [
#                         to_partition_key(ticker)
#                         for ticker in table.column("ticker").to_pylist()
#                     ]
#                 ),
#             )
#         expr = (
#             pa_compute.field("window_start")
#             >= pa.scalar(start_timestamp, type=schema.field("window_start").type)
#         ) & (
#             pa_compute.field("window_start")
#             < pa.scalar(
#                 limit_timestamp,
#                 type=schema.field("window_start").type,
#             )
#         )
#         table = table.filter(expr)

#         # TODO: Also check that these rows are within range for this file's date (not just the whole session).
#         # And if we're doing that (figuring date for each file), we can just skip reading the file.
#         # Might able to do a single comparison using compute.days_between.
#         # https://arrow.apache.org/docs/python/generated/pyarrow.compute.days_between.html

#         if table.num_rows == 0:
#             skipped_table_count += 1
#             continue

#         yield table
#     print(f"{tables_read_count=} {skipped_table_count=}")


def generate_csv_agg_tables(
    config: PolygonConfig, overwrite: bool = False
) -> Tuple[pa.Schema, Iterator[pa.Table]]:
    """zipline does bundle ingestion one ticker at a time."""

    # Polygon Aggregate flatfile timestamps are in nanoseconds (like trades), not milliseconds as the docs say.
    # I make the timestamp timezone-aware because that's how Unix timestamps work and it may help avoid mistakes.
    timestamp_type = pa.timestamp("ns", tz='UTC')

    # But we can't use the timestamp type in the schema here because it's not supported by the CSV reader.
    # So we'll use int64 and cast it after reading the CSV file.
    # https://github.com/apache/arrow/issues/44030

    # strptime(3) (used by CSV reader for timestamps in ConvertOptions.timestamp_parsers) supports Unix timestamps (%s) and milliseconds (%f) but not nanoseconds.
    # https://www.geeksforgeeks.org/how-to-use-strptime-with-milliseconds-in-python/
    # Actually that's the wrong strptime (it's Python's).  C++ strptime(3) doesn't even support %f.
    # https://github.com/apache/arrow/issues/39839#issuecomment-1915981816
    # Also I don't think you can use those in a format string without a separator.

    # Polygon price scale is 4 decimal places (i.e. hundredths of a penny), but we'll use 10 because we have precision to spare.
    # price_type = pa.decimal128(precision=38, scale=10)
    # 64bit float a little overkill but avoids any plausible truncation error.
    price_type = pa.float64()

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
    if config.agg_time == "minute":
        polygon_aggs_schema = polygon_aggs_schema.append(
            pa.field(PARTITION_COLUMN_NAME, pa.string(), nullable=False)
        )

    return (
        polygon_aggs_schema,
        generate_tables_from_csv_files(
            config,
            schema=polygon_aggs_schema,
            overwrite=overwrite,
        ),
    )


def generate_batches_from_tables(tables):
    for table in tables:
        for batch in table.to_batches():
            yield batch


def concat_all_aggs_from_csv(
    config: PolygonConfig,
    overwrite: bool = False,
) -> str:
    schema, tables = generate_csv_agg_tables(config, overwrite=overwrite)

    by_ticker_aggs_arrow_dir = config.by_ticker_aggs_arrow_dir
    # if os.path.exists(by_ticker_aggs_arrow_dir):
    #     if overwrite:
    #         print(f"Removing {by_ticker_aggs_arrow_dir=}")
    #         shutil.rmtree(by_ticker_aggs_arrow_dir)
    #     else:
    #         # TODO: Validate the existing data.
    #         print(f"Found existing {by_ticker_aggs_arrow_dir=}")
    #         return by_ticker_aggs_arrow_dir

    partitioning = None
    if PARTITION_COLUMN_NAME in schema.names:
        partitioning = pa_ds.partitioning(
            pa.schema([(PARTITION_COLUMN_NAME, pa.string())]), flavor="hive"
        )

    print(f"Scattering aggregates by ticker to {by_ticker_aggs_arrow_dir=}")
    pa_ds.write_dataset(
        generate_batches_from_tables(tables),
        schema=schema,
        base_dir=by_ticker_aggs_arrow_dir,
        partitioning=partitioning,
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
    )
    print(f"Scattered aggregates by ticker to {by_ticker_aggs_arrow_dir=}")
    return by_ticker_aggs_arrow_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--calendar_name", default="XNYS")

    parser.add_argument("--start_date", default="2014-06-16")
    parser.add_argument("--end_date", default="2024-09-06")
    # parser.add_argument("--start_date", default="2020-01-01")
    # parser.add_argument("--end_date", default="2020-12-31")

    parser.add_argument("--agg_time", default="day")

    parser.add_argument("--overwrite", action="store_true")

    # TODO: These defaults should be None but for dev convenience they are set for my local config.
    parser.add_argument("--data_dir", default="/Volumes/Oahu/Mirror/files.polygon.io")
    # parser.add_argument("--aggs_pattern", default="**/*.csv.gz")
    # parser.add_argument("--aggs_pattern", default="2020/10/**/*.csv.gz")

    args = parser.parse_args()

    # Maybe the way to do this is to use the os.environ as the argparser defaults.
    if args.data_dir:
        os.environ["POLYGON_DATA_DIR"] = args.data_dir

    config = PolygonConfig(
        environ=os.environ,
        calendar_name=args.calendar_name,
        start_date=args.start_date,
        end_date=args.end_date,
        agg_time=args.agg_time,
    )

    concat_all_aggs_from_csv(
        config=config,
        overwrite=args.overwrite,
    )
