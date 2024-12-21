from .config import PolygonConfig

from typing import Iterator, Tuple

import pandas as pd

import pyarrow as pa
from pyarrow import dataset as pa_ds
from pyarrow import compute as pa_compute
from pyarrow import parquet as pa_parquet
from pyarrow import csv as pa_csv

from fsspec.implementations.arrow import ArrowFSWrapper

import os
import resource

import datetime
import pandas_market_calendars

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor


def trades_schema(raw: bool = False) -> pa.Schema:
    # There is some problem reading the timestamps as timestamps so we have to read as integer then change the schema.
    # Polygon Aggregate flatfile timestamps are in nanoseconds (like trades), not milliseconds as the docs say.
    # I make the timestamp timezone-aware because that's how Unix timestamps work and it may help avoid mistakes.
    # timestamp_type = pa.timestamp("ns", tz="UTC")
    timestamp_type = pa.int64() if raw else pa.timestamp("ns", tz="UTC")

    # Polygon price scale is 4 decimal places (i.e. hundredths of a penny), but we'll use 10 because we have precision to spare.
    # price_type = pa.decimal128(precision=38, scale=10)
    # 64bit float a little overkill but avoids any plausible truncation error.
    price_type = pa.float64()

    return pa.schema(
            [
                pa.field("ticker", pa.string(), nullable=False),
                pa.field("conditions", pa.string(), nullable=False),
                pa.field("correction", pa.string(), nullable=False),
                pa.field("exchange", pa.int8(), nullable=False),
                pa.field("id", pa.string(), nullable=False),
                pa.field("participant_timestamp", timestamp_type, nullable=False),
                pa.field("price", price_type, nullable=False),
                pa.field("sequence_number", pa.int64(), nullable=False),
                pa.field("sip_timestamp", timestamp_type, nullable=False),
                pa.field("size", pa.int64(), nullable=False),
                pa.field("tape", pa.int8(), nullable=False),
                pa.field("trf_id", pa.int64(), nullable=False),
                pa.field("trf_timestamp", timestamp_type, nullable=False),
            ]
        )


def trades_dataset(config: PolygonConfig) -> pa_ds.Dataset:
    """
    Create a pyarrow dataset from the trades files.
    """

    # https://arrow.apache.org/docs/python/filesystems.html#using-arrow-filesystems-with-fsspec
    # https://filesystem-spec.readthedocs.io/en/latest/_modules/fsspec/spec.html#AbstractFileSystem.glob.
    fsspec = ArrowFSWrapper(config.filesystem)

    # We sort by path because they have the year and month in the dir names and the date in the filename.
    paths = sorted(
        fsspec.glob(os.path.join(config.trades_dir, config.csv_paths_pattern))
    )

    return pa_ds.FileSystemDataset.from_paths(paths,
                                              format=pa_ds.CsvFileFormat(),
                                              schema=trades_schema(raw=True),
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


def cast_trades(trades):
    trades = trades.cast(trades_schema())
    condition_values = cast_strings_to_list(trades.column("conditions").combine_chunks())
    return trades.append_column('condition_values', condition_values)


def date_to_path(date, ext=".csv.gz"):
    # return f"{date.year}/{date.month:02}/{date.isoformat()}{ext}"
    return date.strftime("%Y/%m/%Y-%m-%d") + ext


def convert_to_custom_aggs_file(config: PolygonConfig,
                                overwrite: bool,
                                timestamp: pd.Timestamp,
                                start_session: pd.Timestamp,
                                end_session: pd.Timestamp):
    date = timestamp.to_pydatetime().date()
    aggs_date_path = date_to_path(date, ext=".parquet")
    aggs_path = f"{config.custom_aggs_dir}/{aggs_date_path}"
    # aggs_by_ticker_path = f"{config.custom_aggs_by_ticker_dir}/{aggs_date_path}"
    fsspec = ArrowFSWrapper(config.filesystem)
    if fsspec.exists(aggs_path) or fsspec.exists(aggs_by_ticker_path):
        if overwrite:
            if fsspec.exists(aggs_path):
                config.filesystem.delete_file(aggs_path)
            if fsspec.exists(aggs_by_ticker_path):
                config.filesystem.delete_file(aggs_by_ticker_path)
        else:
            if fsspec.exists(aggs_path):
                print(f"SKIPPING: {date=} File exists {aggs_path=}")
            if fsspec.exists(aggs_by_ticker_path):
                print(f"SKIPPING: {date=} File exists {aggs_by_ticker_path=}")
            return
    fsspec.mkdir(fsspec._parent(aggs_path))
    fsspec.mkdir(fsspec._parent(aggs_by_ticker_path))
    trades_path = f"{config.trades_dir}/{date_to_path(date)}"
    if not fsspec.exists(trades_path):
        print(f"ERROR: Trades file missing.  Skipping {date=}.  {trades_path=}")
        return
    print(f"{trades_path=}")
    format = pa_ds.CsvFileFormat()
    trades_ds = pa_ds.FileSystemDataset.from_paths([trades_path], format=format, schema=trades_schema(raw=True), filesystem=config.filesystem)
    fragments = trades_ds.get_fragments()
    fragment = next(fragments)
    try:
        next(fragments)
        print("ERROR: More than one fragment for {path=}")
    except StopIteration:
        pass
    trades = fragment.to_table(schema=trades_ds.schema)
    trades = trades.cast(trades_schema())
    min_timestamp = pa.compute.min(trades.column('sip_timestamp')).as_py()
    max_timestamp = pa.compute.max(trades.column('sip_timestamp')).as_py()
    if min_timestamp < start_session:
        print(f"ERROR: {min_timestamp=} < {start_session=}")
    if max_timestamp >= end_session:
        print(f"ERROR: {max_timestamp=} >= {end_session=}")
    trades_df = trades.to_pandas()
    trades_df["window_start"] = trades_df["sip_timestamp"].dt.floor(aggregate_timedelta)
    aggs_df = trades_df.groupby(["ticker", "window_start"]).agg(
        open=('price', 'first'),
        high=('price', 'max'),
        low=('price', 'min'),
        close=('price', 'last'),
        volume=('size', 'sum'),
    )
    aggs_df['transactions'] = trades_df.groupby(["ticker", "window_start"]).size()
    aggs_df.reset_index(inplace=True)
    aggs_table = pa.Table.from_pandas(aggs_df).select(['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions'])
    aggs_table = aggs_table.sort_by([('ticker', 'ascending'), ('window_start', 'ascending')])
    print(f"{aggs_by_ticker_path=}")
    pa_parquet.write_table(table=aggs_table,
                           where=aggs_by_ticker_path, filesystem=to_config.filesystem) 
    aggs_table = aggs_table.sort_by([('window_start', 'ascending'), ('ticker', 'ascending')])
    print(f"{aggs_path=}")
    pa_parquet.write_table(table=aggs_table,
                           where=aggs_path, filesystem=to_config.filesystem)


# def convert_to_custom_aggs(config: PolygonConfig,
#                             overwrite: bool,
#                             timestamp: pd.Timestamp,
#                             start_session: pd.Timestamp,
#                             end_session: pd.Timestamp):
#     date = timestamp.to_pydatetime().date()
#     aggs_date_path = date_to_path(date, ext=".parquet")
#     aggs_path = f"{config.custom_aggs_dir}/{aggs_date_path}"
#     # aggs_by_ticker_path = f"{config.custom_aggs_by_ticker_dir}/{aggs_date_path}"
#     fsspec = ArrowFSWrapper(config.filesystem)
#     if fsspec.exists(aggs_path) or fsspec.exists(aggs_by_ticker_path):
#         if overwrite:
#             if fsspec.exists(aggs_path):
#                 config.filesystem.delete_file(aggs_path)
#             if fsspec.exists(aggs_by_ticker_path):
#                 config.filesystem.delete_file(aggs_by_ticker_path)
#         else:
#             if fsspec.exists(aggs_path):
#                 print(f"SKIPPING: {date=} File exists {aggs_path=}")
#             if fsspec.exists(aggs_by_ticker_path):
#                 print(f"SKIPPING: {date=} File exists {aggs_by_ticker_path=}")
#             return
#     fsspec.mkdir(fsspec._parent(aggs_path))
#     fsspec.mkdir(fsspec._parent(aggs_by_ticker_path))
#     trades_path = f"{config.trades_dir}/{date_to_path(date)}"
#     if not fsspec.exists(trades_path):
#         print(f"ERROR: Trades file missing.  Skipping {date=}.  {trades_path=}")
#         return
#     print(f"{trades_path=}")
#     format = pa_ds.CsvFileFormat()
#     trades_ds = pa_ds.FileSystemDataset.from_paths([trades_path], format=format, schema=trades_schema(raw=True), filesystem=config.filesystem)
#     fragments = trades_ds.get_fragments()
#     fragment = next(fragments)
#     try:
#         next(fragments)
#         print("ERROR: More than one fragment for {path=}")
#     except StopIteration:
#         pass
#     trades = fragment.to_table(schema=trades_ds.schema)
#     trades = trades.cast(trades_schema())
#     min_timestamp = pa.compute.min(trades.column('sip_timestamp')).as_py()
#     max_timestamp = pa.compute.max(trades.column('sip_timestamp')).as_py()
#     if min_timestamp < start_session:
#         print(f"ERROR: {min_timestamp=} < {start_session=}")
#     if max_timestamp >= end_session:
#         print(f"ERROR: {max_timestamp=} >= {end_session=}")
#     trades_df = trades.to_pandas()
#     trades_df["window_start"] = trades_df["sip_timestamp"].dt.floor(aggregate_timedelta)
#     aggs_df = trades_df.groupby(["ticker", "window_start"]).agg(
#         open=('price', 'first'),
#         high=('price', 'max'),
#         low=('price', 'min'),
#         close=('price', 'last'),
#         volume=('size', 'sum'),
#     )
#     aggs_df['transactions'] = trades_df.groupby(["ticker", "window_start"]).size()
#     aggs_df.reset_index(inplace=True)
#     aggs_table = pa.Table.from_pandas(aggs_df).select(['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions'])
#     aggs_table = aggs_table.sort_by([('ticker', 'ascending'), ('window_start', 'ascending')])
#     print(f"{aggs_by_ticker_path=}")
#     pa_parquet.write_table(table=aggs_table,
#                            where=aggs_by_ticker_path, filesystem=to_config.filesystem) 
#     aggs_table = aggs_table.sort_by([('window_start', 'ascending'), ('ticker', 'ascending')])
#     print(f"{aggs_path=}")
#     pa_parquet.write_table(table=aggs_table,
#                            where=aggs_path, filesystem=to_config.filesystem) 
#     pa_ds.write_dataset(
#         generate_batches_from_tables(tables),
#         schema=schema,
#         base_dir=by_ticker_aggs_arrow_dir,
#         partitioning=partitioning,
#         format="parquet",
#         existing_data_behavior="overwrite_or_ignore",
#     )


# def generate_csv_trades_tables(
#     config: PolygonConfig,
# ) -> Tuple[datetime.date, Iterator[pa.Table]]:
#     """Generator for trades tables from flatfile CSVs."""
#     # Use pandas_market_calendars so we can get extended hours.
#     # NYSE and NASDAQ have extended hours but XNYS does not.
#     calendar = pandas_market_calendars.get_calendar(config.calendar_name)
#     schedule = calendar.schedule(start_date=config.start_timestamp, end_date=config.end_timestamp, start="pre", end="post")
#     for timestamp, session in schedule.iterrows():
#         date = timestamp.to_pydatetime().date()
#         trades_csv_path = f"{config.trades_dir}/{date_to_path(date)}"
#         format = pa_ds.CsvFileFormat()
#         trades_ds = pa_ds.FileSystemDataset.from_paths([trades_csv_path], format=format, schema=trades_schema(raw=True), filesystem=config.filesystem)
#         fragments = trades_ds.get_fragments()
#         fragment = next(fragments)
#         try:
#             next(fragments)
#             print("ERROR: More than one fragment for {path=}")
#         except StopIteration:
#             pass
#         trades = fragment.to_table(schema=trades_ds.schema)
#         trades = trades.cast(trades_schema())
#         min_timestamp = pa.compute.min(trades.column('sip_timestamp')).as_py()
#         max_timestamp = pa.compute.max(trades.column('sip_timestamp')).as_py()
#         start_session = session['pre']
#         end_session = session['post']
#         # print(f"{start_session=} {end_session=}")
#         # print(f"{min_timestamp=} {max_timestamp=}")
#         if min_timestamp < start_session:
#             print(f"ERROR: {min_timestamp=} < {start_session=}")
#         # The end_session is supposed to be a limit but there are many with trades at that second.
#         if max_timestamp >= (end_session + pd.Timedelta(seconds=1)):
#             # print(f"ERROR: {max_timestamp=} >= {end_session=}")
#             print(f"ERROR: {max_timestamp=} > {end_session+pd.Timedelta(seconds=1)=}")
#         yield date, trades
#         del fragment
#         del fragments
#         del trades_ds


def generate_csv_trades_tables(
    config: PolygonConfig,
) -> Tuple[datetime.date, Iterator[pa.Table]]:
    """Generator for trades tables from flatfile CSVs."""
    # Use pandas_market_calendars so we can get extended hours.
    # NYSE and NASDAQ have extended hours but XNYS does not.
    calendar = pandas_market_calendars.get_calendar(config.calendar_name)
    schedule = calendar.schedule(start_date=config.start_timestamp, end_date=config.end_timestamp, start="pre", end="post")
    for timestamp, session in schedule.iterrows():
        date = timestamp.to_pydatetime().date()
        trades_csv_path = f"{config.trades_dir}/{date_to_path(date)}"
        convert_options = pa_csv.ConvertOptions(column_types=trades_schema(raw=True))
        trades = pa_csv.read_csv(trades_csv_path, convert_options=convert_options)
        trades = trades.cast(trades_schema())
        # min_timestamp = pa.compute.min(trades.column('sip_timestamp')).as_py()
        # max_timestamp = pa.compute.max(trades.column('sip_timestamp')).as_py()
        # start_session = session['pre']
        # end_session = session['post']
        # # print(f"{start_session=} {end_session=}")
        # # print(f"{min_timestamp=} {max_timestamp=}")
        # if min_timestamp < start_session:
        #     print(f"ERROR: {min_timestamp=} < {start_session=}")
        # # The end_session is supposed to be a limit but there are many with trades at that second.
        # if max_timestamp >= (end_session + pd.Timedelta(seconds=1)):
        #     # print(f"ERROR: {max_timestamp=} >= {end_session=}")
        #     print(f"ERROR: {max_timestamp=} > {end_session+pd.Timedelta(seconds=1)=}")
        yield date, trades
        del trades


def trades_to_custom_aggs(config: PolygonConfig, date: datetime.date, table: pa.Table):
    print(f"{date=} {pa.total_allocated_bytes()=}")
    mp = pa.default_memory_pool()
    print(f"{mp.backend_name=} {mp=}")
    print(f"{resource.getrusage(resource.RUSAGE_SELF).ru_maxrss=}")
    table = table.filter(pa_compute.greater(table["size"], 0))
    table = table.filter(pa_compute.equal(table["correction"], "0"))
    table = table.append_column("window_start", 
                                pa_compute.floor_temporal(table["sip_timestamp"],
                                                          multiple=config.agg_timedelta.seconds, unit="second"))
    table = table.group_by(["ticker", "window_start"], use_threads=False).aggregate([
        ('price', 'first'), ('price', 'max'), ('price', 'min'), ('price', 'last'), ('size', 'sum'), ([], "count_all")
    ])
    table = table.rename_columns({
        'price_first': 'open',
        'price_max': 'high',
        'price_min': 'low',
        'price_last': 'close',
        'size_sum': 'volume',
        'count_all': 'transactions'})
    table.append_column('date', pa.array([date] * len(table), type=pa.date32()))
    table.append_column('year', pa.array([date.year] * len(table), type=pa.uint16()))
    table.append_column('month', pa.array([date.month] * len(table), type=pa.uint8()))
    table = table.sort_by([('window_start', 'ascending'), ('ticker', 'ascending')])
    return table
    # trades_df = table.to_pandas()
    # trades_df["window_start"] = trades_df["sip_timestamp"].dt.floor(config.agg_timedelta)
    # aggs_df = trades_df.groupby(["ticker", "window_start"]).agg(
    #     open=('price', 'first'),
    #     high=('price', 'max'),
    #     low=('price', 'min'),
    #     close=('price', 'last'),
    #     volume=('size', 'sum'),
    # )
    # aggs_df['transactions'] = trades_df.groupby(["ticker", "window_start"]).size()
    # aggs_df.reset_index(inplace=True)
    # aggs_df['date'] = date
    # aggs_df['year'] = date.year
    # aggs_df['month'] = date.month
    # aggs_table = pa.Table.from_pandas(aggs_df).select(
    #     ['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start', 'transactions', 'date', 'year', 'month']
    # )
    # del aggs_df
    # print(f"{aggs_table.schema=}")


def custom_aggs_schema(raw: bool = False) -> pa.Schema:
    timestamp_type = pa.int64() if raw else pa.timestamp("ns", tz="UTC")
    price_type = pa.float64()
    return pa.schema(
            [
                pa.field("ticker", pa.string(), nullable=False),
                pa.field("volume", pa.int64(), nullable=False),
                pa.field("open", price_type, nullable=False),
                pa.field("close", price_type, nullable=False),
                pa.field("high", price_type, nullable=False),
                pa.field("low", price_type, nullable=False),
                pa.field("window_start", timestamp_type, nullable=False),
                pa.field("transactions", pa.int64(), nullable=False),
                pa.field("date", pa.date32(), nullable=False),
                pa.field("year", pa.uint16(), nullable=False),
                pa.field("month", pa.uint8(), nullable=False),
            ]
        )


def custom_aggs_partitioning() -> pa.Schema:
    return pa_ds.partitioning(
        pa.schema([('year', pa.uint16()), ('month', pa.uint8()), ('date', pa.date32())]), flavor="hive"
    )


def generate_custom_agg_batches_from_tables(config: PolygonConfig) -> pa.RecordBatch:
    for date, trades_table in generate_csv_trades_tables(config):
        for batch in trades_to_custom_aggs(config, date, trades_table).to_batches():
            yield batch
        del trades_table


def generate_custom_agg_tables(config: PolygonConfig) -> pa.Table:
    for date, trades_table in generate_csv_trades_tables(config):
        yield trades_to_custom_aggs(config, date, trades_table)


def configure_write_custom_aggs_to_dataset(config: PolygonConfig):
    def write_custom_aggs_to_dataset(args: Tuple[datetime.date, pa.Table]):
        date, table = args
        pa_ds.write_dataset(
            trades_to_custom_aggs(config, date, table),
            filesystem=config.filesystem,
            base_dir=config.custom_aggs_dir,
            partitioning=custom_aggs_partitioning(),
            format="parquet",
            existing_data_behavior="overwrite_or_ignore",
        )
    return write_custom_aggs_to_dataset


def convert_all_to_custom_aggs(
    config: PolygonConfig, overwrite: bool = False
) -> str:
    if overwrite:
        print("WARNING: overwrite not implemented/ignored.")

    # MAX_FILES_OPEN = 8
    # MIN_ROWS_PER_GROUP = 100_000

    print(f"{config.custom_aggs_dir=}")

    # pa.set_memory_pool()

    # pa_ds.write_dataset(
    #     generate_custom_agg_batches_from_tables(config),
    #     schema=custom_aggs_schema(),
    #     filesystem=config.filesystem,
    #     base_dir=config.custom_aggs_dir,
    #     partitioning=custom_aggs_partitioning(),
    #     format="parquet",
    #     existing_data_behavior="overwrite_or_ignore",
    #     max_open_files = MAX_FILES_OPEN,
    #     min_rows_per_group = MIN_ROWS_PER_GROUP,
    # )

    for date, trades_table in generate_csv_trades_tables(config):
        aggs_table = trades_to_custom_aggs(config, date, trades_table)
        pa_ds.write_dataset(
            aggs_table,
            # schema=custom_aggs_schema(),
            filesystem=config.filesystem,
            base_dir=config.custom_aggs_dir,
            partitioning=custom_aggs_partitioning(),
            format="parquet",
            existing_data_behavior="overwrite_or_ignore",
            # max_open_files=MAX_FILES_OPEN,
            # min_rows_per_group=MIN_ROWS_PER_GROUP,
        )
        del aggs_table
        del trades_table

    # with ProcessPoolExecutor(max_workers=1) as executor:
    #     executor.map(
    #         configure_write_custom_aggs_to_dataset(config),
    #         generate_csv_trades_tables(config),
    #     )

    print(f"Generated aggregates to {config.custom_aggs_dir=}")
    return config.custom_aggs_dir