from .config import PolygonConfig, PARTITION_COLUMN_NAME, to_partition_key

from typing import Iterator, Tuple

import pyarrow as pa
import pyarrow.compute as pa_compute
import pyarrow.csv as pa_csv
import pyarrow.dataset as pa_ds
import pyarrow.fs as pa_fs

from fsspec.implementations.arrow import ArrowFSWrapper

import os
import datetime

import numpy as np
import pandas as pd


# Polygon Trade Conditions codes that don't reflect a current market-priced trade.
# https://polygon.io/docs/rest/stocks/market-operations/condition-codes
# Odd lots are excluded because although their volume counts the prices don't.
EXCLUDED_CONDITION_CODES = {
    # 2,   # Average Price
    # 7,   # Cash Sale
    10,  # Derivatively Priced
    # 12,  # Form T / Extended Hours
    13,  # Extended Hours (Sold Out Of Sequence)
    # 15,  # Official Close
    # 16,  # Official Open
    20,  # Next Day
    21,  # Price Variation
    # 22,  # Prior Reference
    29,  # Seller
    32,  # Sold (Out of Sequence)
    # 33,  # Sold + Stopped
    41,  # Trade Thru Exempt
    52,  # Contingent Trade
    53   # Qualified Contingent Trade
}


def trades_schema(raw: bool = False) -> pa.Schema:
    # There is some problem reading the timestamps as timestamps so we have to read as integer then change the schema.
    # Polygon Aggregate flatfile timestamps are in nanoseconds (like trades), not milliseconds as the docs say.
    # I make the timestamp timezone-aware because that's how Unix timestamps work and it may help avoid mistakes.
    # The timezone is America/New_York because that's the US exchanges timezone and the date is a trading day.
    # timestamp_type = pa.timestamp("ns", tz="America/New_York")
    # timestamp_type = pa.int64() if raw else pa.timestamp("ns", tz=tz)
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

    return pa_ds.FileSystemDataset.from_paths(
        paths,
        format=pa_ds.CsvFileFormat(),
        schema=trades_schema(raw=True),
        filesystem=config.filesystem,
    )


def cast_strings_to_list(
    string_array, separator=",", default="0", value_type=pa.uint8()
):
    """Cast a PyArrow StringArray of comma-separated numbers to a ListArray of values."""

    # Create a mask to identify empty strings
    is_empty = pa_compute.equal(pa_compute.utf8_trim_whitespace(string_array), "")

    # Use replace_with_mask to replace empty strings with the default ("0")
    filled_column = pa_compute.replace_with_mask(
        string_array, is_empty, pa.scalar(default)
    )

    # Split the strings by comma
    split_array = pa_compute.split_pattern(filled_column, pattern=separator)

    # Cast each element in the resulting lists to integers
    return pa_compute.cast(split_array, pa.list_(value_type))


def ordinary_trades_mask(table: pa.Table) -> pa.BooleanArray:
    conditions_dict = table["conditions"].combine_chunks().dictionary_encode()
    list_of_codes = cast_strings_to_list(conditions_dict.dictionary).to_pylist()
    code_dictionary = pa.array(set(codes).isdisjoint(EXCLUDED_CONDITION_CODES) for codes in list_of_codes)
    include_mask = pa.DictionaryArray.from_arrays(conditions_dict.indices, code_dictionary).dictionary_decode()
    return pa_compute.and_(include_mask, pa_compute.equal(table["correction"], "0"))


def cast_trades(trades) -> pa.Table:
    trades = trades.cast(trades_schema())
    condition_values = cast_strings_to_list(
        trades.column("conditions").combine_chunks()
    )
    return trades.append_column("condition_values", condition_values)


def custom_aggs_schema(raw: bool = False) -> pa.Schema:
    # timestamp_type = pa.int64() if raw else pa.timestamp("ns", tz=tz)
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
            pa.field("vwap", price_type, nullable=False),
            pa.field("traded_value", price_type, nullable=False),
            pa.field("cumulative_traded_value", price_type, nullable=False),
            pa.field("date", pa.date32(), nullable=False),
            pa.field("year", pa.uint16(), nullable=False),
            pa.field("month", pa.uint8(), nullable=False),
            pa.field(PARTITION_COLUMN_NAME, pa.string(), nullable=False),
        ]
    )


def custom_aggs_partitioning() -> pa.Schema:
    return pa_ds.partitioning(
        pa.schema(
            [("year", pa.uint16()), ("month", pa.uint8()), ("date", pa.date32())]
        ),
        flavor="hive",
    )


def get_aggs_dates(config: PolygonConfig) -> set[datetime.date]:
    file_info = config.filesystem.get_file_info(config.aggs_dir)
    if file_info.type == pa_fs.FileType.NotFound:
        return set()
    aggs_ds = pa_ds.dataset(
        config.aggs_dir,
        format="parquet",
        schema=custom_aggs_schema(),
        partitioning=custom_aggs_partitioning(),
    )
    return set(
        [
            pa_ds.get_partition_keys(fragment.partition_expression).get("date")
            for fragment in aggs_ds.get_fragments()
        ]
    )


def generate_csv_trades_tables(
    config: PolygonConfig, overwrite: bool = False
) -> Iterator[Tuple[datetime.date, pa.Table]]:
    """Generator for trades tables from flatfile CSVs."""
    existing_aggs_dates = set()
    if not overwrite:
        existing_aggs_dates = get_aggs_dates(config)
    schedule = config.calendar.trading_index(
        start=config.start_timestamp, end=config.end_timestamp, period="1D"
    )
    for timestamp in schedule:
        date: datetime.date = timestamp.to_pydatetime().date()
        if date in existing_aggs_dates:
            continue
        trades_csv_path = config.date_to_csv_file_path(date)
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


def trades_to_custom_aggs(
    config: PolygonConfig,
    date: datetime.date,
    table: pa.Table,
) -> pa.Table:
    print(f"{date=} {pa.default_memory_pool()=}")
    table = table.append_column(
        "traded_value", pa_compute.multiply(table["price"], table["size"])
    )
    table = table.append_column(
        "window_start",
        pa_compute.floor_temporal(
            table["sip_timestamp"], multiple=config.agg_timedelta.seconds, unit="second"
        ),
    )
    table = table.sort_by([("ticker", "ascending"), ("sip_timestamp", "ascending")])
    table = table.group_by(["ticker", "window_start"], use_threads=False).aggregate(
        [
            ("price", "first"),
            ("price", "max"),
            ("price", "min"),
            ("price", "last"),
            ("traded_value", "sum"),
            ("size", "sum"),
            ([], "count_all"),
        ]
    )
    table = table.rename_columns(
        {
            "price_first": "open",
            "price_max": "high",
            "price_min": "low",
            "price_last": "close",
            "size_sum": "volume",
            "traded_value_sum": "traded_value",
            "count_all": "transactions",
        }
    )
    table = table.sort_by([("ticker", "ascending"), ("window_start", "ascending")])
    table = table.append_column(
        "vwap", pa_compute.divide(table["traded_value"], table["volume"])
    )
    # Calculate cumulative traded value by ticker
    traded_values_by_ticker = table.group_by("ticker").aggregate([("traded_value", "list")])
    cumulative_sum_arrays = [
        pa_compute.cumulative_sum(pa.array(values_list)) for values_list in traded_values_by_ticker["traded_value_list"].combine_chunks()
    ]
    table = table.append_column("cumulative_traded_value", pa.concat_arrays(cumulative_sum_arrays))
    
    # table.append_column('date', pa.array([date] * len(table), type=pa.date32()))
    # table.append_column('year', pa.array([date.year] * len(table), type=pa.uint16()))
    # table.append_column('month', pa.array([date.month] * len(table), type=pa.uint8()))
    table = table.append_column("date", pa.array(np.full(len(table), date)))
    table = table.append_column(
        "year", pa.array(np.full(len(table), date.year), type=pa.uint16())
    )
    table = table.append_column(
        "month", pa.array(np.full(len(table), date.month), type=pa.uint8())
    )
    table = table.append_column(
        PARTITION_COLUMN_NAME,
        pa.array(
            [to_partition_key(ticker) for ticker in table.column("ticker").to_pylist()]
        ),
    )
    # print(f"aggs {date=} {table.to_pandas().head()=}")
    return table


# def generate_custom_agg_batches_from_tables(config: PolygonConfig):
#     for date, trades_table in generate_csv_trades_tables(config):
#         aggs_table = trades_to_custom_aggs(config, date, trades_table)
#         yield aggs_table
#         del aggs_table
#         del trades_table


def file_visitor(written_file):
    print(f"{written_file.path=}")


def convert_trades_to_custom_aggs(
    config: PolygonConfig, overwrite: bool = False
) -> str:
    if overwrite:
        print("WARNING: overwrite not implemented/ignored.")

    # MAX_FILES_OPEN = 8
    # MIN_ROWS_PER_GROUP = 100_000

    print(f"{config.aggs_dir=}")

    # pa.set_memory_pool()

    for date, trades_table in generate_csv_trades_tables(config):
        pa_ds.write_dataset(
            trades_to_custom_aggs(config,
                                  date,
                                  trades_table.filter(ordinary_trades_mask(trades_table))),
            filesystem=config.filesystem,
            base_dir=config.aggs_dir,
            partitioning=custom_aggs_partitioning(),
            format="parquet",
            existing_data_behavior="overwrite_or_ignore",
            file_visitor=file_visitor,
            # max_open_files=10,
            # min_rows_per_group=MIN_ROWS_PER_GROUP,
        )
        # pa_ds.write_dataset(
        #     trades_to_custom_events(config,
        #                             date,
        #                             trades_table.filter(pa_compute.invert(ordinary_trades_mask(trades_table)))),
        #     filesystem=config.filesystem,
        #     base_dir=config.events_dir,
        #     partitioning=custom_events_partitioning(),
        #     format="parquet",
        #     existing_data_behavior="overwrite_or_ignore",
        #     file_visitor=file_visitor,
        # )
        del trades_table

    # with ProcessPoolExecutor(max_workers=1) as executor:
    #     executor.map(
    #         configure_write_custom_aggs_to_dataset(config),
    #         generate_csv_trades_tables(config),
    #     )

    print(f"Generated aggregates to {config.aggs_dir=}")
    return config.aggs_dir


# https://github.com/twopirllc/pandas-ta/issues/731#issuecomment-1766786952

# def calculate_mfi(high, low, close, volume, period):
#     typical_price = (high + low + close) / 3
#     money_flow = typical_price * volume
#     mf_sign = np.where(typical_price > np.roll(typical_price, shift=1), 1, -1)
#     signed_mf = money_flow * mf_sign

#     # Calculate gain and loss using vectorized operations
#     positive_mf = np.maximum(signed_mf, 0)
#     negative_mf = np.maximum(-signed_mf, 0)

#     mf_avg_gain = np.convolve(positive_mf, np.ones(period), mode='full')[:len(positive_mf)] / period
#     mf_avg_loss = np.convolve(negative_mf, np.ones(period), mode='full')[:len(negative_mf)] / period

#     epsilon = 1e-10  # Small epsilon value to avoid division by zero
#     mfi = 100 - 100 / (1 + mf_avg_gain / (mf_avg_loss + epsilon))
#     return mfi


def get_by_ticker_aggs_dates(config: PolygonConfig) -> set[datetime.date]:
    file_info = config.filesystem.get_file_info(config.by_ticker_aggs_arrow_dir)
    if file_info.type == pa_fs.FileType.NotFound:
        return set()
    by_ticker_aggs_ds = pa_ds.dataset(
        config.by_ticker_aggs_arrow_dir,
        format="parquet",
        schema=custom_aggs_schema(),
        partitioning=custom_aggs_partitioning(),
    )
    return set(
        [
            pa_ds.get_partition_keys(fragment.partition_expression).get("date")
            for fragment in by_ticker_aggs_ds.get_fragments()
        ]
    )


def batches_for_date(aggs_ds: pa_ds.Dataset, date: pd.Timestamp):
    date_filter_expr = (
        (pa_compute.field("year") == date.year)
        & (pa_compute.field("month") == date.month)
        & (pa_compute.field("date") == date.date())
    )
    print(f"table for {date=}")
    # return aggs_ds.scanner(filter=date_filter_expr).to_batches()
    table = aggs_ds.scanner(filter=date_filter_expr).to_table()
    table = table.sort_by([("part", "ascending"), ("ticker", "ascending"), ("window_start", "ascending"), ])
    return table.to_batches()


def generate_batches_for_schedule(config, aggs_ds):
    schedule = config.calendar.trading_index(
        start=config.start_timestamp, end=config.end_timestamp, period="1D"
    )
    for timestamp in schedule:
        # print(f"{timestamp=}")
        yield from batches_for_date(aggs_ds=aggs_ds, date=timestamp)


# def scatter_custom_aggs_to_by_ticker(
#     config: PolygonConfig,
#     overwrite: bool = False,
# ) -> str:
#     lock = FileLock(config.lock_file_path, blocking=False)
#     with lock:
#         if not lock.is_locked:
#             raise IOError("Failed to acquire lock for updating custom assets.")
#         with open(config.by_ticker_dates_path, "a") as f:
#             f.write("I have a bad feeling about this.")
#             by_ticker_aggs_arrow_dir = scatter_custom_aggs_to_by_ticker_(config, overwrite)

#             print(f"Scattered custom aggregates by ticker to {by_ticker_aggs_arrow_dir=}")
#             return by_ticker_aggs_arrow_dir


def filter_by_date(config: PolygonConfig) -> pa_compute.Expression:
    start_date = config.start_timestamp.tz_localize(config.calendar.tz.key).date()
    limit_date = (
        (config.end_timestamp + pd.Timedelta(days=1))
        .tz_localize(config.calendar.tz.key)
        .date()
    )
    return (pa_compute.field("date") >= start_date) & (
        pa_compute.field("date") <= limit_date
    )


# def generate_batches_with_partition(
#     config: PolygonConfig,
#     aggs_ds: pa_ds.Dataset,
# ) -> Iterator[pa.Table]:
#     for fragment in aggs_ds.sort_by("date").get_fragments(
#         filter=filter_by_date(config),
#     ):
#         for batch in fragment.to_batches():
#             # batch = batch.append_column(
#             #     PARTITION_COLUMN_NAME,
#             #     pa.array(
#             #         [
#             #             to_partition_key(ticker)
#             #             for ticker in batch.column("ticker").to_pylist()
#             #         ]
#             #     ),
#             # )
#             yield batch.sort_by(
#                 [("ticker", "ascending"), ("window_start", "ascending")]
#             )
#             del batch
#         del fragment


def generate_batches_with_partition(
    config: PolygonConfig,
    aggs_ds: pa_ds.Dataset,
) -> Iterator[pa.Table]:
    for fragment in (
        aggs_ds.filter(filter_by_date(config))
        .sort_by([(PARTITION_COLUMN_NAME, "ascending"), ("date", "ascending")])
        .get_fragments()
    ):
        for batch in fragment.to_batches():
            yield batch.sort_by(
                [("ticker", "ascending"), ("window_start", "ascending")]
            )
            del batch
        del fragment


def scatter_custom_aggs_to_by_ticker(config, overwrite=False) -> str:
    aggs_ds = pa_ds.dataset(
        config.aggs_dir,
        format="parquet",
        schema=custom_aggs_schema(),
        partitioning=custom_aggs_partitioning(),
    )
    by_ticker_schema = aggs_ds.schema
    partitioning = pa_ds.partitioning(
        pa.schema([(PARTITION_COLUMN_NAME, pa.string())]),
        flavor="hive",
    )
    by_ticker_aggs_arrow_dir = config.by_ticker_aggs_arrow_dir
    print(f"Scattering custom aggregates by ticker to {by_ticker_aggs_arrow_dir=}")
    pa_ds.write_dataset(
        # generate_batches_with_partition(config=config, aggs_ds=aggs_ds),
        generate_batches_for_schedule(config=config, aggs_ds=aggs_ds),
        schema=by_ticker_schema,
        base_dir=by_ticker_aggs_arrow_dir,
        partitioning=partitioning,
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
    )
    print(f"Scattered aggregates by ticker to {by_ticker_aggs_arrow_dir=}")
    return by_ticker_aggs_arrow_dir


# def scatter_custom_aggs_to_by_ticker(config, overwrite=False) -> str:
#     file_info = config.filesystem.get_file_info(config.aggs_dir)
#     if file_info.type == pa_fs.FileType.NotFound:
#         raise FileNotFoundError(f"{config.aggs_dir=} not found.")

#     by_ticker_aggs_arrow_dir = config.by_ticker_aggs_arrow_dir
#     if os.path.exists(by_ticker_aggs_arrow_dir):
#         if overwrite:
#             print(f"Removing {by_ticker_aggs_arrow_dir=}")
#             shutil.rmtree(by_ticker_aggs_arrow_dir)

#     schedule = config.calendar.trading_index(
#         start=config.start_timestamp, end=config.end_timestamp, period="1D"
#     )
#     assert type(schedule) is pd.DatetimeIndex

#     print(f"Scattering custom aggregates by ticker to {by_ticker_aggs_arrow_dir=}")
#     aggs_ds = pa_ds.dataset(
#         config.aggs_dir,
#         format="parquet",
#         schema=custom_aggs_schema(),
#         partitioning=custom_aggs_partitioning(),
#     )
#     by_ticker_partitioning = pa_ds.partitioning(
#         pa.schema([(PARTITION_COLUMN_NAME, pa.string())]),
#         # pa.schema(
#         #     [
#         #         (PARTITION_COLUMN_NAME, pa.string()),
#         #         ("year", pa.uint16()),
#         #         ("month", pa.uint8()),
#         #         ("date", pa.date32()),
#         #     ]
#         # ),
#         flavor="hive",
#     )
#     by_ticker_schema = custom_aggs_schema()
#     by_ticker_schema = by_ticker_schema.append(
#         pa.field(PARTITION_COLUMN_NAME, pa.string(), nullable=False),
#     )

#     # TODO: Collect the dates we've scattered and write a special partition key with them.
#     pa_ds.write_dataset(
#         generate_batches_for_schedule(schedule, aggs_ds),
#         schema=by_ticker_schema,
#         base_dir=by_ticker_aggs_arrow_dir,
#         partitioning=by_ticker_partitioning,
#         format="parquet",
#         existing_data_behavior="overwrite_or_ignore",
#         # max_open_files=250,
#         # file_visitor=file_visitor,
#     )

#     return by_ticker_aggs_arrow_dir


# def generate_tables_from_custom_aggs_ds(
#     aggs_ds: pa_ds.Dataset, schedule: pd.DatetimeIndex
# ):
#     for timestamp in schedule:
#         yield table_for_date(aggs_ds=aggs_ds, date=timestamp.to_pydatetime().date())
