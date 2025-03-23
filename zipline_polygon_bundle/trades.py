from .config import PolygonConfig, PARTITION_COLUMN_NAME, to_partition_key

from typing import Iterator, Tuple

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.compute as pa_compute
import pyarrow.csv as pa_csv
import pyarrow.fs as pa_fs

from fsspec.implementations.arrow import ArrowFSWrapper

import os
import datetime
import shutil

import numpy as np
import pandas as pd
import pandas_ta as ta


def trades_schema(raw: bool = False, tz: str = "America/New York") -> pa.Schema:
    # There is some problem reading the timestamps as timestamps so we have to read as integer then change the schema.
    # Polygon Aggregate flatfile timestamps are in nanoseconds (like trades), not milliseconds as the docs say.
    # I make the timestamp timezone-aware because that's how Unix timestamps work and it may help avoid mistakes.
    # The timezone is America/New_York because that's the US exchanges timezone and the date is a trading day.
    # timestamp_type = pa.timestamp("ns", tz="America/New_York")
    timestamp_type = pa.int64() if raw else pa.timestamp("ns", tz=tz)

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
        schema=trades_schema(raw=True, tz=config.calendar.tz.key),
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
    int_list_array = pa_compute.cast(split_array, pa.list_(value_type))

    return int_list_array


def cast_trades(trades, tz: str = "America/New York") -> pa.Table:
    trades = trades.cast(trades_schema(tz=tz))
    condition_values = cast_strings_to_list(
        trades.column("conditions").combine_chunks()
    )
    return trades.append_column("condition_values", condition_values)


def date_to_path(date, ext=".csv.gz"):
    # return f"{date.year}/{date.month:02}/{date.isoformat()}{ext}"
    return date.strftime("%Y/%m/%Y-%m-%d") + ext


def custom_aggs_schema(raw: bool = False, tz: str = "America/New York") -> pa.Schema:
    timestamp_type = pa.int64() if raw else pa.timestamp("ns", tz=tz)
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
        pa.schema(
            [("year", pa.uint16()), ("month", pa.uint8()), ("date", pa.date32())]
        ),
        flavor="hive",
    )


def get_custom_aggs_dates(config: PolygonConfig) -> set[datetime.date]:
    file_info = config.filesystem.get_file_info(config.custom_aggs_dir)
    if file_info.type == pa_fs.FileType.NotFound:
        return set()
    aggs_ds = pa_ds.dataset(
        config.custom_aggs_dir,
        format="parquet",
        schema=custom_aggs_schema(tz=config.calendar.tz.key),
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
    custom_aggs_dates = set()
    if not overwrite:
        custom_aggs_dates = get_custom_aggs_dates(config)
    schedule = config.calendar.trading_index(
        start=config.start_timestamp, end=config.end_timestamp, period="1D"
    )
    for timestamp in schedule:
        date = timestamp.to_pydatetime().date()
        if date in custom_aggs_dates:
            continue
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


def trades_to_custom_aggs(
    config: PolygonConfig,
    date: datetime.date,
    table: pa.Table,
    include_trf: bool = False,
) -> pa.Table:
    print(f"{datetime.datetime.now()=} {date=} {pa.default_memory_pool()=}")
    # print(f"{resource.getrusage(resource.RUSAGE_SELF).ru_maxrss=}")
    table = table.filter(pa_compute.greater(table["size"], 0))
    table = table.filter(pa_compute.equal(table["correction"], "0"))
    if not include_trf:
        table = table.filter(pa_compute.not_equal(table["exchange"], 4))
    table = table.append_column(
        "price_total", pa_compute.multiply(table["price"], table["size"])
    )
    table = table.append_column(
        "window_start",
        pa_compute.floor_temporal(
            table["sip_timestamp"], multiple=config.agg_timedelta.seconds, unit="second"
        ),
    )
    table = table.group_by(["ticker", "window_start"], use_threads=False).aggregate(
        [
            ("price", "first"),
            ("price", "max"),
            ("price", "min"),
            ("price", "last"),
            ("price_total", "sum"),
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
            "price_total_sum": "total",
            "count_all": "transactions",
        }
    )
    table = table.append_column(
        "vwap", pa_compute.divide(table["total"], table["volume"])
    )
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
    table = table.sort_by([("window_start", "ascending"), ("ticker", "ascending")])
    return table


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
            filesystem=config.filesystem,
            base_dir=config.aggs_dir,
            partitioning=custom_aggs_partitioning(),
            format="parquet",
            existing_data_behavior="overwrite_or_ignore",
            file_visitor=file_visitor,
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


def table_for_date(aggs_ds: pa_ds.Dataset, date: datetime.date) -> pa.Table:
    date_filter_expr = (
        (pa_compute.field("year") == date.year)
        & (pa_compute.field("month") == date.month)
        & (pa_compute.field("date") == date)
    )
    print(f"{date=}")
    table = aggs_ds.to_table(filter=date_filter_expr)
    # TODO: Check that these rows are within range for this file's date (not just the whole session).
    # And if we're doing that (figuring date for each file), we can just skip reading the file.
    # Might able to do a single comparison using compute.days_between.
    # https://arrow.apache.org/docs/python/generated/pyarrow.compute.days_between.html
    table = table.append_column(
        PARTITION_COLUMN_NAME,
        pa.array(
            [
                to_partition_key(ticker)
                for ticker in table.column("ticker").to_pylist()
            ]
        ),
    )
    return table


def scatter_custom_aggs_to_by_ticker(
    config: PolygonConfig,
    overwrite: bool = False,
) -> str:
    file_info = config.filesystem.get_file_info(config.custom_aggs_dir)
    if file_info.type == pa_fs.FileType.NotFound:
        raise FileNotFoundError(f"{config.custom_aggs_dir=} not found.")

    by_ticker_aggs_arrow_dir = config.by_ticker_aggs_arrow_dir
    if os.path.exists(by_ticker_aggs_arrow_dir):
        if overwrite:
            print(f"Removing {by_ticker_aggs_arrow_dir=}")
            shutil.rmtree(by_ticker_aggs_arrow_dir)
        else:
            print(f"Found existing {by_ticker_aggs_arrow_dir=}")
            return by_ticker_aggs_arrow_dir

    schedule = config.calendar.trading_index(
        start=config.start_timestamp, end=config.end_timestamp, period="1D"
    )
    assert type(schedule) is pd.DatetimeIndex

    print(f"Scattering custom aggregates by ticker to {by_ticker_aggs_arrow_dir=}")
    aggs_ds = pa_ds.dataset(
        config.custom_aggs_dir,
        format="parquet",
        schema=custom_aggs_schema(tz=config.calendar.tz.key),
        partitioning=custom_aggs_partitioning(),
    )
    by_ticker_partitioning = pa_ds.partitioning(
        pa.schema([(PARTITION_COLUMN_NAME, pa.string())]), flavor="hive"
    )
    for timestamp in schedule:
        pa_ds.write_dataset(
            table_for_date(aggs_ds=aggs_ds, date=timestamp.to_pydatetime().date()),
            base_dir=by_ticker_aggs_arrow_dir,
            partitioning=by_ticker_partitioning,
            format="parquet",
            existing_data_behavior="overwrite_or_ignore",
            # file_visitor=file_visitor,
        )
    print(f"Scattered custom aggregates by ticker to {by_ticker_aggs_arrow_dir=}")
    return by_ticker_aggs_arrow_dir


def generate_tables_from_custom_aggs_ds(
    aggs_ds: pa_ds.Dataset, schedule: pd.DatetimeIndex
):
    for timestamp in schedule:
        yield table_for_date(aggs_ds=aggs_ds, date=timestamp.to_pydatetime().date())


def calculate_mfi(typical_price: pd.Series, money_flow: pd.Series, period: int):
    mf_sign = np.where(typical_price > np.roll(typical_price, shift=1), 1, -1)
    signed_mf = money_flow * mf_sign

    # Calculate gain and loss using vectorized operations
    positive_mf = np.maximum(signed_mf, 0)
    negative_mf = np.maximum(-signed_mf, 0)

    mf_avg_gain = (
        np.convolve(positive_mf, np.ones(period), mode="full")[: len(positive_mf)]
        / period
    )
    mf_avg_loss = (
        np.convolve(negative_mf, np.ones(period), mode="full")[: len(negative_mf)]
        / period
    )

    epsilon = 1e-10  # Small epsilon value to avoid division by zero
    mfi = 100 - (100 / (1 + mf_avg_gain / (mf_avg_loss + epsilon)))
    return mfi


# https://github.com/twopirllc/pandas-ta/blob/main/pandas_ta/momentum/stoch.py
# https://github.com/twopirllc/pandas-ta/blob/development/pandas_ta/momentum/stoch.py
# `k` vs `fast_k` arg names.
# https://github.com/twopirllc/pandas-ta/issues/726
# Results affected by values outside range
# https://github.com/twopirllc/pandas-ta/issues/535


def calculate_stoch(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    k: int = 14,
    d: int = 3,
    smooth_k: int = 3,
    mamode: str = "sma",
):
    """Indicator: Stochastic Oscillator (STOCH)"""
    lowest_low = low.rolling(k).min()
    highest_high = high.rolling(k).max()

    stoch = 100 * (close - lowest_low)
    stoch /= ta.utils.non_zero_range(highest_high, lowest_low)

    stoch_k = ta.overlap.ma(
        mamode, stoch.loc[stoch.first_valid_index() :,], length=smooth_k
    )
    stoch_d = (
        ta.overlap.ma(mamode, stoch_k.loc[stoch_k.first_valid_index() :,], length=d)
        if stoch_k is not None
        else None
    )
    # Histogram
    stoch_h = stoch_k - stoch_d if stoch_d is not None else None

    return stoch_k, stoch_d, stoch_h


def compute_per_ticker_signals(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    df = df.set_index("window_start").sort_index()
    session_index = pd.date_range(
        start=df.index[0], end=df.index[-1], freq=pd.Timedelta(seconds=60)
    )
    df = df.reindex(session_index)
    df.index.rename("window_start", inplace=True)

    # df["minute_of_day"] = (df.index.hour * 60) + df.index.minute
    # df["day_of_week"] = df.index.day_of_week

    df.transactions = df.transactions.fillna(0)
    df.volume = df.volume.fillna(0)
    df.total = df.total.fillna(0)
    df.close = df.close.ffill()
    close = df.close
    df.vwap = df.vwap.fillna(close)
    df.high = df.high.fillna(close)
    df.low = df.low.fillna(close)
    df.open = df.open.fillna(close)
    price_open = df.open
    high = df.high
    low = df.low
    vwap = df.vwap
    # volume = df.volume
    total = df.total
    next_close = close.shift()

    # TODO: Odometer rollover signal.  Relative difference to nearest power of 10.
    # Something about log10 being a whole number?  When is $50 the rollover vs $100 or $10?

    # "True (Typical?) Price" which I think is an approximation of VWAP.
    # Trouble with both is that if there are no trades in a bar we get NaN.
    # That then means we get NaN for averages for the next period-1 bars too.
    # Question is whether to ffill the price for these calculations.
    df["TP"] = (high + low + close) / 3

    # Gain/loss in this bar.
    df["ret1bar"] = close.div(price_open).sub(1)

    for t in range(2, period):
        df[f"ret{t}bar"] = close.div(price_open.shift(t - 1)).sub(1)

    # Average True Range (ATR)
    true_range = pd.concat(
        [high.sub(low), high.sub(next_close).abs(), low.sub(next_close).abs()], axis=1
    ).max(1)
    # Normalized ATR (NATR) or Average of Normalized TR.
    # Choice of NATR operations ordering discussion: https://www.macroption.com/normalized-atr/
    # He doesn't talk about VWAP but I think that is a better normalizing price for a bar.
    # atr = true_range.ewm(span=period).mean()
    # df["natr_c"] = atr / close
    # df["antr_c"] = (true_range / close).ewm(span=period).mean()
    # df["natr_v"] = atr / vwap
    # df["antr_v"] = (true_range / vwap).ewm(span=period).mean()
    df["NATR"] = (true_range / vwap).ewm(span=period).mean()

    # True Price as HLC average VS VWAP.
    # VWAP is better I think but is quite different than standard CCI.
    # Three ways to compute CCI, all give the same value using TP.
    # tp = (high + low + close) / 3
    # df['SMA'] = ta.sma(tp, length=period)
    # df['sma_r'] = tp.rolling(period).mean()
    # df['MAD'] = ta.mad(tp, length=period)
    # # Series.mad deprecated. mad = (s - s.mean()).abs().mean()
    # df['mad_r'] = tp.rolling(period).apply(lambda x: (pd.Series(x) - pd.Series(x).mean()).abs().mean())

    # df['cci_r'] = (tp - df['sma_r']) / (0.015 * df['mad_r'])
    # df['CCI'] = (tp - df['SMA']) / (0.015 * df['MAD'])
    # df['cci_ta'] = ta.cci(high=high, low=low, close=close, length=period)

    df["taCCI"] = ta.cci(high=high, low=low, close=close, length=period)

    # https://gist.github.com/quantra-go-algo/1b37bfb74d69148f0dfbdb5a2c7bdb25
    # https://medium.com/@huzaifazahoor654/how-to-calculate-cci-in-python-a-step-by-step-guide-9a3f61698be6
    sma = pd.Series(ta.sma(vwap, length=period))
    mad = pd.Series(ta.mad(vwap, length=period))
    df["CCI"] = (vwap - sma) / (0.015 * mad)

    # df['MFI'] = calculate_mfi(high=high, low=low, close=close, volume=volume, period=period)
    df["MFI"] = calculate_mfi(typical_price=vwap, money_flow=total, period=period)

    # We use Stochastic (rather than MACD because we need a ticker independent indicator.
    # IOW a percentage price oscillator (PPO) rather than absolute price oscillator (APO).
    # https://www.alpharithms.com/moving-average-convergence-divergence-macd-031217/
    # We're using 14/3 currently rather than the usual 26/12 popular for MACD though.
    stoch_k, stoch_d, stoch_h = calculate_stoch(high, low, close, k=period)
    df["STOCHk"] = stoch_k
    df["STOCHd"] = stoch_d
    df["STOCHh"] = stoch_h

    return df


def iterate_all_aggs_tables(
    config: PolygonConfig,
    valid_tickers: pa.Array,
):
    schedule = config.calendar.trading_index(
        start=config.start_timestamp, end=config.end_timestamp, period="1D"
    )
    for timestamp in schedule:
        date = timestamp.to_pydatetime().date()
        aggs_ds = pa_ds.dataset(
            config.custom_aggs_dir,
            format="parquet",
            schema=custom_aggs_schema(tz=config.calendar.tz.key),
            partitioning=custom_aggs_partitioning(),
        )
        date_filter_expr = (
            (pa_compute.field("year") == date.year)
            & (pa_compute.field("month") == date.month)
            & (pa_compute.field("date") == date)
        )
        # print(f"{date_filter_expr=}")
        for fragment in aggs_ds.get_fragments(filter=date_filter_expr):
            session_filter = (
                (pa_compute.field("window_start") >= start_dt)
                & (pa_compute.field("window_start") < end_dt)
                & pa_compute.is_in(pa_compute.field("ticker"), valid_tickers)
            )
            # Sorting table doesn't seem to avoid needing to sort the df.  Maybe use_threads=False on to_pandas would help?
            # table = fragment.to_table(filter=session_filter).sort_by([('ticker', 'ascending'), ('window_start', 'descending')])
            table = fragment.to_table(filter=session_filter)
            if table.num_rows > 0:
                metadata = (
                    dict(table.schema.metadata) if table.schema.metadata else dict()
                )
                metadata["date"] = date.isoformat()
                table = table.replace_schema_metadata(metadata)
                yield table


# def iterate_all_aggs_with_signals(config: PolygonConfig):
#     for table in iterate_all_aggs_tables(config):
#         df = table.to_pandas()
#         df = df.groupby("ticker").apply(
#             compute_per_ticker_signals, include_groups=False
#         )
#         yield pa.Table.from_pandas(df)


def compute_signals_for_all_custom_aggs(
    from_config: PolygonConfig,
    to_config: PolygonConfig,
    valid_tickers: pa.Array,
    overwrite: bool = False,
) -> str:
    if overwrite:
        print("WARNING: overwrite not implemented/ignored.")

    print(f"{to_config.custom_aggs_dir=}")

    for aggs_table in iterate_all_aggs_tables(from_config, valid_tickers):
        metadata = aggs_table.schema.metadata
        date = datetime.date.fromisoformat(metadata[b"date"].decode("utf-8"))
        print(f"{date=}")
        df = aggs_table.to_pandas()
        df = df.groupby("ticker").apply(
            compute_per_ticker_signals, include_groups=False
        )
        table = pa.Table.from_pandas(df)
        if table.num_rows > 0:
            table = table.replace_schema_metadata(metadata)
            table = table.append_column("date", pa.array(np.full(len(table), date)))
            table = table.append_column(
                "year", pa.array(np.full(len(table), date.year), type=pa.uint16())
            )
            table = table.append_column(
                "month", pa.array(np.full(len(table), date.month), type=pa.uint8())
            )
            table = table.sort_by(
                [("ticker", "ascending"), ("window_start", "ascending")]
            )
            pa_ds.write_dataset(
                table,
                filesystem=to_config.filesystem,
                base_dir=to_config.custom_aggs_dir,
                partitioning=custom_aggs_partitioning(),
                format="parquet",
                existing_data_behavior="overwrite_or_ignore",
                file_visitor=file_visitor,
            )
    return to_config.custom_aggs_dir
