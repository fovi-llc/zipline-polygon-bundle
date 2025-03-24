from .config import PolygonConfig
from .trades import custom_aggs_schema, custom_aggs_partitioning

import datetime
import numpy as np
import pyarrow as pa
import pyarrow.compute as pa_compute
import pyarrow.dataset as pa_ds
import pandas_ta as ta
import pandas as pd


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
            config.aggs_dir,
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


def file_visitor(written_file):
    print(f"{written_file.path=}")


def compute_signals_for_all_aggs(
    from_config: PolygonConfig,
    to_config: PolygonConfig,
    valid_tickers: pa.Array,
    overwrite: bool = False,
) -> str:
    if overwrite:
        print("WARNING: overwrite not implemented/ignored.")

    # Need a different aggs_dir for the signals because schema is different.
    print(f"{to_config.aggs_dir=}")

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
                base_dir=to_config.aggs_dir,
                partitioning=custom_aggs_partitioning(),
                format="parquet",
                existing_data_behavior="overwrite_or_ignore",
                file_visitor=file_visitor,
            )
    return to_config.aggs_dir
