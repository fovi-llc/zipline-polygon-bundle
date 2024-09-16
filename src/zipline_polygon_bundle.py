import pyarrow.compute
from zipline.data.bundles import register

from config import PolygonConfig
from tickers_and_names import PolygonAssets
from concat_all_aggs import concat_all_aggs_from_csv

import polygon
from urllib3 import HTTPResponse

import pyarrow
import pandas as pd
import csv
import os
import logging


def list_to_string(x):
    if not hasattr(x, "__len__"):
        return str(x)
    if len(x) == 0:
        return ""
    if len(x) == 1:
        return str(x[0])
    s = set([str(y) for y in x])
    return f"[{']['.join(sorted(list(s)))}]"


def get_ticker_universe(config: PolygonConfig, fetch_missing: bool = False):
    tickers_csv_path = config.tickers_csv_path
    print(f"{tickers_csv_path=}")
    parquet_path = tickers_csv_path.removesuffix(".csv") + ".parquet"
    if not os.path.exists(parquet_path):
        if os.path.exists(tickers_csv_path):
            os.remove(tickers_csv_path)
        assets = PolygonAssets(config)
        all_tickers = assets.load_all_tickers(fetch_missing=fetch_missing)
        all_tickers.info()
        # all_tickers.to_csv(tickers_csv_path)
        logging.info("Merging tickers")
        merged_tickers = assets.merge_tickers(all_tickers)
        merged_tickers.info()
        merged_tickers.to_parquet(tickers_csv_path.removesuffix(".csv") + ".parquet")
        print(
            f"Saved {len(merged_tickers)} tickers to {tickers_csv_path.removesuffix('.csv') + '.parquet'}"
        )
    if not os.path.exists(tickers_csv_path):
        merged_tickers = pd.read_parquet(parquet_path)
        merged_tickers["name"] = merged_tickers["name"].apply(list_to_string)
        merged_tickers["share_class_figi"] = merged_tickers["share_class_figi"].apply(
            list_to_string
        )
        merged_tickers["delisted_utc"] = merged_tickers["delisted_utc"].apply(
            list_to_string
        )
        merged_tickers["currency_name"] = merged_tickers["currency_name"].apply(
            list_to_string
        )
        merged_tickers["locale"] = merged_tickers["locale"].apply(list_to_string)
        merged_tickers["market"] = merged_tickers["market"].apply(list_to_string)
        merged_tickers.to_csv(
            tickers_csv_path, escapechar="\\", quoting=csv.QUOTE_NONNUMERIC
        )
        print(f"Saved {len(merged_tickers)} tickers to {tickers_csv_path}")

    # merged_tickers = pd.read_csv(
    #     tickers_csv_path,
    #     escapechar="\\",
    #     quoting=csv.QUOTE_NONNUMERIC,
    #     dtype={
    #         "ticker": str,
    #         "primary_exchange": str,
    #         "cik": str,
    #         "type": str,
    #         "share_class_figi": str,
    #     },
    #     # converters={
    #     #     "ticker": lambda x: str(x),
    #     #     "start_date": lambda x: pd.to_datetime(x),
    #     #     "cik": lambda x: str(x) if x else None,
    #     #     "name": lambda x: str(x),
    #     #     "end_date": lambda x: pd.to_datetime(x),
    #     #     "composite_figi": lambda x: str(x).upper(),
    #     #     "share_class_figi": lambda x: str(x).upper(),
    #     #     "currency_name": lambda x: str(x).lower(),
    #     #     "locale": lambda x: str(x).lower(),
    #     #     "market": lambda x: str(x).lower(),
    #     #     "primary_exchange": lambda x: str(x).strip().upper(),
    #     #     "type": lambda x: str(x).upper(),
    #     # },
    # )
    merged_tickers = pd.read_parquet(parquet_path)
    merged_tickers.info()
    return merged_tickers


def polygon_equities_bundle_day(
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
):
    config = PolygonConfig(
        environ=environ,
        calendar_name=calendar.name,
        start_session=start_session,
        end_session=end_session,
        agg_time="day",
    )
    return polygon_equities_bundle(
        config,
        asset_db_writer,
        minute_bar_writer,
        daily_bar_writer,
        adjustment_writer,
        calendar,
        start_session,
        end_session,
        cache,
        show_progress,
        output_dir,
    )


def polygon_equities_bundle_minute(
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
):
    config = PolygonConfig(
        environ=environ,
        calendar_name=calendar.name,
        start_session=start_session,
        end_session=end_session,
        agg_time="minute",
    )
    return polygon_equities_bundle(
        config,
        asset_db_writer,
        minute_bar_writer,
        daily_bar_writer,
        adjustment_writer,
        calendar,
        start_session,
        end_session,
        cache,
        show_progress,
        output_dir,
    )


def load_polygon_splits(config: PolygonConfig) -> pd.DataFrame:
    splits_path = config.splits_parquet
    if os.path.exists(splits_path):
        splits = pd.read_parquet(splits_path)
        if len(splits) > 10000:
            return splits
        logging.error(f"Only found {len(splits)=} at {splits_path}")

    client = polygon.RESTClient(api_key=config.api_key)
    splits = client.list_splits(limit=1000)
    if splits is HTTPResponse:
        logging.error(f"HTTPResponse from {splits}")
        raise ValueError(f"HTTPResponse from {splits}")
    splits = pd.DataFrame(splits)
    os.makedirs(os.path.dirname(splits_path), exist_ok=True)
    splits.to_parquet(splits_path)
    if len(splits) <= 10000:
        logging.error(f"Only found {len(splits)=} at {splits_path}")
    return splits


def load_splits(config: PolygonConfig, ticker_to_sid: dict[str, int]) -> pd.DataFrame:
    splits = load_polygon_splits(config)
    splits["sid"] = splits["ticker"].apply(lambda x: ticker_to_sid.get(x, pd.NA))
    splits.dropna(inplace=True)
    splits["sid"] = splits["sid"].astype('int64')
    splits["execution_date"] = pd.to_datetime(splits["execution_date"])
    splits.rename(columns={"execution_date": "effective_date"}, inplace=True)
    # Not only do we want a float for ratio but some to/from are not integers.
    splits["split_from"] = splits["split_from"].astype(float)
    splits["split_to"] = splits["split_to"].astype(float)
    splits["ratio"] = splits["split_to"] / splits["split_from"]
    splits.drop(columns=["ticker", "split_from", "split_to"], inplace=True)
    splits.info()
    return splits


def load_polygon_dividends(config: PolygonConfig) -> pd.DataFrame:
    dividends_path = config.dividends_parquet
    if os.path.exists(dividends_path):
        dividends = pd.read_parquet(dividends_path)
        if len(dividends) > 10000:
            return dividends
        logging.error(f"Only found {len(dividends)=} at {dividends_path}")

    client = polygon.RESTClient(api_key=config.api_key)
    dividends = client.list_dividends(limit=1000)
    if dividends is HTTPResponse:
        logging.error(f"HTTPResponse from {dividends}")
        raise ValueError(f"HTTPResponse from {dividends}")
    dividends = pd.DataFrame(dividends)
    os.makedirs(os.path.dirname(dividends_path), exist_ok=True)
    dividends.to_parquet(dividends_path)
    if len(dividends) <= 10000:
        logging.error(f"Only found {len(dividends)=} at {dividends_path}")
    return dividends


# class Dividend:
#     cash_amount: Optional[float] = None
#     currency: Optional[str] = None
#     declaration_date: Optional[str] = None
#     dividend_type: Optional[str] = None
#     ex_dividend_date: Optional[str] = None
#     frequency: Optional[int] = None
#     pay_date: Optional[str] = None
#     record_date: Optional[str] = None
#     ticker: Optional[str] = None


def load_dividends(
    config: PolygonConfig, ticker_to_sid: dict[str, int]
) -> pd.DataFrame:
    dividends = load_polygon_dividends(config)
    dividends["sid"] = dividends["ticker"].apply(lambda x: ticker_to_sid.get(x, pd.NA))
    dividends.dropna(how="any", inplace=True)
    dividends["sid"] = dividends["sid"].astype('int64')
    dividends["declaration_date"] = pd.to_datetime(dividends["declaration_date"])
    dividends["ex_dividend_date"] = pd.to_datetime(dividends["ex_dividend_date"])
    dividends["record_date"] = pd.to_datetime(dividends["record_date"])
    dividends["pay_date"] = pd.to_datetime(dividends["pay_date"])
    dividends.rename(
        columns={
            "cash_amount": "amount",
            "declaration_date": "declared_date",
            "ex_dividend_date": "ex_date",
        },
        inplace=True,
    )
    dividends.drop(
        columns=["ticker", "frequency", "currency", "dividend_type"], inplace=True
    )
    dividends.info()
    return dividends


def polygon_equities_bundle(
    config: PolygonConfig,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
):
    assert calendar == config.calendar

    metadata = pd.DataFrame(
        columns=(
            "start_date",
            "end_date",
            "auto_close_date",
            "symbol",
            "exchange",
            "asset_name",
        )
    )

    if not os.path.exists(config.by_ticker_hive_dir):
        concat_all_aggs_from_csv(config)

    aggregates = pyarrow.dataset.dataset(config.by_ticker_hive_dir)

    ticker_to_sid = {}

    # Get data for all stocks and write to Zipline
    if config.agg_time == "day":
        daily_bar_writer.write(
            process_day_aggregates(
                aggregates,
                calendar.sessions_in_range(start_session, end_session),
                metadata,
                calendar,
                ticker_to_sid,
            ),
            show_progress=show_progress,
        )
    else:
        minute_bar_writer.write(
            process_minute_aggregates(
                aggregates,
                calendar.sessions_minutes(start_session, end_session),
                metadata,
                calendar,
                ticker_to_sid,
            ),
            show_progress=show_progress,
        )

    # Write the metadata
    asset_db_writer.write(equities=metadata)

    # Load splits and dividends
    splits = load_splits(config, ticker_to_sid)
    dividends = load_dividends(config, ticker_to_sid)

    # Write splits and dividends
    adjustment_writer.write(splits=splits, dividends=dividends)


def symbol_to_upper(s: str) -> str:
    return "".join(map(lambda c: ("^" + c.upper()) if c.islower() else c, s))


def process_day_aggregates(
    aggregates, sessions, metadata, calendar, ticker_to_sid: dict[str, int]
):
    table = aggregates.to_table()
    table = table.rename_columns({"ticker": "symbol", "window_start": "day"})
    table = table.sort_by([("symbol", "ascending")])
    symbols = sorted(set(table.column("symbol").to_pylist()))
    for sid, symbol in enumerate(symbols):
        ticker_to_sid[symbol] = sid
        df = table.filter(
            pyarrow.compute.field("symbol") == pyarrow.scalar(symbol)
        ).to_pandas()
        df["day"] = pd.to_datetime(df["day"].dt.date)
        df = df.set_index("day")
        # The SQL schema zipline uses for symbols ignores case
        if not symbol.isupper():
            df["symbol"] = symbol_to_upper(symbol)
        # Remove duplicates
        df = df[~df.index.duplicated()]
        # Take days as per calendar
        df = df[df.index.isin(sessions)]
        if len(df) < 2:
            print(f" WARNING: Not enough data for {symbol}")
            # day_writer apparently is okay with these, just minute_writer barfs.
            # continue
        # Check first and last date.
        start_date = df.index[0]
        end_date = df.index[-1]
        # Synch to the official exchange calendar
        df = df.reindex(sessions.tz_localize(None))[
            start_date:end_date
        ]  # tz_localize(None)
        # Missing volume and transactions are zero
        df["volume"] = df["volume"].fillna(0)
        df["transactions"] = df["transactions"].fillna(0)
        # Forward fill missing price data (better than backfill)
        df.ffill(inplace=True)
        # Back fill missing data (maybe necessary for the first day)
        df.bfill(inplace=True)
        # There should be no missing data
        if df.isnull().sum().sum() > 0:
            print(f" WARNING: Missing data for {symbol}")

        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)

        # Add a row to the metadata DataFrame. Don't forget to add an exchange field.
        metadata.loc[sid] = (
            start_date,
            end_date,
            ac_date,
            symbol_to_upper(symbol),
            calendar.name,
            symbol,
        )
        yield sid, df
    return


def process_minute_aggregates(
    aggregates, sessions, metadata, calendar, ticker_to_sid: dict[str, int]
):
    table = aggregates.to_table()
    table = table.rename_columns({"ticker": "symbol", "window_start": "timestamp"})
    table = table.sort_by([("symbol", "ascending")])
    symbols = sorted(set(table.column("symbol").to_pylist()))
    for sid, symbol in enumerate(symbols):
        ticker_to_sid[symbol] = sid
        df = table.filter(
            pyarrow.compute.field("symbol") == pyarrow.scalar(symbol)
        ).to_pandas()
        # df["day"] = pd.to_datetime(df["day"].dt.date)
        df = df.set_index("timestamp")
        # The SQL schema zipline uses for symbols ignores case
        if not symbol.isupper():
            df["symbol"] = symbol_to_upper(symbol)
        df.info()
        # Remove duplicates
        df = df[~df.index.duplicated()]
        # Take days as per calendar
        df = df[df.index.isin(sessions)]
        if len(df) < 2:
            print(f"WARNING: Not enough data for {symbol}")
            continue
        # Check first and last date.
        start_date = df.index[0].date()
        end_date = df.index[-1].date()
        # Synch to the official exchange calendar
        df = df.reindex(sessions.tz_localize(None))[
            start_date:end_date
        ]  # tz_localize(None)
        # Missing volume and transactions are zero
        df["volume"] = df["volume"].fillna(0)
        df["transactions"] = df["transactions"].fillna(0)
        # Forward fill missing price data (better than backfill)
        df.ffill(inplace=True)
        # Back fill missing data (maybe necessary for the first day)
        df.bfill(inplace=True)
        # There should be no missing data
        if df.isnull().sum().sum() > 0:
            print(f" WARNING: Missing data for {symbol}")

        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)

        # Add a row to the metadata DataFrame. Don't forget to add an exchange field.
        metadata.loc[sid] = (
            start_date,
            end_date,
            ac_date,
            symbol_to_upper(symbol),
            calendar.name,
            symbol,
        )
        # A df with 1 bar crashes zipline/data/bcolz_minute_bars.py", line 747
        # pd.Timestamp(dts[0]), direction="previous"
        if len(df) > 1:
            yield sid, df
        else:
            print(f" WARNING: Not enough data post reindex for {symbol}")
    return


def register_polygon_equities_bundle(
    bundlename,
    start_session=None,
    end_session="now",
    calendar_name="XNYS",
    agg_time="day",
    # ticker_list=None,
    # watchlists=None,
    # include_asset_types=None,
):
    if agg_time not in ["day", "minute"]:
        raise ValueError(f"agg_time must be 'day' or 'minute', not {agg_time}")
    register(
        bundlename,
        (
            polygon_equities_bundle_minute
            if agg_time == "minute"
            else polygon_equities_bundle_day
        ),
        start_session=start_session,
        end_session=end_session,
        calendar_name=calendar_name,
        # minutes_per_day=390,
        # create_writers=True,
    )


# if __name__ == "__main__":
#     logging.basicConfig(level=logging.WARNING)
#     os.environ["POLYGON_MIRROR_DIR"] = "/Volumes/Oahu/Mirror/files.polygon.io"
#     config = PolygonConfig(
#         environ=os.environ,
#         calendar_name="XNYS",
#         # start_session="2003-10-01",
#         # start_session="2018-01-01",
#         start_session="2023-01-01",
#         # end_session="2023-01-12",
#         end_session="2023-12-31",
#         # end_session="2024-06-30",
#     )
#     splits = load_polygon_splits(config)
#     splits.info()
#     print(splits.head())
#     dividends = load_polygon_dividends(config)
#     dividends.info()
#     print(dividends.head())
#     tickers = set(
#         splits["ticker"].unique().tolist() + dividends["ticker"].unique().tolist()
#     )
#     print(f"{len(tickers)=}")
#     ticker_to_sid = {ticker: sid for sid, ticker in enumerate(tickers)}
#     splits = load_splits(config, ticker_to_sid)
#     splits.info()
#     print(splits.head())
#     dividends = load_dividends(config, ticker_to_sid)
#     dividends.info()
#     print(dividends.head())
