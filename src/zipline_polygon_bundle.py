from zipline.data.bundles import register

from config import PolygonConfig
from concat_all_aggs import concat_all_aggs_from_csv, generate_csv_agg_tables

import polygon
from urllib3 import HTTPResponse

import pyarrow
import pyarrow.compute

import pandas as pd
import datetime
import os
import logging


# TODO: Change warnings to be relative to number of days in the range.


def symbol_to_upper(s: str) -> str:
    if s.isupper():
        return s
    return "".join(map(lambda c: ("^" + c.upper()) if c.islower() else c, s))


def load_polygon_splits(
    config: PolygonConfig, first_start_end: datetime.date, last_end_date: datetime.date
) -> pd.DataFrame:
    splits_path = config.api_cache_path(
        start_date=first_start_end, end_date=last_end_date, filename="list_splits"
    )
    if not os.path.exists(splits_path):
        client = polygon.RESTClient(api_key=config.api_key)
        splits = client.list_splits(
            limit=1000,
            execution_date_gte=first_start_end,
            execution_date_lt=last_end_date + datetime.timedelta(days=1),
        )
        if splits is HTTPResponse:
            raise ValueError(f"Polygon.list_splits bad HTTPResponse: {splits}")
        splits = pd.DataFrame(splits)
        print(f"Got {len(splits)=} from Polygon list_splits.")
        os.makedirs(os.path.dirname(splits_path), exist_ok=True)
        splits.to_parquet(splits_path)
        if len(splits) < 10000:
            logging.error(f"Only got {len(splits)=} from Polygon list_splits.")
        # We will always load from the file to avoid any chance of weird errors.
    if os.path.exists(splits_path):
        splits = pd.read_parquet(splits_path)
        print(f"Loaded {len(splits)} splits from {splits_path}")
        if len(splits) < 10000:
            logging.error(f"Only found {len(splits)=} at {splits_path}")
        return splits
    raise ValueError(f"Failed to load splits from {splits_path}")


def load_splits(
    config: PolygonConfig,
    first_start_end: datetime.date,
    last_end_date: datetime.date,
    ticker_to_sid: dict[str, int],
) -> pd.DataFrame:
    splits = load_polygon_splits(config, first_start_end, last_end_date)
    splits["sid"] = splits["ticker"].apply(lambda t: ticker_to_sid.get(t, pd.NA))
    splits.dropna(inplace=True)
    splits["sid"] = splits["sid"].astype("int64")
    splits["execution_date"] = pd.to_datetime(splits["execution_date"])
    splits.rename(columns={"execution_date": "effective_date"}, inplace=True)
    # Not only do we want a float for ratio but some to/from are not integers.
    splits["split_from"] = splits["split_from"].astype(float)
    splits["split_to"] = splits["split_to"].astype(float)
    splits["ratio"] = splits["split_from"] / splits["split_to"]
    splits.drop(columns=["ticker", "split_from", "split_to"], inplace=True)
    splits.info()
    return splits


def load_polygon_dividends(
    config: PolygonConfig, first_start_end: datetime.date, last_end_date: datetime.date
) -> pd.DataFrame:
    dividends_path = config.api_cache_path(
        start_date=first_start_end, end_date=last_end_date, filename="list_dividends"
    )
    if not os.path.exists(dividends_path):
        client = polygon.RESTClient(api_key=config.api_key)
        dividends = client.list_dividends(
            limit=1000,
            record_date_gte=first_start_end,
            pay_date_lt=last_end_date + datetime.timedelta(days=1),
        )
        if dividends is HTTPResponse:
            raise ValueError(f"Polygon.list_dividends bad HTTPResponse: {dividends}")
        dividends = pd.DataFrame(dividends)
        print(f"Got {len(dividends)=} from Polygon list_dividends.")
        if len(dividends) < 10000:
            logging.error(f"Only got {len(dividends)=} from Polygon list_dividends.")
        os.makedirs(os.path.dirname(dividends_path), exist_ok=True)
        dividends.to_parquet(dividends_path)
        # We will always load from the file to avoid any chance of weird errors.
    if os.path.exists(dividends_path):
        dividends = pd.read_parquet(dividends_path)
        print(f"Loaded {len(dividends)} splits from {dividends_path}")
        if len(dividends) < 10000:
            logging.error(f"Only found {len(dividends)=} at {dividends_path}")
        return dividends
    raise ValueError(f"Failed to load dividends from {dividends_path}")


def load_dividends(
    config: PolygonConfig,
    first_start_end: datetime.date,
    last_end_date: datetime.date,
    ticker_to_sid: dict[str, int],
) -> pd.DataFrame:
    dividends = load_polygon_dividends(config, first_start_end, last_end_date)
    dividends["sid"] = dividends["ticker"].apply(lambda t: ticker_to_sid.get(t, pd.NA))
    dividends.dropna(how="any", inplace=True)
    dividends["sid"] = dividends["sid"].astype("int64")
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


def generate_all_agg_tables_from_csv(
    config: PolygonConfig,
):
    schema, tables = generate_csv_agg_tables(config)
    for table in tables:
        table = table.sort_by([("ticker", "ascending"), ("window_start", "ascending")])
        yield table


def process_day_aggregates(
    table,
    sessions,
    metadata,
    calendar,
    symbol_to_sid: dict[str, int],
    dates_with_data: set,
):
    for symbol, sid in symbol_to_sid.items():
        df = table.filter(
            pyarrow.compute.field("symbol") == pyarrow.scalar(symbol)
        ).to_pandas()
        # The SQL schema zipline uses for symbols ignores case
        symbol_escaped = symbol_to_upper(symbol)
        df["symbol"] = symbol_escaped
        df["day"] = pd.to_datetime(df["day"].dt.date)
        df = df.set_index("day")
        if not df.index.is_monotonic_increasing:
            print(f" INFO: {symbol=} {sid=} not monotonic increasing")
            df.sort_index(inplace=True)
            # Remove duplicates
            df = df[~df.index.duplicated(keep='first')]
        # Take days as per calendar
        df = df[df.index.isin(sessions)]
        if len(df) < 2:
            print(f" WARNING: Not enough data for {symbol=} {sid=}")
            # day_writer apparently is okay with these, just minute_writer barfs.
            # continue
        # Check first and last date.
        start_date = df.index[0]
        dates_with_data.add(start_date.date())
        end_date = df.index[-1]
        dates_with_data.add(end_date.date())
        # Synch to the official exchange calendar
        df = df.reindex(sessions.tz_localize(None))[
            start_date:end_date
        ]  # tz_localize(None)
        # Missing volume and transactions are zero
        df["volume"] = df["volume"].fillna(0)
        df["transactions"] = df["transactions"].fillna(0)
        # Forward fill missing price data (better than backfill)
        df.ffill(inplace=True)
        # Back fill missing data (maybe necessary for before the first day bar)
        df.bfill(inplace=True)
        # There should be no missing data
        if df.isnull().sum().sum() > 0:
            print(f" WARNING: Missing data for {symbol=} {sid=}")

        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)

        # Add a row to the metadata DataFrame. Don't forget to add an exchange field.
        metadata.loc[sid] = (
            start_date,
            end_date,
            ac_date,
            symbol_escaped,
            calendar.name,
            symbol,
        )
        yield sid, df
    return


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

    config.by_ticker_hive_dir = os.path.join(cache, "day_aggs_by_ticker")
    concat_all_aggs_from_csv(config)
    aggregates = pyarrow.dataset.dataset(config.by_ticker_hive_dir)

    # Zipline uses case-insensitive symbols, so we need to convert them to uppercase with a ^ prefix when lowercase.
    # This is because the SQL schema zipline uses for symbols ignores case.
    # We put the original symbol in the asset_name field.
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

    table = aggregates.to_table()
    table = table.rename_columns({"ticker": "symbol", "window_start": "day"})
    # Get all the symbols in the table by using value_counts to tabulate the unique values.
    # pyarrow.Table.column returns a pyarrow.ChunkedArray.
    # https://arrow.apache.org/docs/python/generated/pyarrow.ChunkedArray.html#pyarrow.ChunkedArray.value_counts
    symbols = sorted(table.column("symbol").value_counts().field(0).to_pylist())
    symbol_to_sid = {symbol: sid for sid, symbol in enumerate(symbols)}
    dates_with_data = set()

    # Get data for all stocks and write to Zipline
    daily_bar_writer.write(
        process_day_aggregates(
            table=table,
            sessions=calendar.sessions_in_range(start_session, end_session),
            metadata=metadata,
            calendar=calendar,
            symbol_to_sid=symbol_to_sid,
            dates_with_data=dates_with_data,
        ),
        show_progress=show_progress,
    )

    # Write the metadata
    asset_db_writer.write(equities=metadata)

    # Load splits and dividends
    first_start_end = min(dates_with_data)
    last_end_date = max(dates_with_data)
    splits = load_splits(config, first_start_end, last_end_date, symbol_to_sid)
    dividends = load_dividends(config, first_start_end, last_end_date, symbol_to_sid)

    # Write splits and dividends
    adjustment_writer.write(splits=splits, dividends=dividends)


def process_minute_aggregates(
    aggregates,
    sessions,
    metadata,
    calendar,
    ticker_to_sid: dict[str, int],
    dates_with_data: set,
):
    aggregates = aggregates.rename_columns({"ticker": "symbol", "window_start": "timestamp"})
    for symbol in sorted(set(aggregates.column("symbol").to_pylist())):
        if symbol not in ticker_to_sid:
            ticker_to_sid[symbol] = len(ticker_to_sid) + 1
        sid = ticker_to_sid[symbol]
        df = aggregates.filter(
            pyarrow.compute.field("symbol") == pyarrow.scalar(symbol)
        ).to_pandas()
        df = df.set_index("timestamp")
        # The SQL schema zipline uses for symbols ignores case
        if not symbol.isupper():
            df["symbol"] = symbol_to_upper(symbol)
        # # Remove duplicates
        # df = df[~df.index.duplicated()]
        # Take minutes as per calendar
        df = df[df.index.isin(sessions)]
        if len(df) < 2:
            print(f" WARNING: Not enough data for {symbol=} {sid=}")
            continue
        # Check first and last date.
        start_date = df.index[0].date()
        dates_with_data.add(start_date)
        end_date = df.index[-1].date()
        dates_with_data.add(end_date)
        # # Synch to the official exchange calendar
        # df = df.reindex(sessions.tz_localize(None))
        # # Missing volume and transactions are zero
        # df["volume"] = df["volume"].fillna(0)
        # df["transactions"] = df["transactions"].fillna(0)
        # df.info()
        # # Forward fill missing price data (better than backfill)
        # df.ffill(inplace=True)
        # df.info()
        # # Back fill missing data (maybe necessary for the first day)
        # df.bfill(inplace=True)
        # df.info()
        # # There should be no missing data
        # if len(df) != len(sessions):
        #     print(f" WARNING: Missing data for {symbol=} {len(df)=} != {len(sessions)=}")
        # if df.isnull().sum().sum() > 0:
        #     print(f" WARNING: nulls in data for {symbol=} {df.isnull().sum().sum()}")

        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)

        # If metadata already has this sid, just extend the end_date and ac_date.
        if sid in metadata.index:
            if metadata.loc[sid, "start_date"] >= start_date:
                print(
                    f" ERROR: {symbol=} {sid=} {metadata.loc[sid, 'start_date']=} >= {start_date=}"
                )
            if metadata.loc[sid, "end_date"] >= start_date:
                print(
                    f" ERROR: {symbol=} {sid=} {metadata.loc[sid, 'end_date']=} >= {end_date=}"
                )
            metadata.loc[sid, "end_date"] = end_date
            metadata.loc[sid, "auto_close_date"] = ac_date
        else:
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
            print(f"\n{symbol=} {sid=} {len(df)=} {df.index[0]=} {df.index[-1]=}")
            yield sid, df
        else:
            print(f" WARNING: Not enough data post reindex for {symbol=} {sid=}")
    return


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

    # aggregates = generate_all_aggs_from_csv(
    #     config, calendar, start_session, end_session
    # )

    schema, tables = generate_csv_agg_tables(config)

    ticker_to_sid = {}
    dates_with_data = set()

    minute_bar_writer.write(
        process_minute_aggregates(
            tables,
            calendar.sessions_minutes(start_session, end_session),
            metadata,
            calendar,
            ticker_to_sid,
            dates_with_data,
        ),
        show_progress=show_progress,
    )

    # Write the metadata
    asset_db_writer.write(equities=metadata)

    # Load splits and dividends
    first_start_end = min(dates_with_data)
    last_end_date = max(dates_with_data)
    splits = load_splits(config, first_start_end, last_end_date, ticker_to_sid)
    dividends = load_dividends(config, first_start_end, last_end_date, ticker_to_sid)

    # Write splits and dividends
    adjustment_writer.write(splits=splits, dividends=dividends)


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
        raise ValueError(f"agg_time must be 'day' or 'minute', not '{agg_time}'")
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
#     os.environ["ZIPLINE_ROOT"] = "/Volumes/Oahu/Workspaces/zipline"
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
