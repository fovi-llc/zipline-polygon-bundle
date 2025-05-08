from zipline.data.bundles import register
from zipline.data.resample import minute_frame_to_session_frame

from exchange_calendars.calendar_helpers import parse_date

from .concat_all_aggs import concat_all_aggs_from_csv, generate_csv_agg_tables
from .adjustments import load_splits, load_dividends
from .config import PolygonConfig, AGG_TIME_DAY, AGG_TIME_MINUTE, AGG_TIME_TRADES
from .nyse_all_hours_calendar import register_nyse_all_hours_calendar
from .trades import convert_trades_to_custom_aggs, scatter_custom_aggs_to_by_ticker

import pyarrow
import pyarrow.compute
import pyarrow.dataset

import pandas as pd

import os
from filelock import FileLock
import logging


# TODO: Change warnings to be relative to number of days in the range.


def symbol_to_upper(s: str) -> str:
    if s.isupper():
        return s
    return "".join(map(lambda c: ("^" + c.upper()) if c.islower() else c, s))


def generate_all_agg_tables_from_csv(
    config: PolygonConfig,
):
    schema, tables = generate_csv_agg_tables(config)
    for table in tables:
        table = table.sort_by([("ticker", "ascending"), ("window_start", "ascending")])
        yield table


def rename_polygon_to_zipline(table: pyarrow.Table, time_name: str) -> pyarrow.Table:
    table = table.rename_columns(
        [
            (
                "symbol"
                if name == "ticker"
                else time_name if name == "window_start" else name
            )
            for name in table.column_names
        ]
    )
    return table


def process_day_table(
    table,
    sessions,
    minutes,
    metadata,
    calendar,
    symbol_to_sid: dict[str, int],
    dates_with_data: set[pd.Timestamp],
    agg_time: str,
):
    table = rename_polygon_to_zipline(table, "day")
    symbols = table.column("symbol").unique().to_pylist()
    for sid, symbol in enumerate(symbols):
        symbol_to_sid[symbol] = sid
    for symbol, sid in symbol_to_sid.items():
        df = table.filter(
            pyarrow.compute.field("symbol") == pyarrow.scalar(symbol)
        ).to_pandas()
        # The SQL schema zipline uses for symbols ignores case
        sql_symbol = symbol_to_upper(symbol)
        df["symbol"] = sql_symbol
        df["day"] = pd.to_datetime(df["day"].dt.tz_convert(calendar.tz.key).dt.date)
        df = df.set_index("day")
        if not df.index.is_monotonic_increasing:
            print(f" INFO: {symbol=} {sid=} not monotonic increasing: {df.index.min()=} {df.index.max()=}")
            df.sort_index(inplace=True)
            # Remove duplicates
            df = df[~df.index.duplicated(keep="first")]
        # Take days as per calendar
        df = df[df.index.isin(sessions)]
        # 2019-08-13 has a bunch of tickers with multiple day aggs per date
        # TODO: Actually they're for different days so if the filtering doesn't work then do something about it.
        # df = aggregate_multiple_aggs_per_date(df)
        if len(df) < 1:
            continue
        # Check first and last date.
        start_date = df.index[0]
        dates_with_data.add(start_date)
        end_date = df.index[-1]
        dates_with_data.add(end_date)
        try:
            duplicated_index = df.index.duplicated(keep=False)
            df_with_duplicates = df[duplicated_index]
            if len(df_with_duplicates) > 0:
                print(f" WARNING: {symbol=} {sid=} {len(df_with_duplicates)=}")
                df_with_duplicates.info()
                print(df_with_duplicates)
            # Synch to the official exchange calendar
            df = df.reindex(sessions.tz_localize(None))
        except ValueError as e:
            print(f" ERROR: {symbol=} {sid=} {e}")
            print(f"{start_date=} {end_date=} {sessions[0]=} {sessions[-1]=}")
            df.info()
        # Missing volume and transactions are zero
        df["volume"] = df["volume"].fillna(0)
        df["transactions"] = df["transactions"].fillna(0)
        # TODO: These fills should have the same price for OHLC (open for backfill, close for forward fill)
        # Forward fill missing price data (better than backfill)
        df.ffill(inplace=True)
        # Back fill missing data (maybe necessary for before the first day bar)
        df.bfill(inplace=True)
        # There should be no missing data
        if df.isnull().sum().sum() > 0:
            print(f" WARNING: Missing data for {symbol=} {sid=}")

        # The auto_close date is the day after the last trade.
        auto_close_date = end_date + pd.Timedelta(days=1)

        # Add a row to the metadata DataFrame. Don't forget to add an exchange field.
        metadata.loc[sid] = (
            start_date,
            end_date,
            auto_close_date,
            sql_symbol,
            calendar.name,
            symbol,
        )
        if len(df) > 0:
            yield sid, df
    return


def process_minute_table(
    table,
    sessions,
    minutes,
    metadata,
    calendar,
    symbol_to_sid: dict[str, int],
    dates_with_data: set[pd.Timestamp],
    agg_time: str,
):
    table = rename_polygon_to_zipline(table, "timestamp")
    # print(f"{minutes[:5]=}\n{minutes[-5:]=}")
    table = table.filter(pyarrow.compute.field("timestamp").isin(minutes))
    # print(f"filtered {table.num_rows=}")
    table_df = table.to_pandas()
    # print(f"{table_df.head()=}")
    for symbol, df in table_df.groupby("symbol"):
        # print(f"\n{symbol=} {len(df)=} {df['timestamp'].min()} {df['timestamp'].max()}")
        if symbol not in symbol_to_sid:
            symbol_to_sid[symbol] = len(symbol_to_sid) + 1
        sid = symbol_to_sid[symbol]
        # The SQL schema zipline uses for symbols ignores case
        sql_symbol = symbol_to_upper(symbol)
        df["symbol"] = sql_symbol
        df = df.set_index("timestamp")
        # Shouldn't need to do this because the table is sorted.
        if not df.index.is_monotonic_increasing:
            print(f" INFO: {symbol=} {sid=} not monotonic increasing")
            df.sort_index(inplace=True)
        if agg_time == AGG_TIME_DAY:
            df.drop(columns=["symbol", "transactions"], inplace=True)
            # Remember first and last date.
            start_date = df.index[0].tz_convert(calendar.tz.key).normalize()
            dates_with_data.add(start_date)
            end_date = df.index[-1].tz_convert(calendar.tz.key).normalize()
            dates_with_data.add(end_date)
            df = df[df.index.isin(minutes)]
            len_before = len(df)
            # print(f"{start_date=} {end_date=} {dates_with_data=}")
            # print(f"day pre {df.head()=}")
            if len(df) < 1:
                # TODO: Move sid assignment until after this check for no data.
                print(
                    f" WARNING: No data for {symbol=} {sid=} {len_before=} {start_date=} {end_date=}"
                )
                continue
            df = minute_frame_to_session_frame(df, calendar)
            # print(f"day sess {df.head()=}")
            # df["symbol"] = sql_symbol
            df = df[df.index.isin(sessions)]

            # The auto_close date is the day after the last trade.
            # auto_close_date = end_date + pd.Timedelta(days=1)
            auto_close_date = None

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
                metadata.loc[sid, "auto_close_date"] = auto_close_date
            else:
                # Add a row to the metadata DataFrame. Don't forget to add an exchange field.
                # metadata = pd.DataFrame(
                #     columns=(
                #         "start_date",
                #         "end_date",
                #         "auto_close_date",
                #         "symbol",
                #         "exchange",
                #         "asset_name",
                #     )
                # )
                metadata.loc[sid] = (
                    start_date,
                    end_date,
                    auto_close_date,
                    sql_symbol,
                    calendar.name,
                    symbol,
                )
            # df = df.reindex(sessions.tz_localize(None))
            df = df.reindex(sessions)
            # Missing volume and transactions are zero
            df["volume"] = df["volume"].fillna(0)
            # df["transactions"] = df["transactions"].fillna(0)
            # Forward fill missing price data (better than backfill)
            # TODO: These fills should have the same price for OHLC (open for backfill, close for forward fill)
            df.ffill(inplace=True)
            # Back fill missing data (maybe necessary for before the first day bar)
            # TODO: Don't want to backfill future values.  What's better here?
            df.bfill(inplace=True)
            if len(df) > 0:
                # print(f"\n{symbol=} {sid=} {len_before=} {start_timestamp=} {end_date=} {end_timestamp=} {len(df)=}")
                yield sid, df
            else:
                print(
                    f" WARNING: No day bars for {symbol=} {sid=} {len_before=} {start_date=} {start_date=} {end_date=} {end_date=}"
                )
        else:
            len_before = len(df)
            df = df[df.index.isin(minutes)]
            if len(df) < 2:
                print(
                    f" WARNING: Not enough data for {symbol=} {sid=} {len(df)=} {len_before=}"
                )
                continue

            # A df with 1 bar crashes zipline/data/bcolz_minute_bars.py", line 747
            # pd.Timestamp(dts[0]), direction="previous"
            if len(df) > 1:
                yield sid, df
            else:
                print(
                    f" WARNING: Not enough minute bars for {symbol=} {sid=} {len(df)=}"
                )
    return


def process_aggregates(
    process_table_func,
    fragments,
    sessions,
    minutes,
    metadata,
    calendar,
    symbol_to_sid: dict[str, int],
    dates_with_data: set[pd.Timestamp],
    agg_time: str,
):
    # We do this by Hive partition at a time because each ticker will be complete.
    for fragment in fragments:
        # Only get the columns Zipline allows.
        table = fragment.to_table(
            columns=[
                "ticker",
                "window_start",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "transactions",
            ]
        )
        table = table.sort_by([("ticker", "ascending"), ("window_start", "ascending")])
        yield from process_table_func(
            table=table,
            sessions=sessions,
            minutes=minutes,
            metadata=metadata,
            calendar=calendar,
            symbol_to_sid=symbol_to_sid,
            dates_with_data=dates_with_data,
            agg_time=agg_time,
        )
        del table

    # This doesn't seem to be hardly any faster than the above, something with the GIL?
    # Also to use this we'd need to make sure the symbol_to_sid and dates_with_data are thread safe.
    # with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
    #     futures = [
    #         executor.submit(
    #             process_minute_fragment,
    #             fragment,
    #             sessions,
    #             minutes,
    #             metadata,
    #             calendar,
    #             symbol_to_sid,
    #             dates_with_data,
    #             agg_time,
    #         )
    #         for fragment in fragments
    #     ]
    #     for future in concurrent.futures.as_completed(futures):
    #         yield from future.result()


def ingest_polygon_equities_bundle(
    agg_time: str,
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_date,
    end_date,
    cache,
    show_progress,
    output_dir,
):
    config = PolygonConfig(
        environ=environ,
        calendar_name=calendar.name,
        start_date=start_date,
        end_date=end_date,
        agg_time=agg_time,
    )

    print(f"{calendar.name=} {start_date=} {end_date=}")
    print(f"{calendar.sessions_in_range(start_date, end_date)[:4]}")
    print(f"{calendar.sessions_in_range(start_date, end_date)[-4:]}")
    print(f"{calendar.sessions_minutes(start_date, end_date)[:4]}")
    print(f"{calendar.sessions_minutes(start_date, end_date)[-4:]}")

    if agg_time in [AGG_TIME_TRADES, "1min", "1minute"]:
        convert_trades_to_custom_aggs(config, overwrite=False)
        by_ticker_aggs_arrow_dir = scatter_custom_aggs_to_by_ticker(config)
    else:
        by_ticker_aggs_arrow_dir = concat_all_aggs_from_csv(config)
    aggregates = pyarrow.dataset.dataset(by_ticker_aggs_arrow_dir)
    # print(f"{aggregates.schema=}")
    # 3.5 billion rows for 10 years of minute data.
    # print(f"{aggregates.count_rows()=}")
    # Can't sort the dataset because that reads it all into memory.
    # aggregates = aggregates.sort_by([("ticker", "ascending"), ("window_start", "ascending")])
    # print("Sorted")

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

    symbol_to_sid = {}
    # Keep track of earliest and latest dates with data across all symbols.
    dates_with_data = set()

    # Get data for all stocks and write to Zipline
    daily_bar_writer.write(
        process_aggregates(
            process_day_table if config.agg_time == AGG_TIME_DAY else process_minute_table,
            fragments=aggregates.get_fragments(),
            sessions=calendar.sessions_in_range(start_date, end_date),
            minutes=calendar.sessions_minutes(start_date, end_date),
            metadata=metadata,
            calendar=calendar,
            symbol_to_sid=symbol_to_sid,
            dates_with_data=dates_with_data,
            agg_time=AGG_TIME_DAY,
        ),
        show_progress=show_progress,
    )

    if config.agg_time != AGG_TIME_DAY:
        minute_bar_writer.write(
            process_aggregates(
                process_minute_table,
                fragments=aggregates.get_fragments(),
                sessions=calendar.sessions_in_range(start_date, end_date),
                minutes=calendar.sessions_minutes(start_date, end_date),
                metadata=metadata,
                calendar=calendar,
                symbol_to_sid=symbol_to_sid,
                dates_with_data=dates_with_data,
                agg_time=AGG_TIME_MINUTE,
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


def ingest_polygon_equities_bundle_for_agg_time(agg_time: str):
    def ingest_polygon_equities_bundle_inner(
        environ,
        asset_db_writer,
        minute_bar_writer,
        daily_bar_writer,
        adjustment_writer,
        calendar,
        start_date,
        end_date,
        cache,
        show_progress,
        output_dir,
    ):
        return ingest_polygon_equities_bundle(
            agg_time=agg_time,
            environ=environ,
            asset_db_writer=asset_db_writer,
            minute_bar_writer=minute_bar_writer,
            daily_bar_writer=daily_bar_writer,
            adjustment_writer=adjustment_writer,
            calendar=calendar,
            start_date=start_date,
            end_date=end_date,
            cache=cache,
            show_progress=show_progress,
            output_dir=output_dir,
        )

    return ingest_polygon_equities_bundle_inner


def register_polygon_equities_bundle(
    bundlename,
    start_date=None,
    end_date=None,
    calendar_name="XNYS",
    agg_time=AGG_TIME_DAY,
    minutes_per_day=390,
    environ=os.environ,
    # ticker_list=None,
    # watchlists=None,
    # include_asset_types=None,
):
    register_nyse_all_hours_calendar()

    # pd.set_option("display.max_columns", None)
    # pd.set_option("display.width", 500)

    # Note that "minute" is the Polygon minute aggs and "1minute" is the trades.
    if agg_time not in [AGG_TIME_DAY, AGG_TIME_MINUTE, AGG_TIME_TRADES, "1min", "1minute"]:
        raise ValueError(
            f"agg_time must be 'day', 'minute' (aggs), or '1minute' (trades), not '{agg_time}'"
        )

    # We need to know the start and end dates of the session before the bundle is
    # registered because even though we only need it for ingest, the metadata in
    # the writer is initialized and written before our ingest function is called.
    if start_date is None or end_date is None:
        config = PolygonConfig(
            environ=environ,
            calendar_name=calendar_name,
            start_date=start_date,
            end_date=end_date,
            agg_time=agg_time,
        )
        first_aggs_date, last_aggs_date = config.find_first_and_last_aggs(
            config.aggs_dir if agg_time in [AGG_TIME_DAY, AGG_TIME_MINUTE] else config.trades_dir,
            config.csv_paths_pattern,
        )
        # print(f"{bundlename=} {first_aggs_date=} {last_aggs_date=}")
        if start_date is None:
            start_date = first_aggs_date
        if end_date is None:
            end_date = last_aggs_date

    start_session = parse_date(start_date, raise_oob=False) if start_date else None
    end_session = parse_date(end_date, raise_oob=False) if end_date else None
    # print(f"Registered {bundlename=} {agg_time=} {start_session=} {end_session=}")

    register(
        bundlename,
        ingest_polygon_equities_bundle_for_agg_time(agg_time),
        start_session=start_session,
        end_session=end_session,
        calendar_name=calendar_name,
        minutes_per_day=minutes_per_day,
        # create_writers=True,
    )


# if __name__ == "__main__":
#     logging.basicConfig(level=logging.WARNING)
#     os.environ["POLYGON_MIRROR_DIR"] = "/Volumes/Oahu/Mirror/files.polygon.io"
#     os.environ["ZIPLINE_ROOT"] = "/Volumes/Oahu/Workspaces/zipline"
#     config = PolygonConfig(
#         environ=os.environ,
#         calendar_name="XNYS",
#         # start_date="2003-10-01",
#         # start_date="2018-01-01",
#         start_date="2023-01-01",
#         # end_date="2023-01-12",
#         end_date="2023-12-31",
#         # end_date="2024-06-30",
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
