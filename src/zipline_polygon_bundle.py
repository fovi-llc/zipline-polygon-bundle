import pyarrow.compute
from zipline.data.bundles import register

from config import PolygonConfig
from tickers_and_names import PolygonAssets
from concat_all_aggs import concat_all_aggs_from_csv

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


def polygon_equities_bundle(
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
    config = PolygonConfig(environ=environ, calendar_name=calendar.name, start_session=start_session, end_session=end_session)
    assert calendar == config.calendar

    # Empty DataFrames for dividends, splits, and metadata
    divs = pd.DataFrame(
        columns=["sid", "amount", "ex_date", "record_date", "declared_date", "pay_date"]
    )
    splits = pd.DataFrame(columns=["sid", "ratio", "effective_date"])
    metadata = pd.DataFrame(
        columns=("start_date", "end_date", "auto_close_date", "symbol", "exchange")
    )

    if not os.path.exists(config.by_ticker_hive_dir):
        concat_all_aggs_from_csv(config)

    aggregates = pyarrow.dataset.dataset(config.by_ticker_hive_dir)

    # Check valid trading dates, according to the selected exchange calendar
    sessions = calendar.sessions_in_range(start_session, end_session)
    # sessions = calendar.sessions_in_range('1995-05-02', '2020-05-27')

    # Get data for all stocks and write to Zipline
    daily_bar_writer.write(process_aggregates(aggregates, sessions, metadata))

    # Write the metadata
    asset_db_writer.write(equities=metadata)

    # Write splits and dividends
    adjustment_writer.write(splits=splits, dividends=divs)


def process_aggregates(aggregates, sessions, metadata):
    # yield sid, df
    table = aggregates.to_table()
    # print(f"{table=}")
    table = table.rename_columns({'ticker': 'symbol', 'window_start': 'day'})
    symbols = sorted(list(set(table.column("symbol").to_pylist())))
    print(f"{len(symbols)=}")
    print(f"{symbols[0:10]=}")
    for sid, symbol in enumerate(symbols):
        print(f"{sid=}, {symbol=}")

        df = table.filter(pyarrow.compute.field("symbol") == pyarrow.scalar(symbol)).to_pandas()
        df = df.set_index("day")
        yield sid, df
    return


def register_polygon_equities_bundle(
    bundlename,
    start_session=pd.Timestamp("2023-01-03"),
    # end_session="now",
    end_session=pd.Timestamp("2023-12-28"),
    calendar_name="XNYS",
    # ticker_list=None,
    # watchlists=None,
    # include_asset_types=None,
):
    register(
        bundlename,
        polygon_equities_bundle,
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
#     print(f"{get_ticker_universe(config, fetch_missing=True)}")
