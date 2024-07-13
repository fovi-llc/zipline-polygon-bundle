from zipline.data.bundles import register

from config import PolygonConfig
from tickers_and_names import PolygonAssets

import pandas as pd
import os


def get_ticker_universe(config: PolygonConfig, fetch_missing: bool = False):
    assets = PolygonAssets(config)
    tickers_csv_path = config.tickers_csv_path
    print(f"{tickers_csv_path=}")
    if not os.path.exists(tickers_csv_path) or not os.path.exists(
        tickers_csv_path.removesuffix(".csv") + ".parquet"
    ):
        all_tickers = assets.load_all_tickers(fetch_missing=fetch_missing)
        # all_tickers.to_csv(tickers_csv_path)
        merged_tickers = assets.merge_tickers(all_tickers)
        merged_tickers.to_csv(tickers_csv_path)
        print(f"Saved {len(merged_tickers)} tickers to {tickers_csv_path}")
        merged_tickers.to_parquet(tickers_csv_path.removesuffix(".csv") + ".parquet")
        print(
            f"Saved {len(merged_tickers)} tickers to {tickers_csv_path.removesuffix('.csv') + '.parquet'}"
        )
    merged_tickers = pd.read_csv(
        tickers_csv_path,
        #  dtype={'ticker': str, 'name': str, 'exchange': str, 'composite_figi': str, 'currency_name': str,
        #         'locale': str, 'market': str, 'primary_exchange':str, 'share_class_figi': str, 'type': str},
        converters={
            "ticker": lambda x: str(x).strip(),
            "start_date": lambda x: pd.to_datetime(x),
            "cik": lambda x: int(x),
            "name": lambda x: str(x).strip(),
            "end_date": lambda x: pd.to_datetime(x),
            "composite_figi": lambda x: str(x).strip().upper(),
            "share_class_figi": lambda x: str(x).strip().upper(),
            "currency_name": lambda x: str(x).strip().lower(),
            "locale": lambda x: str(x).strip().lower(),
            "market": lambda x: str(x).strip().lower(),
            "primary_exchange": lambda x: str(x).strip().upper(),
            "type": lambda x: str(x).strip().upper(),
        },
    )
    merged_tickers.info()
    return merged_tickers


def polygon_equities_bundle(
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar_name,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
):
    config = PolygonConfig(environ=environ, calendar_name=calendar_name)
    if config.api_key is None:
        raise ValueError(
            "Please set your POLYGON_API_KEY environment variable and retry."
        )

    raw_data = fetch_data_table(
        api_key, show_progress, environ.get("QUANDL_DOWNLOAD_ATTEMPTS", 5)
    )
    asset_metadata = gen_asset_metadata(raw_data[["symbol", "date"]], show_progress)

    exchanges = pd.DataFrame(
        data=[["SIP", "SIP", "US"]],
        columns=["exchange", "canonical_name", "country_code"],
    )
    asset_db_writer.write(equities=asset_metadata, exchanges=exchanges)

    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)

    raw_data.set_index(["date", "symbol"], inplace=True)
    daily_bar_writer.write(
        parse_pricing_and_vol(raw_data, sessions, symbol_map),
        show_progress=show_progress,
    )

    raw_data.reset_index(inplace=True)
    raw_data["symbol"] = raw_data["symbol"].astype("category")
    raw_data["sid"] = raw_data.symbol.cat.codes
    adjustment_writer.write(
        splits=parse_splits(
            raw_data[
                [
                    "sid",
                    "date",
                    "split_ratio",
                ]
            ].loc[raw_data.split_ratio != 1],
            show_progress=show_progress,
        ),
        dividends=parse_dividends(
            raw_data[
                [
                    "sid",
                    "date",
                    "ex_dividend",
                ]
            ].loc[raw_data.ex_dividend != 0],
            show_progress=show_progress,
        ),
    )


def register_polygon_equities_bundle(
    bundlename,
    start_session="2000-01-01",
    end_session="now",
    calendar_name="XYNS",
    ticker_list=None,
    watchlists=None,
    include_asset_types=None,
):
    register(
        bundlename,
        polygon_equities_bundle,
        start_session=start_session,
        end_session=end_session,
        calendar_name=calendar_name,
    )


if __name__ == "__main__":
    config = PolygonConfig(
        environ=os.environ,
        calendar_name="XNYS",
        start_session="2014-01-01",
        end_session="2024-07-01",
        # start_session="2023-01-01",
        # end_session="2023-01-15",
    )
    print(f"{get_ticker_universe(config, fetch_missing=True)}")
