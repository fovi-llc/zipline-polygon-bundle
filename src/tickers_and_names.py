from config import PolygonConfig

import datetime
import os
import pandas as pd
import polygon
import logging
from concurrent.futures import ProcessPoolExecutor


def fetch_and_save_tickers_for_date(config: PolygonConfig, date: pd.Timestamp):
    logging.info("fetch_and_save_tickers_for_date")
    try:
        logging.info(f"Fetching tickers for {date}")
        assets = PolygonAssets(config)
        all_tickers = assets.fetch_all_tickers(date=date)
        assets.save_tickers_for_date(all_tickers, date)
    except Exception as e:
        logging.error(f"Error fetching tickers for {date}: {e}")


class PolygonAssets:
    def __init__(self, config: PolygonConfig):
        self.config = config
        self.polygon_client = polygon.RESTClient(api_key=config.api_key)

    def fetch_tickers(
        self,
        date: pd.Timestamp,
        active: bool = True,
    ):
        response = self.polygon_client.list_tickers(
            market=self.config.market, active=active, date=date.date(), limit=500
        )
        tickers_df = pd.DataFrame(list(response))
        tickers_df.drop(
            columns=[
                "currency_symbol",
                "base_currency_symbol",
                "base_currency_name",
                "source_feed",
            ],
            inplace=True,
        )

        # Test tickers have no CIK value.
        tickers_df = tickers_df.dropna(subset=["ticker", "cik"])
        tickers_df["request_date"] = date
        tickers_df["last_updated_utc"] = pd.to_datetime(tickers_df["last_updated_utc"])
        tickers_df["delisted_utc"] = pd.to_datetime(tickers_df["delisted_utc"])

        tickers_df.set_index(["ticker", "cik", "request_date", "active"], inplace=True)

        return tickers_df

    def validate_active_tickers(self, tickers: pd.DataFrame):
        # # All tickers are active
        # inactive_tickers = (
        #     tickers[~tickers.index.get_level_values("active")]
        #     .index.get_level_values("ticker")
        #     .tolist()
        # )
        # assert (
        #     not inactive_tickers
        # ), f"{len(inactive_tickers)} tickers are not active: {inactive_tickers[:15]}"

        # No tickers with missing last_updated_utc
        missing_last_updated_utc_tickers = (
            tickers[tickers["last_updated_utc"].isnull()]
            .index.get_level_values("ticker")
            .tolist()
        )
        assert (
            not missing_last_updated_utc_tickers
        ), f"{len(missing_last_updated_utc_tickers)} tickers have missing last_updated_utc: {missing_last_updated_utc_tickers[:15]}"

        # No tickers with missing name
        missing_name_tickers = (
            tickers[tickers["name"].isnull()].index.get_level_values("ticker").tolist()
        )
        if missing_name_tickers:
            logging.warning(
                f"{len(missing_name_tickers)} tickers have missing name: {missing_name_tickers[:15]}"
            )
        # assert (
        #     not missing_name_tickers
        # ), f"{len(missing_name_tickers)} tickers have missing name: {missing_name_tickers[:15]}"

        # No tickers with missing locale
        missing_locale_tickers = (
            tickers[tickers["locale"].isnull()]
            .index.get_level_values("ticker")
            .tolist()
        )
        assert (
            not missing_locale_tickers
        ), f"{len(missing_locale_tickers)} tickers have missing locale: {missing_locale_tickers[:15]}"

        # No tickers with missing market
        missing_market_tickers = (
            tickers[tickers["market"].isnull()]
            .index.get_level_values("ticker")
            .tolist()
        )
        assert (
            not missing_market_tickers
        ), f"{len(missing_market_tickers)} tickers have missing market: {missing_market_tickers[:15]}"

        # No tickers with missing primary exchange
        missing_primary_exchange_tickers = (
            tickers[tickers["primary_exchange"].isnull()]
            .index.get_level_values("ticker")
            .tolist()
        )
        # We'll just warn here and filter in the merge_tickers function.
        if missing_primary_exchange_tickers:
            logging.warning(
                f"{len(missing_primary_exchange_tickers)} tickers have missing primary exchange: {missing_primary_exchange_tickers[:15]}"
            )
        # assert (
        #     not missing_primary_exchange_tickers
        # ), f"{len(missing_primary_exchange_tickers)} tickers have missing primary exchange: {missing_primary_exchange_tickers[:15]}"

        # No tickers with missing type
        missing_type_tickers = (
            tickers[tickers["type"].isnull()].index.get_level_values("ticker").tolist()
        )
        # assert not missing_type_tickers, f"{len(missing_type_tickers)} tickers have missing type: {missing_type_tickers[:15]}"
        # Just a warning because there are legit tickers with missing type
        if missing_type_tickers:
            logging.warning(
                f"{len(missing_type_tickers)} tickers have missing type: {missing_type_tickers[:15]}"
            )

        # No tickers with missing currency
        missing_currency_tickers = (
            tickers[tickers["currency_name"].isnull()]
            .index.get_level_values("ticker")
            .tolist()
        )
        assert (
            not missing_currency_tickers
        ), f"{len(missing_currency_tickers)} tickers have missing currency_name: {missing_currency_tickers[:15]}"

    def fetch_all_tickers(self, date: pd.Timestamp):
        all_tickers = pd.concat(
            [
                self.fetch_tickers(date=date, active=True),
                self.fetch_tickers(date=date, active=False),
            ]
        )

        # Reset the index of the DataFrame
        all_tickers.reset_index(inplace=True)

        all_tickers.set_index(["ticker", "cik", "active", "request_date"], inplace=True)

        # We're keeping these tickers with missing type because it is some Polygon bug.
        # # Drop rows with no type.  Not sure why this happens but we'll ignore them for now.
        # active_tickers = active_tickers.dropna(subset=["type"])

        all_tickers = all_tickers.astype(
            {
                "composite_figi": "string",
                "currency_name": "string",
                "locale": "string",
                "market": "string",
                "name": "string",
                "primary_exchange": "string",
                "share_class_figi": "string",
                "type": "string",
            }
        )

        all_tickers.sort_index(inplace=True)

        self.validate_active_tickers(all_tickers)

        return all_tickers

    def ticker_file_exists(self, date: pd.Timestamp):
        return os.path.exists(self.config.ticker_file_path(date))

    def save_tickers_for_date(self, tickers: pd.DataFrame, date: pd.Timestamp):
        tickers.to_parquet(self.config.ticker_file_path(date))

    def load_tickers_for_date(self, date: pd.Timestamp):
        try:
            return pd.read_parquet(self.config.ticker_file_path(date))
        except (FileNotFoundError, IOError) as e:
            logging.error(f"Error loading tickers for {date}: {e}")
            try:
                # Remove the file so that it can be fetched again
                os.remove(self.config.ticker_file_path(date))
            except (FileNotFoundError, IOError) as e2:
                logging.error(
                    f"Error removing file {self.config.ticker_file_path(date)}: {e2}"
                )
            return None

    def load_all_tickers(self, fetch_missing=False, max_workers=None):
        dates = list(
            self.config.calendar.trading_index(
                start=self.config.start_timestamp,
                end=self.config.end_timestamp,
                period="1D",
            )
        )
        if fetch_missing:
            missing_dates = [
                date for date in dates if not self.ticker_file_exists(date)
            ]
            print(f"{len(missing_dates)=}")
            if missing_dates:
                if max_workers == 0:
                    for date in missing_dates:
                        fetch_and_save_tickers_for_date(self.config, date)
                else:
                    print("Starting process pool executor")
                    with ProcessPoolExecutor(max_workers=max_workers) as executor:
                        executor.map(
                            fetch_and_save_tickers_for_date,
                            [self.config] * len(missing_dates),
                            missing_dates,
                        )
        dates_with_files = [date for date in dates if self.ticker_file_exists(date)]
        if fetch_missing and (len(dates_with_files) != len(dates)):
            logging.warning(
                f"Only {len(dates_with_files)} of {len(dates)} dates have files."
            )
        all_tickers = pd.concat(
            [self.load_tickers_for_date(date) for date in dates_with_files]
        )
        all_tickers.info()
        return all_tickers

    def merge_tickers(self, all_tickers: pd.DataFrame):
        all_tickers.reset_index(inplace=True)

        # Make sure there are no leading or trailing spaces in the column values
        all_tickers = all_tickers.map(lambda x: x.strip() if isinstance(x, str) else x)

        # Drops rows with missing primary exchange (rare but means it isn't actually active).
        all_tickers.dropna(subset=["name", "primary_exchange"], inplace=True)

        # # Only keep rows with type CS, ETF, or ADRC
        # all_tickers = all_tickers[all_tickers["type"].isin(["CS", "ETF", "ADRC"])]

        merged_tickers = (
            all_tickers.sort_values(by="request_date")
            .groupby(
                ["ticker", "cik", "type", "share_class_figi", "active"], dropna=False
            )
            .agg(
                {
                    "request_date": ["min", "max"],
                    "name": "unique",
                    "composite_figi": "unique",
                    "primary_exchange": "unique",
                    "currency_name": "unique",
                    "locale": "unique",
                    "market": "unique",
                }
            )
            .sort_values(by=("request_date", "max"), ascending=False)
        )

        # Flatten the multi-level column index
        merged_tickers.columns = [
            "_".join(col).strip() for col in merged_tickers.columns.values
        ]

        # Rename the columns
        merged_tickers.rename(
            columns={"request_date_min": "start_date", "request_date_max": "end_date"},
            inplace=True,
        )
        merged_tickers.rename(
            columns=lambda x: x.removesuffix("_unique"),
            inplace=True,
        )

        return merged_tickers


# Initialize ticker files in __main__.  Use CLI args to specify start and end dates.
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Initialize ticker files.")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in ISO format (YYYY-MM-DD)",
        default="2014-05-01",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in ISO format (YYYY-MM-DD)",
        default="2024-04-01",
    )
    args = parser.parse_args()

    start_date = (
        datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        if args.start_date
        else datetime.date.today()
    )
    end_date = (
        datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()
        if args.end_date
        else datetime.date.today()
    )

    all_tickers = load_all_tickers(start_date, end_date, fetch_missing=True)
    merged_tickers = merge_tickers(all_tickers)
    merged_tickers.to_csv(f"data/tickers/us_tickers_{start_date}-{end_date}.csv")
    ticker_names = ticker_names_from_merged_tickers(merged_tickers)
    print(ticker_names)
