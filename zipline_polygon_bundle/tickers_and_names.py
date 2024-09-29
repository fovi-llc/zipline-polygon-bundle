from .config import PolygonConfig

import datetime
import os
import pandas as pd
import polygon
import logging
from concurrent.futures import ProcessPoolExecutor


def configure_logging():
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(processName)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
    )


def fetch_and_save_tickers_for_date(config: PolygonConfig, date: pd.Timestamp):
    try:
        configure_logging()
        logger = logging.getLogger()
        logger.info(f"Fetching tickers for {date}")
        assets = PolygonAssets(config)
        all_tickers = assets.fetch_all_tickers(date=date)
        assets.save_tickers_for_date(all_tickers, date)
    except Exception as e:
        logger.exception(f"Error fetching tickers for {date}: {e}")


def unique_simple_list(x):
    return [str(y) for y in x.unique() if pd.notnull(y)]


def unique_simple_date_list(x):
    return [y.date().isoformat() for y in x.unique() if pd.notnull(y)]


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
        # The currency info is for crypto.  The source_feed is always NA.
        tickers_df.drop(
            columns=[
                "currency_symbol",
                "base_currency_symbol",
                "base_currency_name",
                "source_feed",
            ],
            inplace=True,
        )

        tickers_df["request_date"] = date
        tickers_df["last_updated_utc"] = pd.to_datetime(tickers_df["last_updated_utc"])
        tickers_df["delisted_utc"] = pd.to_datetime(tickers_df["delisted_utc"])

        # Make sure there are no leading or trailing spaces in the column values
        for col in tickers_df.columns:
            if tickers_df[col].dtype == "string":
                # Still gotta check value types because of NA values.
                tickers_df[col] = tickers_df[col].apply(
                    lambda x: x.strip() if isinstance(x, str) else x
                )

        # Test tickers have no CIK value.
        # Actually sometimes listed tickers have no CIK value.
        # Case in point is ALTL ETF which doesn't include a CIK in the Ticker search response for 2024-07-01
        # but did on 2024-06-25.  Suspiciouly 4 years after the listing date of 2020-06-25.
        # We'll have to leave this cleaning of test tickers until we can call Ticker Details.
        # tickers_df = tickers_df.dropna(subset=["ticker", "cik"])

        return tickers_df

    def validate_active_tickers(self, tickers: pd.DataFrame):
        # # All tickers are active
        # inactive_tickers = (
        #     tickers[~tickers.index.get_level_values("active")]["ticker"].unique().tolist()
        # )
        # assert (
        #     not inactive_tickers
        # ), f"{len(inactive_tickers)} tickers are not active: {inactive_tickers[:15]}"

        # No tickers with missing last_updated_utc
        missing_last_updated_utc_tickers = (
            tickers[tickers["last_updated_utc"].isnull()]["ticker"].unique().tolist()
        )
        assert (
            not missing_last_updated_utc_tickers
        ), f"{len(missing_last_updated_utc_tickers)} tickers have missing last_updated_utc: {missing_last_updated_utc_tickers[:15]}"

        # # No tickers with missing name
        # missing_name_tickers = (
        #     tickers[tickers["name"].isnull()]["ticker"].unique().tolist()
        # )
        # if missing_name_tickers:
        #     logging.warning(
        #         f"{len(missing_name_tickers)} tickers have missing name: {missing_name_tickers[:15]}"
        #     )
        # assert (
        #     not missing_name_tickers
        # ), f"{len(missing_name_tickers)} tickers have missing name: {missing_name_tickers[:15]}"

        # No tickers with missing locale
        missing_locale_tickers = (
            tickers[tickers["locale"].isnull()]["ticker"].unique().tolist()
        )
        assert (
            not missing_locale_tickers
        ), f"{len(missing_locale_tickers)} tickers have missing locale: {missing_locale_tickers[:15]}"

        # No tickers with missing market
        missing_market_tickers = (
            tickers[tickers["market"].isnull()]["ticker"].unique().tolist()
        )
        assert (
            not missing_market_tickers
        ), f"{len(missing_market_tickers)} tickers have missing market: {missing_market_tickers[:15]}"

        # # No tickers with missing primary exchange
        # missing_primary_exchange_tickers = (
        #     tickers[tickers["primary_exchange"].isnull()]
        #     .index.get_level_values("ticker")
        #     .tolist()
        # )
        # # We'll just warn here and filter in the merge_tickers function.
        # if missing_primary_exchange_tickers:
        #     logging.warning(
        #         f"{len(missing_primary_exchange_tickers)} tickers have missing primary exchange: {missing_primary_exchange_tickers[:15]}"
        #     )
        # assert (
        #     not missing_primary_exchange_tickers
        # ), f"{len(missing_primary_exchange_tickers)} tickers have missing primary exchange: {missing_primary_exchange_tickers[:15]}"

        # # No tickers with missing type
        # missing_type_tickers = (
        #     tickers[tickers["type"].isnull()].index.get_level_values("ticker").tolist()
        # )
        # # assert not missing_type_tickers, f"{len(missing_type_tickers)} tickers have missing type: {missing_type_tickers[:15]}"
        # # Just a warning because there are legit tickers with missing type
        # if missing_type_tickers:
        #     logging.warning(
        #         f"{len(missing_type_tickers)} tickers have missing type: {missing_type_tickers[:15]}"
        #     )

        # No tickers with missing currency
        missing_currency_tickers = (
            tickers[tickers["currency_name"].isnull()]["ticker"].unique().tolist()
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

        # We're keeping these tickers with missing type because it is some Polygon bug.
        # # Drop rows with no type.  Not sure why this happens but we'll ignore them for now.
        # active_tickers = active_tickers.dropna(subset=["type"])

        self.validate_active_tickers(all_tickers)

        return all_tickers

    def ticker_file_exists(self, date: pd.Timestamp):
        return os.path.exists(self.config.ticker_file_path(date))

    def save_tickers_for_date(self, tickers: pd.DataFrame, date: pd.Timestamp):
        tickers.to_parquet(self.config.ticker_file_path(date))

    def load_tickers_for_date(self, date: pd.Timestamp):
        try:
            tickers = pd.read_parquet(self.config.ticker_file_path(date))
            return tickers
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
                max_workers = (
                    max_workers if max_workers is not None else self.config.max_workers
                )
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
        return all_tickers

    def merge_tickers(self, all_tickers: pd.DataFrame):
        all_tickers.set_index(
            ["ticker", "primary_exchange", "cik", "type", "composite_figi", "active"],
            drop=False,
            inplace=True,
        )
        all_tickers.sort_index(inplace=True)

        # Make sure there are no leading or trailing spaces in the column values
        # This also does some kind of change to the type for the StringArray values which makes Arrow Parquet happy.
        all_tickers = all_tickers.map(lambda x: x.strip() if isinstance(x, str) else x)

        # # Drops rows with missing primary exchange (rare but means it isn't actually active).
        # all_tickers.dropna(subset=["name", "primary_exchange"], inplace=True)

        # # Only keep rows with type CS, ETF, or ADRC
        # all_tickers = all_tickers[all_tickers["type"].isin(["CS", "ETF", "ADRC"])]

        merged_tickers = (
            all_tickers.groupby(
                level=[
                    "ticker",
                    "primary_exchange",
                    "cik",
                    "type",
                    "composite_figi",
                    "active",
                ],
                dropna=False,
            )
            .agg(
                {
                    "request_date": ["min", "max"],
                    "last_updated_utc": lambda x: x.max().date(),
                    "name": "unique",
                    "share_class_figi": unique_simple_list,
                    "delisted_utc": unique_simple_date_list,
                    "currency_name": "unique",
                    "locale": "unique",
                    "market": "unique",
                }
            )
            .sort_values(by=("request_date", "max"))
        )

        # Flatten the multi-level column index
        merged_tickers.columns = [
            "_".join(col).strip() for col in merged_tickers.columns.values
        ]

        # Rename the columns
        merged_tickers.rename(
            columns={
                "request_date_min": "start_date",
                "request_date_max": "end_date",
                "last_updated_utc_<lambda>": "last_updated_utc",
                "share_class_figi_unique_simple_list": "share_class_figi",
                "delisted_utc_unique_simple_date_list": "delisted_utc",
            },
            inplace=True,
        )
        merged_tickers.rename(
            columns=lambda x: x.removesuffix("_unique"),
            inplace=True,
        )

        all_tickers.sort_index(inplace=True)
        return merged_tickers


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
