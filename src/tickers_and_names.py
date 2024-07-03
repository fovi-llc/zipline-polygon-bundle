from config import get_tickers_dir

import datetime
import os
import pandas as pd
import polygon
import logging
import functools

from zipline.utils.calendar_utils import get_calendar


@functools.lru_cache(maxsize=None)
def get_polygon_client(api_key: str = os.getenv("POLYGON_API_KEY")):
    return polygon.RESTClient(api_key=api_key)


def polygon_get_tickers(
    date: datetime.date = datetime.date.today(), active: bool = True
):
    response = get_polygon_client().list_tickers(
        market="stocks", active=active, date=date, limit=500
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
    tickers_df = tickers_df.dropna(subset=["ticker", "cik"])
    tickers_df["request_date"] = pd.to_datetime(date)
    tickers_df["last_updated_utc"] = pd.to_datetime(tickers_df["last_updated_utc"])
    tickers_df["delisted_utc"] = pd.to_datetime(tickers_df["delisted_utc"])

    # Group by 'ticker', 'cik' and get the index of the row with the newest 'last_updated_utc' in each group
    idx = tickers_df.groupby(["ticker", "cik"])["last_updated_utc"].idxmax()

    # Use the indices to get the rows with the newest 'last_updated_utc'
    tickers_df = tickers_df.loc[idx]

    tickers_df.set_index(["ticker", "cik", "request_date", "active"], inplace=True)

    return tickers_df


def validate_active_tickers(tickers: pd.DataFrame, only_one_row_per_ticker=False):
    # All tickers are active
    inactive_tickers = (
        tickers[~tickers.index.get_level_values("active")]
        .index.get_level_values("ticker")
        .tolist()
    )
    assert (
        not inactive_tickers
    ), f"{len(inactive_tickers)} tickers are not active: {inactive_tickers[:15]}"

    if only_one_row_per_ticker:
        # Only one row per ticker
        ticker_counts = tickers.index.get_level_values("ticker").value_counts()
        tickers_with_more_than_one_row = ticker_counts[ticker_counts > 1].index.tolist()
        assert (
            not tickers_with_more_than_one_row
        ), f"{len(tickers_with_more_than_one_row)} tickers with more than one row: {tickers_with_more_than_one_row[:15]}"

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
        tickers[tickers["locale"].isnull()].index.get_level_values("ticker").tolist()
    )
    assert (
        not missing_locale_tickers
    ), f"{len(missing_locale_tickers)} tickers have missing locale: {missing_locale_tickers[:15]}"

    # No tickers with missing market
    missing_market_tickers = (
        tickers[tickers["market"].isnull()].index.get_level_values("ticker").tolist()
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


def fetch_all_tickers(request_date=None, only_one_row_per_ticker=False):
    request_date = (
        request_date.isoformat()
        if isinstance(request_date, datetime.date)
        else request_date
    )

    all_tickers = pd.concat(
        [
            polygon_get_tickers(date=request_date, active=True),
            polygon_get_tickers(date=request_date, active=False),
        ]
    )

    # Reset the index of the DataFrame
    all_tickers.reset_index(inplace=True)

    # Sort so that rows where 'active' is False come first.
    all_tickers.sort_values("active", ascending=False, inplace=True)

    # Drop duplicates based on "ticker", "cik", and "request_date", keeping the first occurrence (active=True)
    all_tickers.drop_duplicates(
        subset=["ticker", "cik", "request_date"], keep="first", inplace=True
    )
    all_tickers.set_index(["ticker", "cik", "request_date", "active"], inplace=True)

    # Drop rows with active=False
    active_tickers = all_tickers.drop(index=False, level=3)

    # There should be no rows with delisted_utc values.  Check that is true then drop the column.
    assert active_tickers["delisted_utc"].isnull().all()
    active_tickers.drop(columns=["delisted_utc"], inplace=True)

    if only_one_row_per_ticker:
        # For rows with more than one CIK, call the Polygon Ticker Details API to get the correct CIK.
        # This is a bug in the Polygon Tickers API.
        ticker_counts = active_tickers.index.get_level_values("ticker").value_counts()
        tickers_with_more_than_one_row = ticker_counts[ticker_counts > 1].index.tolist()
        if tickers_with_more_than_one_row:
            logging.warning(
                f"{len(tickers_with_more_than_one_row)} tickers have more than one CIK: {sorted(tickers_with_more_than_one_row)}"
            )
            for ticker in tickers_with_more_than_one_row:
                response = get_polygon_client().get_ticker_details(
                    ticker=ticker, date=request_date
                )
                # Create a boolean mask for rows with this ticker but a different CIK
                mask = (active_tickers.index.get_level_values("ticker") == ticker) & (
                    active_tickers.index.get_level_values("cik") != response.cik
                )
                # Drop these rows
                active_tickers = active_tickers[~mask]

    # We're keeping these tickers with missing type because it is some Polygon bug.
    # # Drop rows with no type.  Not sure why this happens but we'll ignore them for now.
    # active_tickers = active_tickers.dropna(subset=["type"])

    active_tickers = active_tickers.astype(
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

    active_tickers.sort_index(inplace=True)

    validate_active_tickers(
        active_tickers, only_one_row_per_ticker=only_one_row_per_ticker
    )

    return active_tickers


def ticker_file_path(date: datetime.date):
    ticker_year_dir = os.path.join(get_tickers_dir(), f"tickers_{date.year}")
    os.makedirs(ticker_year_dir, exist_ok=True)
    return os.path.join(ticker_year_dir, f"tickers_{date.isoformat()}.parquet")


def ticker_file_exists(date: datetime.date):
    return os.path.exists(ticker_file_path(date))


def save_tickers_for_date(tickers: pd.DataFrame, date: datetime.date):
    tickers.to_parquet(ticker_file_path(date))


def load_tickers_for_date(date: datetime.date, fetch_missing=False):
    if not ticker_file_exists(date):
        if not fetch_missing:
            return None
        all_tickers = fetch_all_tickers(request_date=date)
        save_tickers_for_date(all_tickers, date)
        return all_tickers
    try:
        return pd.read_parquet(ticker_file_path(date))
    except (FileNotFoundError, IOError) as e:
        logging.error(f"Error loading tickers for {date}: {e}")
        try:
            # Remove the file so that it can be fetched again
            os.remove(ticker_file_path(date))
        except (FileNotFoundError, IOError) as e2:
            logging.error(f"Error removing file {ticker_file_path(date)}: {e2}")
        if fetch_missing:
            logging.info(f"Fetching tickers for {date} again.")
            all_tickers = fetch_all_tickers(request_date=date)
            save_tickers_for_date(all_tickers, date)
            return all_tickers
        return None


def load_all_tickers(
    start_date: datetime.date, end_date: datetime.date, market_calendar=None, fetch_missing=False
):
    if market_calendar is None:
        market_calendar = get_calendar("NYSE")
    all_tickers = pd.concat(
        [
            load_tickers_for_date(date.date(), fetch_missing=fetch_missing)
            for date in market_calendar.trading_index(
                start=start_date, end=end_date, period="1D"
            )
        ]
    )
    return all_tickers


def merge_tickers(all_tickers: pd.DataFrame):
    all_tickers.reset_index(inplace=True)

    # Make sure there are no leading or trailing spaces in the column values
    all_tickers = all_tickers.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Drops rows with missing primary exchange (rare but means it isn't actually active).
    all_tickers.dropna(subset=["name", "primary_exchange"], inplace=True)

    merged_tickers = (
        all_tickers.sort_values("last_updated_utc")
        .groupby(["ticker", "cik", "name"])
        .agg(
            {
                "request_date": ["min", "max"],
                "composite_figi": "last",
                "currency_name": "last",
                "locale": "last",
                "market": "last",
                "primary_exchange": "last",
                "share_class_figi": "last",
                "type": "last",
            }
        )
        .sort_values(by=("request_date", "max"), ascending=False)
    )

    # Flatten the multi-level column index
    merged_tickers.columns = [
        ("_".join(col[:-1] if col[-1] == "last" else col).strip())
        for col in merged_tickers.columns.values
    ]

    # Rename the columns
    merged_tickers.rename(
        columns={"request_date_min": "start_date", "request_date_max": "end_date"},
        inplace=True,
    )

    # Reset the index
    merged_tickers.reset_index(inplace=True)
    merged_tickers.set_index(["ticker", "start_date", "cik", "name"], inplace=True)
    merged_tickers.sort_index(inplace=True)

    return merged_tickers


def ticker_names_from_merged_tickers(merged_tickers: pd.DataFrame):
    ticker_names = merged_tickers.sort_index().reset_index()[
        ["ticker", "name", "start_date"]
    ]
    ticker_names.drop_duplicates(subset=["ticker", "name"], keep="last", inplace=True)
    return ticker_names


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
