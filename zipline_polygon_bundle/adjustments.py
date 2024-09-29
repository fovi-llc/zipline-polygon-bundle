from .config import PolygonConfig

import polygon

import datetime
import logging
import os
import pandas as pd
from urllib3 import HTTPResponse


def load_polygon_splits(
    config: PolygonConfig, first_start_end: datetime.date, last_end_date: datetime.date
) -> pd.DataFrame:
    # N.B. If the schema changes then the filename should change.  We're on v3 now.
    splits_path = config.api_cache_path(
        start_date=first_start_end, end_date=last_end_date, filename="list_splits"
    )
    expected_split_count = (last_end_date - first_start_end).days * 3
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
        if len(splits) < expected_split_count:
            logging.warning(
                f"Only got {len(splits)=} from Polygon list_splits (expected {expected_split_count=}).  "
                "This is probably fine if your historical range is short."
            )
        # We will always load from the file to avoid any chance of weird errors.
    if os.path.exists(splits_path):
        splits = pd.read_parquet(splits_path)
        print(f"Loaded {len(splits)=} from {splits_path}")
        if len(splits) < expected_split_count:
            logging.warning(
                f"Only got {len(splits)=} from Polygon list_splits (expected {expected_split_count=}).  "
                "This is probably fine if your historical range is short."
            )
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
    return splits


def load_polygon_dividends(
    config: PolygonConfig, first_start_date: datetime.date, last_end_date: datetime.date
) -> pd.DataFrame:
    # N.B. If the schema changes then the filename should change.  We're on v3 now.
    dividends_path = config.api_cache_path(
        start_date=first_start_date, end_date=last_end_date, filename="list_dividends"
    )
    if not os.path.exists(dividends_path):
        client = polygon.RESTClient(api_key=config.api_key)
        dividends = client.list_dividends(
            limit=1000,
            record_date_gte=first_start_date,
            pay_date_lt=last_end_date + datetime.timedelta(days=1),
        )
        if dividends is HTTPResponse:
            raise ValueError(f"Polygon.list_dividends bad HTTPResponse: {dividends}")
        dividends = pd.DataFrame(dividends)
        os.makedirs(os.path.dirname(dividends_path), exist_ok=True)
        dividends.to_parquet(dividends_path)
        print(f"Wrote {len(dividends)=} from Polygon list_dividends to {dividends_path=}")
        # if len(dividends) < 10000:
        #     logging.error(f"Only got {len(dividends)=} from Polygon list_dividends.")
    # We will always load from the file to avoid any chance of weird errors.
    if os.path.exists(dividends_path):
        dividends = pd.read_parquet(dividends_path)
        # print(f"Loaded {len(dividends)=} from {dividends_path}")
        # if len(dividends) < 10000:
        #     logging.error(f"Only found {len(dividends)=} at {dividends_path}")
        return dividends
    raise ValueError(f"Failed to load dividends from {dividends_path}")


def load_chunked_polygon_dividends(
    config: PolygonConfig, first_start_end: datetime.date, last_end_date: datetime.date
) -> pd.DataFrame:
    dividends_list = []
    next_start_end = first_start_end
    while next_start_end < last_end_date:
        # We want at most a month of dividends at a time.  They should end on the last day of the month.
        # So the next_end_date is the day before the first day of the next month.
        first_of_next_month = datetime.date(
            next_start_end.year + (next_start_end.month // 12),
            (next_start_end.month % 12) + 1,
            1,
        )
        next_end_date = first_of_next_month - datetime.timedelta(days=1)
        if next_end_date > last_end_date:
            next_end_date = last_end_date
        dividends_list.append(load_polygon_dividends(
            config, next_start_end, next_end_date
        ))
        next_start_end = next_end_date + datetime.timedelta(days=1)
    return pd.concat(dividends_list)


def load_dividends(
    config: PolygonConfig,
    first_start_end: datetime.date,
    last_end_date: datetime.date,
    ticker_to_sid: dict[str, int],
) -> pd.DataFrame:
    dividends = load_chunked_polygon_dividends(config, first_start_end, last_end_date)
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
    return dividends
