from exchange_calendars.calendar_helpers import Date, parse_date
from zipline.utils.calendar_utils import get_calendar

import os
import pandas as pd


class PolygonConfig:
    def __init__(self, environ, calendar_name, start_session: Date, end_session: Date):
        self.calendar_name = calendar_name
        self.start_timestamp = parse_date(
            start_session, calendar=self.calendar, raise_oob=False
        ) if start_session else self.calendar.first_session
        self.end_timestamp = parse_date(
            end_session, calendar=self.calendar, raise_oob=False
        ) if end_session else self.calendar.last_session
        self.max_workers = None
        if environ.get("POLYGON_MAX_WORKERS", "").strip() != "":
            self.max_workers = int(environ.get("POLYGON_MAX_WORKERS"))
        self.api_key = environ.get("POLYGON_API_KEY")
        self.data_dir = environ.get("POLYGON_DATA_DIR", "data/polygon")
        self.asset_subdir = environ.get("POLYGON_ASSET_SUBDIR", "us_stocks_sip")
        self.market = environ.get("POLYGON_MARKET", "stocks")
        self.tickers_dir = environ.get(
            "POLYGON_TICKERS_DIR",
            os.path.join(os.path.join(self.data_dir, "tickers"), self.asset_subdir),
        )
        self.tickers_csv_path = environ.get(
            "POLYGON_TICKERS_CSV",
            os.path.join(
                self.tickers_dir,
                f"tickers_{self.start_timestamp.date().isoformat()}_{self.end_timestamp.date().isoformat()}.csv",
            ),
        )
        self.flat_files_dir = environ.get(
            "POLYGON_FLAT_FILES_DIR", os.path.join(self.data_dir, "flatfiles")
        )
        self.asset_files_dir = os.path.join(self.flat_files_dir, self.asset_subdir)
        self.minute_aggs_dir = os.path.join(self.asset_files_dir, "minute_aggs_v1")

    @property
    def calendar(self):
        return get_calendar(self.calendar_name)

    def ticker_file_path(self, date: pd.Timestamp):
        ticker_year_dir = os.path.join(self.tickers_dir, f"tickers_{date.strftime('%Y')}")
        os.makedirs(ticker_year_dir, exist_ok=True)
        return os.path.join(ticker_year_dir, f"tickers_{date.date().isoformat()}.parquet")
