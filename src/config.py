from exchange_calendars.calendar_helpers import Date, parse_date
from zipline.utils.calendar_utils import get_calendar

import os
import pandas as pd


class PolygonConfig:
    def __init__(self, environ, calendar_name, start_session: Date, end_session: Date):
        self.environ = environ
        self.calendar = get_calendar(calendar_name)
        self.start_timestamp = parse_date(
            start_session, calendar=self.calendar, raise_oob=False
        )
        self.end_timestamp = parse_date(
            end_session, calendar=self.calendar, raise_oob=False
        )

    @property
    def api_key(self):
        return self.environ.get("POLYGON_API_KEY")

    @property
    def data_dir(self):
        return self.environ.get("POLYGON_DATA_DIR", "data/polygon")

    @property
    def asset_subdir(self):
        return self.environ.get("POLYGON_ASSET_SUBDIR", "us_stocks_sip")

    @property
    def tickers_dir(self):
        return self.environ.get(
            "POLYGON_TICKERS_DIR",
            os.path.join(os.path.join(self.data_dir, "tickers"), self.asset_subdir),
        )

    def ticker_file_path(self, date: pd.Timestamp):
        ticker_year_dir = os.path.join(self.tickers_dir, f"tickers_{date.strftime('%Y')}")
        os.makedirs(ticker_year_dir, exist_ok=True)
        return os.path.join(ticker_year_dir, f"tickers_{date.isoformat()}.parquet")

    @property
    def tickers_csv_path(self):
        return self.environ.get(
            "POLYGON_TICKERS_CSV",
            os.path.join(
                self.tickers_dir,
                f"tickers_{self.start_timestamp.date().isoformat()}_{self.end_timestamp.date().isoformat()}.csv",
            ),
        )

    @property
    def flat_files_dir(self):
        return self.environ.get(
            "POLYGON_FLAT_FILES_DIR", os.path.join(self.data_dir, "flatfiles")
        )

    @property
    def asset_files_dir(self):
        return os.path.join(self.flat_files_dir, self.asset_subdir)

    @property
    def minute_aggs_dir(self):
        return os.path.join(self.asset_files_dir, "minute_aggs_v1")
