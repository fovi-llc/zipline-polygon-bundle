from exchange_calendars.calendar_helpers import Date, parse_date, parse_timestamp
from zipline.utils.calendar_utils import get_calendar

import os
import pandas as pd


class PolygonConfig:
    def __init__(
        self,
        environ: dict,
        calendar_name: str,
        start_session: Date,
        end_session: Date,
        agg_time: str = "day",
    ):
        if agg_time not in ["minute", "day"]:
            raise ValueError(f"agg_time must be 'minute' or 'day', got '{agg_time}'")
        self.calendar_name = calendar_name
        self.start_timestamp = (
            parse_date(start_session, calendar=self.calendar)
            if start_session
            else self.calendar.first_session
        )
        self.end_timestamp = (
            parse_date(end_session, calendar=self.calendar)
            if end_session
            else self.calendar.last_session
        )
        self.max_workers = None
        if environ.get("POLYGON_MAX_WORKERS", "").strip() != "":
            self.max_workers = int(environ.get("POLYGON_MAX_WORKERS"))
        self.api_key = environ.get("POLYGON_API_KEY")
        self.data_dir = environ.get("POLYGON_DATA_DIR", "data/files.polygon.io")
        self.cik_cusip_mapping_csv_path = environ.get(
            "CIK_CUSIP_MAPS_CSV", os.path.join(self.data_dir, "cik-cusip-maps.csv")
        )
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
        self.csv_paths_pattern = environ.get("POLYGON_FLAT_FILES_CSV_PATTERN", "**/*.csv.gz")
        self.agg_time = agg_time
        self.asset_files_dir = os.path.join(self.flat_files_dir, self.asset_subdir)
        self.minute_aggs_dir = os.path.join(self.asset_files_dir, "minute_aggs_v1")
        self.day_aggs_dir = os.path.join(self.asset_files_dir, "day_aggs_v1")
        self.aggs_dir = (
            self.minute_aggs_dir if self.agg_time == "minute" else self.day_aggs_dir
        )
        # TODO: The "by ticker" files are temporary/intermediate and should/could be in the zipline data dir.
        self.minute_by_ticker_dir = os.path.join(
            self.asset_files_dir, "minute_by_ticker_v1"
        )
        self.day_by_ticker_dir = os.path.join(self.asset_files_dir, "day_by_ticker_v1")
        self.by_ticker_dir = (
            self.minute_by_ticker_dir
            if self.agg_time == "minute"
            else self.day_by_ticker_dir
        )
        self.arrow_format = environ.get("POLYGON_ARROW_FORMAT", "parquet" if self.agg_time == "day" else "hive")
        # self.by_ticker_hive_dir = os.path.join(
        #     self.by_ticker_dir,
        #     f"{self.agg_time}_{self.start_timestamp.date().isoformat()}_{self.end_timestamp.date().isoformat()}.hive",
        # )
        self.cache_dir = os.path.join(self.asset_files_dir, "api_cache")

    @property
    def calendar(self):
        return get_calendar(self.calendar_name)

    def ticker_file_path(self, date: pd.Timestamp):
        ticker_year_dir = os.path.join(
            self.tickers_dir, f"tickers_{date.strftime('%Y')}"
        )
        os.makedirs(ticker_year_dir, exist_ok=True)
        return os.path.join(
            ticker_year_dir, f"tickers_{date.date().isoformat()}.parquet"
        )
    
    def file_path_to_name(self, path: str):
        return os.path.basename(path).removesuffix(".gz").removesuffix(".csv")

    def by_ticker_aggs_arrow_dir(self, first_path: str, last_path: str):
        return os.path.join(
            self.by_ticker_dir,
            f"{self.file_path_to_name(first_path)}_{self.file_path_to_name(last_path)}.arrow",
        )

    def api_cache_path(
        self, start_date: Date, end_date: Date, filename: str, extension=".parquet"
    ):
        start_str = parse_date(start_date, calendar=self.calendar).date().isoformat()
        end_str = parse_date(end_date, calendar=self.calendar).date().isoformat()
        return os.path.join(
            self.cache_dir, f"{start_str}_{end_str}/{filename}{extension}"
        )


if __name__ == "__main__":
    config = PolygonConfig(os.environ, "XNYS", "2003-10-01", "2023-01-01")
    print(config.__dict__)
