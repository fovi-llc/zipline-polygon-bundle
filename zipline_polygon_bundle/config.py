from exchange_calendars.calendar_helpers import Date, parse_date
from zipline.utils.calendar_utils import get_calendar

from .nyse_all_hours_calendar import NYSE_ALL_HOURS

from typing import Iterator, Mapping, Tuple

import pandas as pd
from pyarrow.fs import LocalFileSystem
import os
import datetime
import re
import fnmatch

AGG_TIME_DAY = "day"
AGG_TIME_MINUTE = "minute"
AGG_TIME_TRADES = "1minute"

PARTITION_COLUMN_NAME = "part"
PARTITION_KEY_LENGTH = 2


def to_partition_key(s: str) -> str:
    """
    Partition key is low cardinality and must be filesystem-safe.
    The reason for partitioning is to keep the parquet files from getting too big.
    10 years of minute aggs for US stocks is 83GB gzipped.  A single parquet would be 62GB on disk.
    Currently the first two characters so files stay under 1GB.  Weird characters are replaced with "A".
    """
    k = (s + "A")[0:PARTITION_KEY_LENGTH].upper()
    if k.isalpha():
        return k
    # Replace non-alpha characters with "A".
    k = "".join([c if c.isalpha() else "A" for c in k])
    return k


class PolygonConfig:
    def __init__(
        self,
        environ: Mapping[str, str],
        calendar_name: str,
        start_date: Date,
        end_date: Date,
        agg_time: str = AGG_TIME_DAY,
    ):
        self.calendar_name = calendar_name
        self.start_date = start_date
        self.end_date = end_date
        # TODO: We can't use PolygonConfig.calendar because it gets these properties for start/end session.
        self.start_timestamp = (
            parse_date(start_date, calendar=self.calendar)
            if start_date
            else self.calendar.first_session
        )
        self.end_timestamp = (
            parse_date(end_date, calendar=self.calendar)
            if end_date
            else self.calendar.last_session
        )
        self.agg_time = agg_time

        self.max_workers = None
        if environ.get("POLYGON_MAX_WORKERS", "").strip() != "":
            self.max_workers = int(environ.get("POLYGON_MAX_WORKERS"))
        self.api_key = environ.get("POLYGON_API_KEY")
        self.filesystem = LocalFileSystem()
        self.data_dir = environ.get("POLYGON_DATA_DIR", "data/files.polygon.io")
        self.cik_cusip_mapping_csv_path = environ.get(
            "CIK_CUSIP_MAPS_CSV", os.path.join(self.data_dir, "cik-cusip-maps.csv")
        )
        self.market = environ.get("POLYGON_MARKET", "stocks")
        self.asset_subdir = environ.get("POLYGON_ASSET_SUBDIR", "us_stocks_sip")
        self.flat_files_dir = environ.get(
            "POLYGON_FLAT_FILES_DIR", os.path.join(self.data_dir, "flatfiles")
        )
        # TODO: Restore non-recusive option.  Always recursive for now.
        self.csv_paths_pattern = environ.get(
            # "POLYGON_FLAT_FILES_CSV_PATTERN", "**/*.csv.gz"
            "POLYGON_FLAT_FILES_CSV_PATTERN",
            "*.csv.gz",
        )
        self.asset_files_dir = os.path.join(self.flat_files_dir, self.asset_subdir)
        self.minute_aggs_dir = os.path.join(self.asset_files_dir, "minute_aggs_v1")
        self.day_aggs_dir = os.path.join(self.asset_files_dir, "day_aggs_v1")
        self.trades_dir = os.path.join(self.asset_files_dir, "trades_v1")
        self.quotes_dir = os.path.join(self.asset_files_dir, "quotes_v1")

        # TODO: The "by ticker" files are temporary/intermediate and should/could be in the zipline data dir.
        self.custom_asset_files_dir = environ.get(
            "CUSTOM_ASSET_FILES_DIR", self.asset_files_dir
        )
        self.tickers_dir = environ.get(
            "POLYGON_TICKERS_DIR",
            os.path.join(self.custom_asset_files_dir, "tickers"),
        )
        self.tickers_csv_path = environ.get(
            "POLYGON_TICKERS_CSV",
            os.path.join(
                self.tickers_dir,
                f"tickers_{self.start_timestamp.date().isoformat()}_{self.end_timestamp.date().isoformat()}.csv",
            ),
        )

        self.cache_dir = os.path.join(self.custom_asset_files_dir, "api_cache")

        self.lock_file_path = os.path.join(self.custom_asset_files_dir, "ingest.lock")
        self.custom_aggs_dates_path = os.path.join(self.custom_asset_files_dir, "aggs_dates.json")
        self.by_ticker_dates_path = os.path.join(self.custom_asset_files_dir, "by_ticker_dates.json")

        self.minute_by_ticker_dir = os.path.join(
            self.custom_asset_files_dir, "minute_by_ticker_v1"
        )
        self.day_by_ticker_dir = os.path.join(
            self.custom_asset_files_dir, "day_by_ticker_v1"
        )

        # If agg_time begins with a digit, it is a timedelta string and we're using custom aggs from trades.
        if bool(re.match(r"^\d", agg_time)):
            self.agg_timedelta = pd.to_timedelta(agg_time)
            self.csv_files_dir = self.trades_dir
            self.custom_aggs_name_format = environ.get(
                "CUSTOM_AGGS_NAME_FORMAT", "{config.agg_timedelta.seconds}sec_aggs"
            )
            self.aggs_dir = os.path.join(
                self.custom_asset_files_dir,
                self.custom_aggs_name_format.format(config=self),
            )
            self.by_ticker_dir = os.path.join(
                self.custom_asset_files_dir,
                (self.custom_aggs_name_format + "_by_ticker").format(config=self),
            )
        elif agg_time == AGG_TIME_MINUTE:
            self.agg_timedelta = pd.to_timedelta("1minute")
            self.aggs_dir = self.minute_aggs_dir
            self.csv_files_dir = self.aggs_dir
            self.by_ticker_dir = self.minute_by_ticker_dir
        elif agg_time == AGG_TIME_DAY:
            self.agg_timedelta = pd.to_timedelta("1day")
            self.aggs_dir = self.day_aggs_dir
            self.csv_files_dir = self.aggs_dir
            self.by_ticker_dir = self.day_by_ticker_dir
        else:
            raise ValueError(
                f"agg_time must be 'minute', 'day', or a timedelta string; got '{agg_time=}'"
            )

        self.arrow_format = environ.get(
            "POLYGON_ARROW_FORMAT", "parquet" if self.agg_time == AGG_TIME_DAY else "hive"
        )
        # self.by_ticker_hive_dir = os.path.join(
        #     self.by_ticker_dir,
        #     f"{self.agg_time}_{self.start_timestamp.date().isoformat()}_{self.end_timestamp.date().isoformat()}.hive",
        # )

    @property
    def calendar(self):
        # print call stack
        # import traceback
        # traceback.print_stack()
        return get_calendar(self.calendar_name, start_session=self.start_date, end_session=self.end_date)

    def ticker_file_path(self, date: pd.Timestamp):
        ticker_year_dir = os.path.join(
            self.tickers_dir, f"tickers_{date.strftime('%Y')}"
        )
        os.makedirs(ticker_year_dir, exist_ok=True)
        return os.path.join(
            ticker_year_dir, f"tickers_{date.date().isoformat()}.parquet"
        )

    def file_path_to_name(self, path: str):
        # TODO: Use csv_paths_pattern to remove the suffixes
        return os.path.basename(path).removesuffix(".gz").removesuffix(".csv")

    def date_to_csv_file_path(self, date: datetime.date, ext=".csv.gz"):
        return f"{self.csv_files_dir}/{date.strftime('%Y/%m/%Y-%m-%d') + ext}"

    @property
    def by_ticker_aggs_arrow_dir(self):
        # TODO: Don't split these up by ingestion range.  They're already time indexed.
        # Only reason to separate them is if we're worried about (or want) data being different across ingestions.
        # This scattering is really slow and is usually gonna be redundant.
        # This wasn't a problem when start/end dates were the calendar bounds when omitted.
        # Can't just drop this because concat_all_aggs_from_csv will skip if it exists.
        # return os.path.join(
        #     self.by_ticker_dir,
        #     f"{self.start_timestamp.date().isoformat()}_{self.end_timestamp.date().isoformat()}.arrow",
        #     # "aggs.arrow",
        # )
        return self.by_ticker_dir

    def api_cache_path(
        self, first_day: pd.Timestamp, last_day: pd.Timestamp, filename: str, extension=".parquet"
    ):
        first_day_str = first_day.date().isoformat()
        last_day_str = last_day.date().isoformat()
        return os.path.join(
            self.cache_dir, f"{first_day_str}_{last_day_str}/{filename}{extension}"
        )

    def csv_paths(self) -> Iterator[str]:
        for root, dirnames, filenames in os.walk(self.aggs_dir, topdown=True):
            if dirnames:
                dirnames[:] = sorted(dirnames)
            # Filter out filenames that don't match the pattern.
            filenames = fnmatch.filter(filenames, self.csv_paths_pattern)
            if filenames:
                for filename in sorted(filenames):
                    yield os.path.join(root, filename)

    def find_first_and_last_aggs(
        self, aggs_dir, file_pattern
    ) -> Tuple[str | None, str | None]:
        # Find the path to the lexically first and last paths in aggs_dir that matches csv_paths_pattern.
        # Would like to use Path.walk(top_down=True) but it is only availble in Python 3.12+.
        # This needs to be efficient because it is called on every init, even though we only need it for ingest.
        # But we can't call it in ingest because the writer initializes and writes the metadata before it is called.
        paths = []
        for root, dirnames, filenames in os.walk(aggs_dir, topdown=True):
            if dirnames:
                # We only want first and last in each directory.
                sorted_dirs = sorted(dirnames)
                dirnames[:] = (
                    [sorted_dirs[0], sorted_dirs[-1]]
                    if len(sorted_dirs) > 1
                    else sorted_dirs
                )
            # Filter out filenames that don't match the pattern.
            filenames = fnmatch.filter(filenames, file_pattern)
            if filenames:
                filenames = sorted(filenames)
                paths.append(os.path.join(root, filenames[0]))
                if len(filenames) > 1:
                    paths.append(os.path.join(root, filenames[-1]))
        if not paths:
            return None, None
        paths = sorted(paths)
        return self.file_path_to_name(paths[0]), self.file_path_to_name(paths[-1])


if __name__ == "__main__":
    config = PolygonConfig(os.environ, "XNYS", "2003-10-01", "2023-01-01")
    print(config.__dict__)
