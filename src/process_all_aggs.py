import os
import glob
from config import PolygonConfig
import pandas as pd
import fastparquet as fp
from pathlib import Path


def aggs_max_ticker_len(
    aggs_path: Path,
    start_timestamp: pd.Timestamp = None,
    end_timestamp: pd.Timestamp = None,
):
    aggs_pf = fp.ParquetFile(aggs_path)
    df = aggs_pf.to_pandas()
    if df.empty:
        return 0
    # Drop rows with window_start not in the range config.start_session to config.end_session
    # Those are dates (i.e. time 0:00:00) and end_session is inclusive.
    if start_timestamp is None and end_timestamp is None:
        print("unreachable?")
        return df["ticker"].str.len().max()
    if start_timestamp is None:
        start_timestamp = pd.Timestamp.min
    if end_timestamp is None:
        end_timestamp = pd.Timestamp.max
    else:
        end_timestamp = end_timestamp + pd.Timedelta(days=1)
    df = df[df["window_start"].between(start_timestamp, end_timestamp, inclusive="left")]
    if df.empty:
        return 0
    print(f"{aggs_path=}")
    return df["ticker"].str.len().max()


def all_aggs_max_ticker_len(
    aggs_dir: str,
    start_timestamp: pd.Timestamp = None,
    end_timestamp: pd.Timestamp = None,
    aggs_pattern: str = "**/*.parquet",
    max_workers=None,
):
    """zipline does bundle ingestion one ticker at a time."""
    paths = list(
        glob.glob(os.path.join(aggs_dir, aggs_pattern), recursive="**" in aggs_pattern)
    )
    if max_workers == 0:
        max_ticker_lens = [
            aggs_max_ticker_len(
                path, start_timestamp=start_timestamp, end_timestamp=end_timestamp
            )
            for path in paths
        ]
        print(f"{len(max_ticker_lens)=}")
        print(f"{max(max_ticker_lens)=}")
    else:
        # with ProcessPoolExecutor(max_workers=max_workers) as executor:
        #     executor.map(
        #         aggs_max_ticker_len,
        #         paths,
        #     )
        print("Not implemented")


if __name__ == "__main__":
    # os.environ["POLYGON_DATA_DIR"] = "/Volumes/Oahu/Mirror/files.polygon.io"
    config = PolygonConfig(
        environ=os.environ,
        calendar_name="XNYS",
        start_session="2020-10-05",
        end_session="2020-10-15",
    )
    all_aggs_max_ticker_len(
        config.minute_aggs_dir,
        aggs_pattern="2020/10/**/*.parquet",
        # start_timestamp=config.start_timestamp,
        # end_timestamp=config.end_timestamp,
        max_workers=0,
    )

# 2016 to 2024
# len(max_ticker_lens)=2184
# max(max_ticker_lens)=10
