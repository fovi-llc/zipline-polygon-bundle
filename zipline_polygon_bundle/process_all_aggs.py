from .config import PolygonConfig

import os
import glob
import pandas as pd
import fastparquet as fp
from pathlib import Path


def apply_to_aggs(
    aggs_path: Path,
    func: callable,
    config: PolygonConfig,
    start_timestamp: pd.Timestamp = None,
    end_timestamp: pd.Timestamp = None,
):
    aggs_pf = fp.ParquetFile(aggs_path)
    df = aggs_pf.to_pandas()
    # Drop rows with window_start not in the range config.start_session to config.end_session
    # Those are dates (i.e. time 00:00:00) and end_session is inclusive.
    if start_timestamp is None and end_timestamp is None:
        return func(df, aggs_path, config)
    if start_timestamp is None:
        start_timestamp = pd.Timestamp.min
    if end_timestamp is None:
        end_timestamp = pd.Timestamp.max
    else:
        end_timestamp = end_timestamp + pd.Timedelta(days=1)
    df = df[
        df["window_start"].between(start_timestamp, end_timestamp, inclusive="left")
    ]
    return func(df, aggs_path, config)


# TODO: Return iterable of results
def apply_to_all_aggs(
    aggs_dir: str,
    func: callable,
    config: PolygonConfig,
    start_timestamp: pd.Timestamp = None,
    end_timestamp: pd.Timestamp = None,
    aggs_pattern: str = "**/*.parquet",
    max_workers=None,
) -> list:
    """zipline does bundle ingestion one ticker at a time."""
    paths = list(
        glob.glob(os.path.join(aggs_dir, aggs_pattern), recursive="**" in aggs_pattern)
    )
    print(f"{len(paths)=}")
    print(f"{paths[:5]=}")
    if max_workers == 0:
        return [
            apply_to_aggs(
                path, func, config=config, start_timestamp=start_timestamp, end_timestamp=end_timestamp
            )
            for path in paths
        ]
    else:
        # with ProcessPoolExecutor(max_workers=max_workers) as executor:
        #     executor.map(
        #         aggs_max_ticker_len,
        #         paths,
        #     )
        print("Not implemented")
        return None


if __name__ == "__main__":
    # os.environ["POLYGON_DATA_DIR"] = "/Volumes/Oahu/Mirror/files.polygon.io"
    def max_ticker_len(df: pd.DataFrame, path: Path, config: PolygonConfig):
        print(f"{path=}")
        return 0 if df.empty else df["ticker"].str.len().max()

    config = PolygonConfig(
        environ=os.environ,
        calendar_name="XNYS",
        start_session="2020-10-07",
        end_session="2020-10-15",
    )
    print(f"{config.aggs_dir=}")
    max_ticker_lens = apply_to_all_aggs(
        config.aggs_dir,
        func=max_ticker_len,
        config=config,
        aggs_pattern="2020/10/**/*.parquet",
        start_timestamp=config.start_timestamp,
        end_timestamp=config.end_timestamp,
        max_workers=0,
    )
    print(f"{max_ticker_lens=}")
    print(f"{len(max_ticker_lens)=}")
    print(f"{max(max_ticker_lens)=}")

# 2016 to 2024
# len(max_ticker_lens)=2184
# max(max_ticker_lens)=10
# 2020-10-07 to 2020-10-15
# max_ticker_lens=[0, 0, 0, 0, 8, 8, 8, 8, 8, 8, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
# len(max_ticker_lens)=22
# max(max_ticker_lens)=8
