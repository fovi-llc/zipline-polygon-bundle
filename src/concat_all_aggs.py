import argparse
import os
from config import PolygonConfig
import fastparquet as fp
import pandas as pd
from pathlib import Path


from process_all_aggs import apply_to_all_aggs


def create_concatenated_aggs(config: PolygonConfig, overwrite: bool = False):
    """Create a single parquet file with all aggregates sorted by ticker then window_start."""
    by_ticker_dir = Path(config.by_ticker_dir)
    by_ticker_dir.mkdir(parents=True, exist_ok=True)
    by_ticker_path = (
        by_ticker_dir
        / f"{config.agg_time}_{config.start_timestamp.date().isoformat()}_{config.end_timestamp.date().isoformat()}.hive"
    )
    print(f"{by_ticker_path=}")
    if by_ticker_path.exists():
        assert overwrite, f"{by_ticker_path} exists and overwrite is False"
        if not overwrite:
            return None
    aggs_schema = {
        "ticker": "string",
        "volume": "int",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "window_start": "datetime64[ns]",
        "transactions": "int",
    }
    df = pd.DataFrame(
        data=[],
        columns=[
            "ticker",
            "volume",
            "open",
            "close",
            "high",
            "low",
            "window_start",
            "transactions",
        ],
    ).astype(aggs_schema)
    # df.set_index(["ticker"], inplace=True)
    # df.info()
    fp.write(
        by_ticker_path,
        df,
        file_scheme="hive",
        has_nulls=False,
        # write_index=True,
        # partition_on=["ticker"],
    )
    return by_ticker_path


def sort_by_window_start(rgs):
    print(f"{type(rgs)=}")
    print(f"{rgs=}")
    return rgs


def concat_aggs(df: pd.DataFrame, aggs_path: Path, config: PolygonConfig):
    if len(df) == 0:
        print(f"Empty {aggs_path}")
        return 0
    print(f"Processing {aggs_path}")
    df.info()
    by_ticker_path = os.path.join(
        config.by_ticker_dir,
        f"{config.agg_time}_{config.start_timestamp.date().isoformat()}_{config.end_timestamp.date().isoformat()}.hive",
    )
    aggs_fp = fp.ParquetFile(aggs_path)
    aggs_df = aggs_fp.to_pandas()
    aggs_df.info()
    # aggs_df.set_index(["ticker"], inplace=True)
    # aggs_df.info()
    # Append to the by_ticker file
    # fp.write(by_ticker_path, aggs_df, append=True, has_nulls=False, write_index=False)
    tickers_fp = fp.ParquetFile(by_ticker_path, verify=True, pandas_nulls=False)
    # tickers_fp.write_row_groups(aggs_df, sort_key=sort_by_window_start)
    tickers_fp.write_row_groups(aggs_df)
    return len(aggs_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_session", default="2020-10-07")
    parser.add_argument("--end_session", default="2020-10-15")
    parser.add_argument("--aggs_pattern", default="2020/10/**/*.parquet")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    config = PolygonConfig(
        environ=os.environ,
        calendar_name="XNYS",
        start_session=args.start_session,
        end_session=args.end_session,
    )

    by_ticker_path = create_concatenated_aggs(config, overwrite=args.overwrite)
    print(f"Created {by_ticker_path=}")

    appended_rows = apply_to_all_aggs(
        config.minute_aggs_dir,
        func=concat_aggs,
        config=config,
        aggs_pattern=args.aggs_pattern,
        start_timestamp=config.start_timestamp,
        end_timestamp=config.end_timestamp,
        max_workers=0,
    )

    print(f"{appended_rows=}")
    print(f"{len(appended_rows)=}")
    print(f"{sum(appended_rows)=}")
