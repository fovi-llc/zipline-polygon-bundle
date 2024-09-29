from .config import PolygonConfig

import os
import glob
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import fastparquet as fp
from pathlib import Path


def split_aggs_by_ticker(aggs_path: Path, by_ticker_dir: Path, extension: str):
    aggs_pf = fp.ParquetFile(aggs_path)
    df = aggs_pf.to_pandas()
    print(df.info())
    print(df.head())
    df.sort_values(["ticker", "window_start"], inplace=True)
    for ticker in df["ticker"].unique():
        ticker_df = df[df["ticker"] == ticker]
        ticker_path = by_ticker_dir / f"{ticker}{extension}"
        if os.path.exists(ticker_path):
            fp.write(ticker_path, ticker_df, has_nulls=False, write_index=False, fixed_text={"ticker": len(ticker) + 1})
        else:            
            fp.write(ticker_path, ticker_df, append=True, has_nulls=False, write_index=False)


def split_all_aggs_by_ticker(
    aggs_dir,
    by_ticker_dir,
    recursive=True,
    extension=".parquet",
    max_workers=None,
):
    """zipline does bundle ingestion one ticker at a time."""
    paths = list(
        glob.glob(os.path.join(by_ticker_dir, f"*{extension}"))
    )
    for path in paths:
        if os.path.exists(path):
            print(f"Removing {path}")
            os.remove(path)
    aggs_pattern = f"**/*{extension}" if recursive else f"*{extension}"
    paths = list(
        glob.glob(os.path.join(aggs_dir, aggs_pattern), recursive=recursive)
    )
    if max_workers == 0:
        for path in paths:
            split_aggs_by_ticker(
                path, by_ticker_dir=by_ticker_dir, extension=extension
            )
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            executor.map(
                split_aggs_by_ticker,
                paths,
                [by_ticker_dir] * len(paths),
                [extension] * len(paths),
            )


if __name__ == "__main__":
    os.environ["POLYGON_DATA_DIR"] = "/Volumes/Oahu/Mirror/files.polygon.io"
    config = PolygonConfig(environ=os.environ, calendar_name="XNYS", start_session=None, end_session=None)
    split_all_aggs_by_ticker(config.minute_aggs_dir)
