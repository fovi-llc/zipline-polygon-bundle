from .config import PolygonConfig

import os
import glob
from concurrent.futures import ProcessPoolExecutor
import pandas as pd


def convert_timestamp(x):
    """
    Polygon timestamps are in nanoseconds, milliseconds, or seconds.
    We can decide automatically based on the size of the number because the only overlaps
    are during the first few months of 1970.  And zero is always the same in any case.
    """
    try:
        unix_time = int(x)
        return pd.to_datetime(
            unix_time,
            unit=(
                "ns"
                if unix_time > 100_000_000_000_000
                else "ms" if unix_time > 10_000_000_000 else "s"
            ),
        )
    except Exception as e:
        print(f"ERROR: Failed to convert '{x}': {e}")
        return pd.NaT


def convert_csv_to_parquet(path, extension, compression):
    print(path)
    try:
        bars_df = pd.read_csv(
            path,
            compression=compression,
            converters={"window_start": convert_timestamp},
            dtype={"ticker": "str"},
        )
        # bars_df["ticker"] = bars_df["ticker"].astype(str)
        # bars_df.info()
        # print(f"{bars_df["ticker"].str.len().max()=}")
        if len(bars_df) == 0:
            print(f"WARNING: Empty {path}")
            return
        # if len(bars_df) < 100000:
        #     print(f"WARNING: Short {path}")
        # Don't change the data.  We're just converting to Parquet to save time.
        # bars_df.set_index(["window_start", "ticker"], inplace=True)
        # bars_df.sort_index(inplace=True)
        parquet_path = path.removesuffix(extension) + ".parquet"
        bars_df.to_parquet(parquet_path)
        # fp.write(parquet_path, bars_df, has_nulls=False, write_index=False, fixed_text={"ticker": bars_df["ticker"].str.len().max()})
        if not os.path.exists(parquet_path):
            print(f"ERROR: Failed to write {parquet_path}")
    except Exception as e:
        print(f"Failed for {path}: {e}")


def process_all_csv_to_parquet(
    aggs_dir,
    recursive=True,
    extension=".csv.gz",
    compression="infer",
    force=False,
    max_workers=None,
):
    """Big CSV files are very slow to read.  So we only read them once and convert them to Parquet."""
    csv_pattern = f"**/*{extension}" if recursive else f"*{extension}"
    paths = list(glob.glob(os.path.join(aggs_dir, csv_pattern), recursive=recursive))
    if force:
        print(f"Removing Parquet files that may exist for {len(paths)} CSV files.")
        for path in paths:
            parquet_path = path.removesuffix(extension) + ".parquet"
            if os.path.exists(parquet_path):
                print(f"Removing {parquet_path}")
                os.remove(parquet_path)
    else:
        csv_file_count = len(paths)
        paths = [
            path
            for path in paths
            if not os.path.exists(path.removesuffix(extension) + ".parquet")
        ]
        if len(paths) < csv_file_count:
            print(f"Skipping {csv_file_count - len(paths)} already converted files.")
    if max_workers == 0:
        for path in paths:
            convert_csv_to_parquet(path, extension=extension, compression=compression)
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            executor.map(
                convert_csv_to_parquet,
                paths,
                [extension] * len(paths),
                [compression] * len(paths),
            )


if __name__ == "__main__":
    # os.environ["POLYGON_DATA_DIR"] = "/Volumes/Oahu/Mirror/files.polygon.io"
    config = PolygonConfig(
        environ=os.environ, calendar_name="XNYS", start_session=None, end_session=None
    )
    process_all_csv_to_parquet(config.aggs_dir)
