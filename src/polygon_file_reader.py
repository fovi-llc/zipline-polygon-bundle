import os
import glob
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from config import get_minute_aggs_dir


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


def convert_minute_csv_to_parquet(path, extension, compression="infer"):
    print(path)
    try:
        bars_df = pd.read_csv(
            path,
            compression=compression,
            converters={"ticker": lambda x: str(x), "window_start": convert_timestamp},
        )
        # bars_df.info()
        if len(bars_df) == 0:
            print(f"WARNING: Empty {path}")
            return
        if len(bars_df) < 100000:
            print(f"WARNING: Short {path}")
        # Don't change the data.  We're just converting to Parquet to save time.
        # bars_df.set_index(["window_start", "ticker"], inplace=True)
        # bars_df.sort_index(inplace=True)
        parquet_path = path.removesuffix(extension) + ".parquet"
        bars_df.to_parquet(parquet_path)
        if not os.path.exists(parquet_path):
            print(f"ERROR: Failed to write {parquet_path}")
    except Exception as e:
        print(f"Failed for {path}: {e}")


def process_all_minute_csv_to_parquet(
    minute_aggs_dir,
    recursive=True,
    extension=".csv.gz",
    compression="infer",
    force=False,
    max_workers=None,
):
    """Big CSV files are very slow to read.  So we only read them once and convert them to Parquet."""
    csv_pattern = f"**/*{extension}" if recursive else f"*{extension}"
    paths = list(
        glob.glob(os.path.join(minute_aggs_dir, csv_pattern), recursive=recursive)
    )
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
    if max_workers == 1:
        for path in paths:
            convert_minute_csv_to_parquet(
                path, extension=extension, compression=compression
            )
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            executor.map(
                convert_minute_csv_to_parquet,
                paths,
                [extension] * len(paths),
                [compression] * len(paths),
            )


if __name__ == "__main__":
    process_all_minute_csv_to_parquet(get_minute_aggs_dir())
