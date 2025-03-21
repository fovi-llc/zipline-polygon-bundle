from zipline_polygon_bundle import compute_signals_for_all_custom_aggs
from zipline_polygon_bundle.config import PolygonConfig
from zipline_polygon_bundle import get_ticker_universe

import os
import argparse

import pyarrow as pa


def get_valid_tickers(config: PolygonConfig):
    tickers = get_ticker_universe(config)
    return pa.array(
        [
            ticker
            for ticker in tickers.index.get_level_values("ticker").to_list()
            if "TEST" not in ticker
        ]
    )


# export POLYGON_TICKERS_DIR=/home/jovyan/data/tickers
# export CUSTOM_ASSET_FILES_DIR=/home/jovyan/data

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--calendar_name", default="NYSE")
    parser.add_argument("--start_date", default="2014-06-16")
    parser.add_argument("--end_date", default="2024-09-06")
    # parser.add_argument("--start_date", default="2020-01-01")
    # parser.add_argument("--end_date", default="2020-12-31")

    parser.add_argument("--agg_duration", default="1min")

    parser.add_argument("--overwrite", action="store_true")

    # parser.add_argument("--data_dir", default="/Volumes/Oahu/Mirror/files.polygon.io")
    parser.add_argument("--data_dir", default=None)
    parser.add_argument("--to_data_dir", default=None)

    args = parser.parse_args()

    if args.data_dir:
        os.environ["POLYGON_DATA_DIR"] = args.data_dir

    from_config = PolygonConfig(
        environ=os.environ,
        calendar_name=args.calendar_name,
        start_date=args.start_date,
        end_date=args.end_date,
        agg_time=args.agg_duration,
    )

    if args.to_data_dir:
        os.environ["CUSTOM_ASSET_FILES_DIR"] = args.to_data_dir

    to_config = PolygonConfig(
        environ=os.environ,
        calendar_name=args.calendar_name,
        start_date=args.start_date,
        end_date=args.end_date,
        agg_time=args.agg_duration,
        custom_aggs_format="{config.agg_timedelta.seconds}sec_aggs_signals",
    )

    # print(f"{from_config.api_key=}")
    valid_tickers = get_valid_tickers(from_config)
    signals_ds_path = compute_signals_for_all_custom_aggs(
        from_config=from_config,
        to_config=to_config,
        valid_tickers=valid_tickers,
        overwrite=args.overwrite,
    )
    print(f"Computed signals for aggregates to {signals_ds_path=}")

    # print(f"{config.aggs_dir=}")
    # print(f"{config.custom_aggs_dir=}")
    # print(f"{os.environ['CUSTOM_DATA_DIR']=}")
