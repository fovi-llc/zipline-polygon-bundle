from zipline_polygon_bundle import convert_all_to_custom_aggs
from zipline_polygon_bundle.config import PolygonConfig

import os
import argparse


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

    if args.to_data_dir:
        os.environ["CUSTOM_ASSET_FILES_DIR"] = args.to_data_dir

    config = PolygonConfig(
        environ=os.environ,
        calendar_name=args.calendar_name,
        start_date=args.start_date,
        end_date=args.end_date,
        agg_time=args.agg_duration,
    )

    convert_all_to_custom_aggs(config, overwrite=args.overwrite)

    # print(f"{config.aggs_dir=}")
    # print(f"{config.custom_aggs_dir=}")
    # print(f"{os.environ['CUSTOM_DATA_DIR']=}")
