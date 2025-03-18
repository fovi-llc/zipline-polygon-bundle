from zipline_polygon_bundle import get_ticker_universe
from zipline_polygon_bundle.config import PolygonConfig

import os
import argparse

# Initialize ticker files in __main__.  Use CLI args to specify start and end dates.
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Initialize ticker files.")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in ISO format (YYYY-MM-DD)",
        default="2014-05-01",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in ISO format (YYYY-MM-DD)",
        default="2024-04-01",
    )
    parser.add_argument("--data_dir", default=None)
    parser.add_argument("--to_data_dir", default=None)
    
    args = parser.parse_args()

    if args.data_dir:
        os.environ["POLYGON_DATA_DIR"] = args.data_dir

    if args.tickers_dir:
        os.environ["POLYGON_TICKERS_DIR"] = args.tickers_dir

    if args.to_data_dir:
        os.environ["CUSTOM_ASSET_FILES_DIR"] = args.to_data_dir

    config = PolygonConfig(
        environ=os.environ,
        calendar_name=args.calendar_name,
        start_date=args.start_date,
        end_date=args.end_date,
        agg_time=args.agg_duration,
    )

    tickers = get_ticker_universe(config)
    print(tickers)
