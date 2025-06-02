#!/usr/bin/env python3
"""
Generate superbars from Polygon flatfile trades and quotes data.

This script processes trades and quotes data to create superbars with extended features
based on the Algoseek US.Equity.TAQ.Minute.Bars.Ext specification.
"""

import os
import sys
import argparse
import datetime
from typing import Optional
import pandas as pd
from exchange_calendars.calendar_helpers import parse_date, Date

# Add the project root to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from zipline_polygon_bundle import (
    PolygonConfig, 
    convert_trades_and_quotes_to_superbars,
    get_superbars_dates,
    superbars_dataset,
)


def main():
    parser = argparse.ArgumentParser(
        description="Generate superbars from Polygon flatfile data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Generate superbars for the last 30 days
    python generate_superbars.py --days 30
    
    # Generate superbars for a specific date range
    python generate_superbars.py --start-date 2024-01-01 --end-date 2024-01-31
    
    # Generate superbars with 5-minute windows
    python generate_superbars.py --agg-time 5minute --days 7
    
    # Check what dates have superbars data
    python generate_superbars.py --list-dates

Environment Variables:
    POLYGON_DATA_DIR: Base directory for Polygon data files
    POLYGON_FLAT_FILES_DIR: Directory containing flatfiles
    CUSTOM_ASSET_FILES_DIR: Directory for custom aggregates output
    POLYGON_MAX_WORKERS: Number of parallel workers
        """,
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD)",
    )
    
    parser.add_argument(
        "--end-date", 
        type=str,
        help="End date (YYYY-MM-DD)",
    )
    
    parser.add_argument(
        "--days",
        type=int,
        help="Number of days to process (from end-date backwards, or from today)",
    )
    
    parser.add_argument(
        "--agg-time",
        type=str,
        default="1minute",
        help="Aggregation time window (e.g., '1minute', '5minute', '1hour')",
    )
    
    parser.add_argument(
        "--calendar", 
        type=str,
        default="XNYS",
        help="Exchange calendar to use (default: XNYS)",
    )
    
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing data",
    )
    
    parser.add_argument(
        "--list-dates",
        action="store_true", 
        help="List dates that have superbars data available",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without actually processing",
    )
    
    args = parser.parse_args()
    
    # Ensure parse_date returns the correct type for PolygonConfig
    if args.days:
        end_date = parse_date(args.end_date) if args.end_date else Date(datetime.date.today())
        start_date = Date(end_date - datetime.timedelta(days=args.days))
    else:
        start_date = parse_date(args.start_date, raise_oob=False) if args.start_date else None
        end_date = parse_date(args.end_date, raise_oob=False) if args.end_date else None

    # Create config
    try:
        config = PolygonConfig(
            environ=os.environ,
            calendar_name=args.calendar,
            start_date=start_date,
            end_date=end_date,
            agg_time=args.agg_time,
        )
    except Exception as e:
        print(f"Error creating config: {e}")
        print("Make sure POLYGON_DATA_DIR environment variable is set")
        return 1
    
    print(f"Polygon data directory: {config.data_dir}")
    print(f"Trades directory: {config.trades_dir}")
    print(f"Quotes directory: {config.quotes_dir}")
    print(f"Superbars output directory: {config.superbars_dir}")
    print(f"Date range: {config.start_timestamp.date()} to {config.end_timestamp.date()}")
    print(f"Aggregation time: {config.agg_time}")
    print()
    
    # List existing dates if requested
    if args.list_dates:
        existing_dates = get_superbars_dates(config)
        if existing_dates:
            print("Dates with superbars data:")
            for date in sorted(existing_dates):
                print(f"  {date}")
        else:
            print("No superbars data found")
        return 0
    
    # Check if required directories exist
    if not os.path.exists(config.trades_dir):
        print(f"Error: Trades directory not found: {config.trades_dir}")
        return 1
    
    if not os.path.exists(config.quotes_dir):
        print(f"Warning: Quotes directory not found: {config.quotes_dir}")
        print("Superbars will be generated from trades data only")
    
    if args.dry_run:
        print("DRY RUN - would process the following:")
        schedule = config.calendar.trading_index(
            start=config.start_timestamp, end=config.end_timestamp, period="1D"
        )
        for timestamp in schedule:
            date = timestamp.to_pydatetime().date()
            trades_file = config.date_to_csv_file_path(date)
            quotes_file = f"{config.quotes_dir}/{date.strftime('%Y/%m/%Y-%m-%d.csv.gz')}"
            
            trades_exists = os.path.exists(trades_file)
            quotes_exists = os.path.exists(quotes_file)
            
            print(f"  {date}: trades={'✓' if trades_exists else '✗'}, quotes={'✓' if quotes_exists else '✗'}")
        return 0
    
    # Generate superbars
    try:
        print("Starting superbars generation...")
        output_dir = convert_trades_and_quotes_to_superbars(config, overwrite=args.overwrite)
        print(f"Successfully generated superbars to: {output_dir}")
        
        # Show summary
        final_dates = get_superbars_dates(config)
        print(f"Total dates with superbars: {len(final_dates)}")
        
        if final_dates:
            print(f"Date range: {min(final_dates)} to {max(final_dates)}")
            
            # Try to load dataset and show sample
            try:
                dataset = superbars_dataset(config)
                sample = dataset.head(5)
                print(f"Sample data shape: {sample.shape}")
                print("Sample columns:", list(sample.column_names))
            except Exception as e:
                print(f"Could not load dataset sample: {e}")
        
        return 0
        
    except Exception as e:
        print(f"Error generating superbars: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
