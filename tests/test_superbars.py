#!/usr/bin/env python3
"""
Test script for superbars functionality.

This script validates the superbars implementation with sample data.
"""

import os
import sys
import datetime
import tempfile
import shutil

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import pyarrow as pa
import numpy as np

from exchange_calendars.calendar_helpers import parse_date

from zipline_polygon_bundle import (
    PolygonConfig,
    superbars_schema,
    trades_and_quotes_to_superbars,
    load_condition_codes,
)


def create_sample_trades_data():
    """Create sample trades data for testing."""
    np.random.seed(42)
    
    # Generate sample trades for AAPL
    n_trades = 1000
    base_time = pd.Timestamp('2024-01-02 09:30:00', tz='UTC')
    
    data = {
        'ticker': ['AAPL'] * n_trades,
        'conditions': ['0'] * n_trades,  # Regular trades
        'correction': [''] * n_trades,
        'exchange': np.random.choice([1, 2, 3, 4], n_trades),
        'id': [f'trade_{i}' for i in range(n_trades)],
        'participant_timestamp': [base_time + pd.Timedelta(seconds=i*0.1) for i in range(n_trades)],
        'price': 150.0 + np.cumsum(np.random.normal(0, 0.01, n_trades)),  # Random walk around $150
        'sequence_number': range(n_trades),
        'sip_timestamp': [base_time + pd.Timedelta(seconds=i*0.1) for i in range(n_trades)],
        'size': np.random.randint(100, 10000, n_trades),  # Random trade sizes
        'tape': [1] * n_trades,
        'trf_id': [0] * n_trades,
        'trf_timestamp': [base_time + pd.Timedelta(seconds=i*0.1) for i in range(n_trades)],
    }
    
    df = pd.DataFrame(data)
    
    # Convert to arrow table
    schema = pa.schema([
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("conditions", pa.string(), nullable=False),
        pa.field("correction", pa.string(), nullable=False),
        pa.field("exchange", pa.int8(), nullable=False),
        pa.field("id", pa.string(), nullable=False),
        pa.field("participant_timestamp", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("sequence_number", pa.int64(), nullable=False),
        pa.field("sip_timestamp", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("size", pa.int64(), nullable=False),
        pa.field("tape", pa.int8(), nullable=False),
        pa.field("trf_id", pa.int64(), nullable=False),
        pa.field("trf_timestamp", pa.timestamp("ns", tz="UTC"), nullable=False),
    ])
    
    return pa.table(df, schema=schema)


def create_sample_quotes_data():
    """Create sample quotes data for testing."""
    np.random.seed(42)
    
    # Generate sample quotes for AAPL
    n_quotes = 500
    base_time = pd.Timestamp('2024-01-02 09:30:00', tz='UTC')
    
    data = {
        'ticker': ['AAPL'] * n_quotes,
        'ask_exchange': np.random.choice([1, 2, 3, 4], n_quotes),
        'ask_price': 150.05 + np.cumsum(np.random.normal(0, 0.01, n_quotes)),
        'ask_size': np.random.randint(100, 5000, n_quotes),
        'bid_exchange': np.random.choice([1, 2, 3, 4], n_quotes),
        'bid_price': 149.95 + np.cumsum(np.random.normal(0, 0.01, n_quotes)),
        'bid_size': np.random.randint(100, 5000, n_quotes),
        'conditions': ['1'] * n_quotes,  # Regular quotes
        'indicators': ['0'] * n_quotes,
        'participant_timestamp': [base_time + pd.Timedelta(seconds=i*0.2) for i in range(n_quotes)],
        'sequence_number': range(n_quotes),
        'sip_timestamp': [base_time + pd.Timedelta(seconds=i*0.2) for i in range(n_quotes)],
        'tape': [1] * n_quotes,
        'trf_timestamp': [base_time + pd.Timedelta(seconds=i*0.2) for i in range(n_quotes)],
    }
    
    df = pd.DataFrame(data)
    
    # Convert to arrow table  
    schema = pa.schema([
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("ask_exchange", pa.int8(), nullable=False),
        pa.field("ask_price", pa.float64(), nullable=False),
        pa.field("ask_size", pa.int64(), nullable=False),
        pa.field("bid_exchange", pa.int8(), nullable=False),
        pa.field("bid_price", pa.float64(), nullable=False),
        pa.field("bid_size", pa.int64(), nullable=False),
        pa.field("conditions", pa.string(), nullable=False),
        pa.field("indicators", pa.string(), nullable=False),
        pa.field("participant_timestamp", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("sequence_number", pa.int64(), nullable=False),
        pa.field("sip_timestamp", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("tape", pa.int8(), nullable=False),
        pa.field("trf_timestamp", pa.timestamp("ns", tz="UTC"), nullable=False),
    ])
    
    return pa.table(df, schema=schema)


def test_superbars_schema():
    """Test that the superbars schema is valid."""
    print("Testing superbars schema...")
    
    schema = superbars_schema()
    print(f"Schema has {len(schema)} fields")
    
    # Check for key fields
    required_fields = [
        'ticker', 'window_start', 'open', 'high', 'low', 'close', 
        'volume', 'vwap', 'transactions', 'volatility_estimate'
    ]
    
    schema_names = [field.name for field in schema]
    
    for field in required_fields:
        if field in schema_names:
            print(f"✓ {field}")
        else:
            print(f"✗ {field} missing")
            return False
    
    print("Schema validation passed!")
    return True


def test_condition_codes():
    """Test loading condition codes."""
    print("\nTesting condition codes loading...")
    
    try:
        codes = load_condition_codes()
        print(f"Loaded {len(codes['trade_conditions'])} trade conditions")
        print(f"Loaded {len(codes['quote_conditions'])} quote conditions")
        
        # Show a few examples
        trade_conditions = codes['trade_conditions']
        if trade_conditions:
            first_trade_cond = list(trade_conditions.values())[0]
            print(f"Example trade condition: {first_trade_cond['name']}")
        
        print("Condition codes loading passed!")
        return True
        
    except Exception as e:
        print(f"✗ Condition codes loading failed: {e}")
        return False


def test_superbars_generation():
    """Test superbars generation with sample data."""
    print("\nTesting superbars generation...")
    
    # Create temporary directory for test
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Create a mock config
            environ = {
                'POLYGON_DATA_DIR': temp_dir,
                'CUSTOM_ASSET_FILES_DIR': temp_dir,
            }
            
            config = PolygonConfig(
                environ=environ,
                calendar_name='XNYS',
                start_date=parse_date("2024-01-02", raise_oob=False),
                end_date=parse_date("2024-01-02", raise_oob=False),
                agg_time='1minute'
            )
            
            # Create sample data
            trades_table = create_sample_trades_data()
            quotes_table = create_sample_quotes_data()
            
            print(f"Created {len(trades_table)} sample trades")
            print(f"Created {len(quotes_table)} sample quotes")
            
            # Generate superbars
            date = datetime.date(2024, 1, 2)
            superbars_table = trades_and_quotes_to_superbars(
                config, date, trades_table, quotes_table
            )
            
            print(f"Generated {len(superbars_table)} superbars")
            
            if len(superbars_table) > 0:
                # Convert to pandas for inspection
                df = superbars_table.to_pandas()
                print(f"Superbars columns: {len(df.columns)}")
                print(f"Sample ticker: {df['ticker'].iloc[0] if not df.empty else 'N/A'}")
                print(f"Sample OHLC: O={df['open'].iloc[0]:.2f}, H={df['high'].iloc[0]:.2f}, L={df['low'].iloc[0]:.2f}, C={df['close'].iloc[0]:.2f}")
                print(f"Sample volume: {df['volume'].iloc[0]}")
                print(f"Sample vwap: {df['vwap'].iloc[0]:.2f}")
                print(f"Sample volatility: {df['volatility_estimate'].iloc[0]:.6f}")
                
                # Check for NaN values in key fields
                key_fields = ['open', 'high', 'low', 'close', 'volume', 'vwap']
                for field in key_fields:
                    if field in df.columns:
                        nan_count = df[field].isna().sum()
                        print(f"{field} NaN count: {nan_count}")
                
                print("Superbars generation passed!")
                return True
            else:
                print("✗ No superbars generated")
                return False
                
        except Exception as e:
            print(f"✗ Superbars generation failed: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Run all tests."""
    print("=" * 50)
    print("SUPERBARS FUNCTIONALITY TESTS")
    print("=" * 50)
    
    tests = [
        test_superbars_schema,
        test_condition_codes, 
        test_superbars_generation,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"RESULTS: {passed}/{total} tests passed")
    print("=" * 50)
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
