from .config import PolygonConfig, PARTITION_COLUMN_NAME, to_partition_key
from .trades import (
    generate_csv_trades_tables,
    ordinary_trades_mask,
    append_by_date_keys,
    by_date_hive_partitioning,
)
from .quotes import quotes_schema, cast_quotes

from typing import Iterator, Tuple
import datetime
import os
import json

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.dataset as pa_ds
import pyarrow.fs as pa_fs
import pandas as pd
import numpy as np

import pandas as pd
import numpy as np


def superbars_schema() -> pa.Schema:
    """
    Extended schema for superbars based on Algoseek US.Equity.TAQ.Minute.Bars.Ext specification.
    This implements the full 89-field specification as closely as possible.
    """
    price_type = pa.float64()
    timestamp_type = pa.timestamp("ns", tz="UTC")
    time_type = pa.string()  # HHMMSSMMM format
    
    return pa.schema([
        # Core identification fields
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("window_start", timestamp_type, nullable=False),
        
        # Quote fields - OpenBar times and prices
        pa.field("open_bar_time", time_type, nullable=True),
        pa.field("open_bid_price", price_type, nullable=True),
        pa.field("open_bid_size", pa.int64(), nullable=True), 
        pa.field("open_ask_price", price_type, nullable=True),
        pa.field("open_ask_size", pa.int64(), nullable=True),
        
        # Trade fields - First trade 
        pa.field("first_trade_time", time_type, nullable=True),
        pa.field("first_trade_price", price_type, nullable=True),
        pa.field("first_trade_size", pa.int64(), nullable=True),
        
        # Quote fields - High bid/ask
        pa.field("high_bid_time", time_type, nullable=True),
        pa.field("high_bid_price", price_type, nullable=True),
        pa.field("high_bid_size", pa.int64(), nullable=True),
        pa.field("ask_price_at_high_bid_price", price_type, nullable=True),
        pa.field("ask_size_at_high_bid_price", pa.int64(), nullable=True),
        
        # Trade fields - High trade
        pa.field("high_trade_time", time_type, nullable=True),
        pa.field("high_trade_price", price_type, nullable=True),
        pa.field("high_trade_size", pa.int64(), nullable=True),
        
        # Quote fields - Low bid/ask  
        pa.field("low_bid_time", time_type, nullable=True),
        pa.field("low_bid_price", price_type, nullable=True),
        pa.field("low_bid_size", pa.int64(), nullable=True),
        pa.field("ask_price_at_low_bid_price", price_type, nullable=True),
        pa.field("ask_size_at_low_bid_price", pa.int64(), nullable=True),
        
        # Trade fields - Low trade
        pa.field("low_trade_time", time_type, nullable=True),
        pa.field("low_trade_price", price_type, nullable=True),
        pa.field("low_trade_size", pa.int64(), nullable=True),
        
        # Quote fields - CloseBar times and prices
        pa.field("close_bar_time", time_type, nullable=True),
        pa.field("close_bid_price", price_type, nullable=True),
        pa.field("close_bid_size", pa.int64(), nullable=True),
        pa.field("close_ask_price", price_type, nullable=True),
        pa.field("close_ask_size", pa.int64(), nullable=True),
        
        # Trade fields - Last trade
        pa.field("last_trade_time", time_type, nullable=True),
        pa.field("last_trade_price", price_type, nullable=True),
        pa.field("last_trade_size", pa.int64(), nullable=True),
        
        # Spread fields
        pa.field("min_spread", price_type, nullable=True),
        pa.field("max_spread", price_type, nullable=True),
        
        # Trade aggregation fields
        pa.field("cancel_size", pa.int64(), nullable=True),
        pa.field("volume_weight_price", price_type, nullable=True),  # VWAP
        
        # Quote count
        pa.field("nbbo_quote_count", pa.int64(), nullable=True),
        
        # Trade-at fields (volume at different price levels)
        pa.field("trade_at_bid", pa.int64(), nullable=True),
        pa.field("trade_at_bid_mid", pa.int64(), nullable=True),
        pa.field("trade_at_mid", pa.int64(), nullable=True),
        pa.field("trade_at_mid_ask", pa.int64(), nullable=True),
        pa.field("trade_at_ask", pa.int64(), nullable=True),
        pa.field("trade_at_cross_or_locked", pa.int64(), nullable=True),
        
        # Volume and trade count
        pa.field("volume", pa.int64(), nullable=True),
        pa.field("total_trades", pa.int64(), nullable=True),
        
        # FINRA volume
        pa.field("finra_volume", pa.int64(), nullable=True),
        
        # Tick direction volumes
        pa.field("uptick_volume", pa.int64(), nullable=True),
        pa.field("downtick_volume", pa.int64(), nullable=True),
        pa.field("repeat_uptick_volume", pa.int64(), nullable=True),
        pa.field("repeat_downtick_volume", pa.int64(), nullable=True),
        pa.field("unknown_volume", pa.int64(), nullable=True),
        
        # Additional fields for extended bars (beyond standard 54)
        pa.field("high_ask_time", time_type, nullable=True),
        pa.field("high_ask_price", price_type, nullable=True),
        pa.field("high_ask_size", pa.int64(), nullable=True),
        pa.field("bid_price_at_high_ask_price", price_type, nullable=True),
        pa.field("bid_size_at_high_ask_price", pa.int64(), nullable=True),
        
        pa.field("low_ask_time", time_type, nullable=True),
        pa.field("low_ask_price", price_type, nullable=True),
        pa.field("low_ask_size", pa.int64(), nullable=True),
        pa.field("bid_price_at_low_ask_price", price_type, nullable=True),
        pa.field("bid_size_at_low_ask_price", pa.int64(), nullable=True),
        
        # FINRA VWAP
        pa.field("finra_volume_weight_price", price_type, nullable=True),
        
        # Trade to mid calculations
        pa.field("trade_to_mid_vol_weight", price_type, nullable=True),
        pa.field("trade_to_mid_vol_weight_relative", price_type, nullable=True),
        
        # Time weighted bid/ask  
        pa.field("time_weight_bid", price_type, nullable=True),
        pa.field("time_weight_ask", price_type, nullable=True),
        
        # Missing Algoseek fields to complete the 89-field specification
        pa.field("odd_lot_trade_count", pa.int64(), nullable=True),
        pa.field("odd_lot_total_shares", pa.int64(), nullable=True),
        pa.field("total_volume", pa.int64(), nullable=True),  # Volume + FinraVolume
        pa.field("total_quote_count", pa.int64(), nullable=True),
        pa.field("total_volume_weight_price", price_type, nullable=True),  # Combined VWAP
        pa.field("time_weight_spread", price_type, nullable=True),
        pa.field("spread_valid_time", pa.int64(), nullable=True),  # milliseconds
        pa.field("exchange_trade_count", pa.int64(), nullable=True),
        pa.field("finra_trade_count", pa.int64(), nullable=True),
        pa.field("exchanges_bid_count", pa.int64(), nullable=True),
        pa.field("exchanges_ask_count", pa.int64(), nullable=True),
        pa.field("volume_weight_spread", price_type, nullable=True),
        pa.field("time_weight_bid_size", price_type, nullable=True),
        pa.field("time_weight_ask_size", price_type, nullable=True),
        
        # Trade count variants (count instead of volume)
        pa.field("trade_at_bid_count", pa.int64(), nullable=True),
        pa.field("trade_at_bid_mid_count", pa.int64(), nullable=True),
        pa.field("trade_at_mid_count", pa.int64(), nullable=True),
        pa.field("trade_at_mid_ask_count", pa.int64(), nullable=True),
        pa.field("trade_at_ask_count", pa.int64(), nullable=True),
        pa.field("trade_at_cross_or_locked_count", pa.int64(), nullable=True),
        
        # Prior Reference Price fields
        pa.field("prior_reference_price_trade_count", pa.int64(), nullable=True),
        pa.field("prior_reference_price_trade_shares", pa.int64(), nullable=True),
        pa.field("volume_weight_price_exclude_prp", price_type, nullable=True),
        pa.field("volume_weight_spread_exclude_prp", price_type, nullable=True),
        
        # Advanced spread and distribution metrics
        pa.field("relative_spread_average", price_type, nullable=True),
        pa.field("trade_cumul_distribution_to_bid", pa.string(), nullable=True),  # Distribution string
        pa.field("retail_trf_buy_size", pa.int64(), nullable=True),
        pa.field("retail_trf_sell_size", pa.int64(), nullable=True),
        
        # Additional derived metrics for ML/analysis (beyond Algoseek)
        pa.field("price_range", price_type, nullable=True),
        pa.field("volatility_estimate", price_type, nullable=True),
        pa.field("momentum_1min", price_type, nullable=True),
        pa.field("relative_spread", price_type, nullable=True),
        pa.field("mid_price_returns", price_type, nullable=True),
        pa.field("order_flow_imbalance", price_type, nullable=True),
        pa.field("effective_spread_mean", price_type, nullable=True),
        pa.field("realized_spread_mean", price_type, nullable=True),
        pa.field("price_impact", price_type, nullable=True),
        
        # Summary statistics
        pa.field("exchange_count", pa.int32(), nullable=True),
        pa.field("condition_code_count", pa.int32(), nullable=True),
        
        # Cumulative metrics
        pa.field("cumulative_volume", pa.int64(), nullable=True),
        pa.field("cumulative_traded_value", price_type, nullable=True),
        
        # Partitioning and date fields
        pa.field("date", pa.date32(), nullable=False),
        pa.field("year", pa.uint16(), nullable=False),
        pa.field("month", pa.uint8(), nullable=False),
        pa.field(PARTITION_COLUMN_NAME, pa.string(), nullable=False),
    ])


def load_condition_codes() -> dict:
    """Load condition codes from JSON file."""
    conditions_path = "data/conditions-stocks.json"
    with open(conditions_path, 'r') as f:
        data = json.load(f)
    
    # Create lookup dicts for trade and quote conditions
    trade_conditions = {}
    quote_conditions = {}
    
    for condition in data['results']:
        cond_id = condition['id']
        if 'trade' in condition.get('data_types', []):
            trade_conditions[cond_id] = condition
        if any(dt in condition.get('data_types', []) for dt in ['bbo', 'nbbo']):
            quote_conditions[cond_id] = condition
    
    return {
        'trade_conditions': trade_conditions,
        'quote_conditions': quote_conditions
    }


def classify_trade_direction(price, bid_price, ask_price, prev_price=None):
    """
    Classify trade as buyer-initiated (1), seller-initiated (-1), or unknown (0).
    Uses tick rule and quote rule.
    """
    if pd.isna(price) or pd.isna(bid_price) or pd.isna(ask_price):
        return 0
    
    mid_price = (bid_price + ask_price) / 2
    
    # Quote rule first
    if price > mid_price:
        return 1  # buyer-initiated
    elif price < mid_price:
        return -1  # seller-initiated
    else:
        # Tick rule for trades at mid
        if prev_price is not None and not pd.isna(prev_price):
            if price > prev_price:
                return 1
            elif price < prev_price:
                return -1
    
    return 0  # unknown


def calculate_garman_klass_volatility(open_price, high_price, low_price, close_price):
    """
    Calculate Garman-Klass volatility estimator.
    More efficient than using only close-to-close returns.
    """
    if any(pd.isna(x) or x <= 0 for x in [open_price, high_price, low_price, close_price]):
        return np.nan
    
    try:
        ln_hl = np.log(high_price / low_price)
        ln_co = np.log(close_price / open_price)
        
        # Garman-Klass formula
        gk_vol = 0.5 * ln_hl**2 - (2 * np.log(2) - 1) * ln_co**2
        return np.sqrt(gk_vol) if gk_vol >= 0 else np.nan
    except (ValueError, ZeroDivisionError):
        return np.nan


def generate_csv_quotes_tables(
    config: PolygonConfig, overwrite: bool = False
) -> Iterator[Tuple[datetime.date, pa.Table]]:
    """Generator for quotes tables from flatfile CSVs."""
    schedule = config.calendar.trading_index(
        start=config.start_timestamp, end=config.end_timestamp, period="1D"
    )
    for timestamp in schedule:
        date: datetime.date = timestamp.to_pydatetime().date()
        quotes_csv_path = f"{config.quotes_dir}/{date.strftime('%Y/%m/%Y-%m-%d.csv.gz')}"
        
        # Check if file exists
        if not os.path.exists(quotes_csv_path):
            continue
            
        try:
            convert_options = pa_csv.ConvertOptions(column_types=quotes_schema(raw=True))
            quotes = pa_csv.read_csv(quotes_csv_path, convert_options=convert_options)
            quotes = cast_quotes(quotes)
            yield date, quotes
        except Exception as e:
            print(f"Error loading quotes for {date}: {e}")
            continue


def trades_and_quotes_to_superbars(
    config: PolygonConfig,
    date: datetime.date,
    trades_table: pa.Table,
    quotes_table: pa.Table = None,
) -> pa.Table:
    """
    Convert trades and quotes data to superbars with extended features using PyArrow.
    """
    print(f"Processing superbars for {date=}")

    if len(trades_table) == 0:
        # Return empty table with correct schema
        return pa.table([], schema=superbars_schema())

    # Add window_start column for aggregation
    window_start = pa.compute.floor_temporal(
        trades_table["sip_timestamp"],
        multiple=config.agg_timedelta.seconds,
        unit="second",
    )
    trades_table = trades_table.append_column("window_start", window_start)

    # Add traded_value column
    traded_value = pa.compute.multiply(trades_table["price"], trades_table["size"])
    trades_table = trades_table.append_column("traded_value", traded_value)

    # Perform group-by aggregation using PyArrow
    grouped = trades_table.group_by(["ticker", "window_start"])
    aggregated = grouped.aggregate(
        [
            ("price", "min"),
            ("price", "max"),
            ("price", "mean"),
            ("price", "stddev"),
            ("price", "sum"),
            ("size", "sum"),
            ("size", "mean"),
            ("size", "stddev"),
            ("size", "min"),
            ("size", "max"),
            ("traded_value", "sum"),
            ("sip_timestamp", "min"),
            ("sip_timestamp", "max"),
        ]
    )

    # Rename columns to match schema
    column_mapping = {
        "price_min": "low",
        "price_max": "high",
        "price_mean": "vwap",
        "price_stddev": "price_std",
        "price_sum": "traded_value",
        "size_sum": "volume",
        "size_mean": "trade_size_mean",
        "size_stddev": "trade_size_std",
        "size_min": "trade_size_min",
        "size_max": "trade_size_max",
        "sip_timestamp_min": "time_to_first_trade",
        "sip_timestamp_max": "time_to_last_trade",
    }

    for old_name, new_name in column_mapping.items():
        aggregated = aggregated.rename_columns({old_name: new_name})

    # Add cumulative metrics
    aggregated = aggregated.sort_by([("ticker", "ascending"), ("window_start", "ascending")])
    cumulative_traded_value = pa_compute.cumulative_sum(aggregated["traded_value"])
    cumulative_volume = pa_compute.cumulative_sum(aggregated["volume"])
    aggregated = aggregated.append_column("cumulative_traded_value", cumulative_traded_value)
    aggregated = aggregated.append_column("cumulative_volume", cumulative_volume)

    # Cast to the correct schema
    schema = superbars_schema()
    columns_dict = {}

    for field in schema:
        if field.name in aggregated.column_names:
            columns_dict[field.name] = aggregated[field.name]
        else:
            # Create null array for missing columns
            null_array = pa.nulls(len(aggregated), type=field.type)
            columns_dict[field.name] = null_array

    return pa.table(columns_dict, schema=schema)
    
    # Add traded_value column
    trades_table = trades_table.append_column(
        "traded_value", 
        pa_compute.multiply(trades_table["price"], trades_table["size"])
    )
    
    # Sort by ticker and timestamp
    trades_table = trades_table.sort_by([
        ("ticker", "ascending"), 
        ("sip_timestamp", "ascending")
    ])
    
    # Convert to pandas for complex calculations
    trades_df = trades_table.to_pandas()
    
    # Group by ticker and window_start
    grouped = trades_df.groupby(['ticker', 'window_start'])
    
    # Calculate basic OHLCV aggregates
    basic_aggs = grouped.agg({
        'price': ['first', 'max', 'min', 'last', 'std', 'count'],
        'size': ['sum', 'mean', 'std', 'min', 'max'],
        'traded_value': 'sum',
        'sip_timestamp': ['min', 'max'],
        'exchange': 'nunique',
    }).round(10)
    
    # Flatten column names
    basic_aggs.columns = ['_'.join(col).strip() for col in basic_aggs.columns]
    basic_aggs = basic_aggs.reset_index()
    
    # Rename columns to match schema
    column_mapping = {
        'price_first': 'open',
        'price_max': 'high', 
        'price_min': 'low',
        'price_last': 'close',
        'price_std': 'price_std',
        'price_count': 'transactions',
        'size_sum': 'volume',
        'size_mean': 'trade_size_mean',
        'size_std': 'trade_size_std',
        'size_min': 'trade_size_min',
        'size_max': 'trade_size_max',
        'traded_value_sum': 'traded_value',
        'sip_timestamp_min': 'first_trade_time',
        'sip_timestamp_max': 'last_trade_time',
        'exchange_nunique': 'exchange_count',
    }
    
    basic_aggs = basic_aggs.rename(columns=column_mapping)
    
    # Calculate additional derived metrics
    basic_aggs['vwap'] = basic_aggs['traded_value'] / basic_aggs['volume']
    basic_aggs['price_range'] = basic_aggs['high'] - basic_aggs['low']
    basic_aggs['momentum_1min'] = (basic_aggs['close'] - basic_aggs['open']) / basic_aggs['open']
    
    # Calculate time-based features
    window_start_ts = pd.to_datetime(basic_aggs['window_start'])
    basic_aggs['time_to_first_trade'] = (
        pd.to_datetime(basic_aggs['first_trade_time']) - window_start_ts
    ).dt.total_seconds() * 1000  # milliseconds
    
    basic_aggs['time_to_last_trade'] = (
        pd.to_datetime(basic_aggs['last_trade_time']) - window_start_ts
    ).dt.total_seconds() * 1000  # milliseconds
    
    basic_aggs['trading_time_span'] = (
        pd.to_datetime(basic_aggs['last_trade_time']) - 
        pd.to_datetime(basic_aggs['first_trade_time'])
    ).dt.total_seconds() * 1000  # milliseconds
    
    # Calculate Garman-Klass volatility
    basic_aggs['volatility_estimate'] = basic_aggs.apply(
        lambda row: calculate_garman_klass_volatility(
            row['open'], row['high'], row['low'], row['close']
        ), axis=1
    )
    
    # Calculate quantiles for trade sizes and prices
    quantile_aggs = grouped.agg({
        'size': lambda x: x.quantile([0.25, 0.5, 0.75]).tolist(),
        'price': lambda x: x.quantile([0.25, 0.75]).tolist(),
    }).reset_index()
    
    # Extract quantile values
    basic_aggs['trade_size_q25'] = quantile_aggs['size'].apply(lambda x: x[0] if len(x) >= 1 else np.nan)
    basic_aggs['trade_size_q50'] = quantile_aggs['size'].apply(lambda x: x[1] if len(x) >= 2 else np.nan)  
    basic_aggs['trade_size_q75'] = quantile_aggs['size'].apply(lambda x: x[2] if len(x) >= 3 else np.nan)
    basic_aggs['price_q25'] = quantile_aggs['price'].apply(lambda x: x[0] if len(x) >= 1 else np.nan)
    basic_aggs['price_q75'] = quantile_aggs['price'].apply(lambda x: x[1] if len(x) >= 2 else np.nan)
    
    # Process quotes data if available
    if quotes_table is not None and len(quotes_table) > 0:
        quotes_features = process_quotes_for_superbars(config, quotes_table)
        # Merge quotes features with trades
        basic_aggs = basic_aggs.merge(
            quotes_features, 
            on=['ticker', 'window_start'], 
            how='left'
        )
    else:
        # Add empty quote columns
        quote_columns = [
            'bid_open', 'bid_high', 'bid_low', 'bid_close',
            'ask_open', 'ask_high', 'ask_low', 'ask_close',
            'spread_open', 'spread_high', 'spread_low', 'spread_close',
            'spread_mean', 'spread_std', 'spread_weighted_mean',
            'bid_size_mean', 'ask_size_mean', 'bid_size_max', 'ask_size_max',
            'total_bid_size', 'total_ask_size', 'quote_updates',
            'bid_updates', 'ask_updates', 'mid_price_returns',
            'effective_spread_mean', 'realized_spread_mean'
        ]
        for col in quote_columns:
            basic_aggs[col] = np.nan
    
    # Add order flow features (simplified without tick-by-tick analysis)
    basic_aggs['buyer_initiated_volume'] = basic_aggs['volume'] * 0.5  # placeholder
    basic_aggs['seller_initiated_volume'] = basic_aggs['volume'] * 0.5  # placeholder
    basic_aggs['order_flow_imbalance'] = 0.0  # placeholder
    basic_aggs['volume_imbalance'] = 0.0  # placeholder
    basic_aggs['volume_weighted_price_std'] = basic_aggs['price_std']  # simplified
    basic_aggs['primary_exchange_volume'] = basic_aggs['volume']  # simplified
    basic_aggs['condition_code_count'] = 1  # simplified
    basic_aggs['effective_spread_mean'] = np.nan
    basic_aggs['realized_spread_mean'] = np.nan
    basic_aggs['relative_spread'] = np.nan
    basic_aggs['price_impact'] = np.nan
    
    # Calculate cumulative metrics by ticker
    basic_aggs = basic_aggs.sort_values(['ticker', 'window_start'])
    basic_aggs['cumulative_traded_value'] = basic_aggs.groupby('ticker')['traded_value'].cumsum()
    basic_aggs['cumulative_volume'] = basic_aggs.groupby('ticker')['volume'].cumsum()
    
    # Add date partition columns
    basic_aggs = append_by_date_keys(date, pa.table(basic_aggs)).to_pandas()
    basic_aggs[PARTITION_COLUMN_NAME] = basic_aggs['ticker'].apply(to_partition_key)
    
    # Drop temporary columns
    cols_to_drop = ['first_trade_time', 'last_trade_time']
    basic_aggs = basic_aggs.drop(columns=[col for col in cols_to_drop if col in basic_aggs.columns])
    
    # Convert back to arrow table with correct schema
    result_table = pa.table(basic_aggs)
    
    # Cast to the correct schema, filling missing columns with nulls
    schema = superbars_schema()
    columns_dict = {}
    
    for field in schema:
        if field.name in result_table.column_names:
            columns_dict[field.name] = result_table[field.name]
        else:
            # Create null array for missing columns
            null_array = pa.nulls(len(result_table), type=field.type)
            columns_dict[field.name] = null_array
    
    return pa.table(columns_dict, schema=schema)


def process_quotes_for_superbars(config: PolygonConfig, quotes_table: pa.Table) -> pd.DataFrame:
    """
    Process quotes data to extract BBO features for superbars.
    Returns a DataFrame with quote-based features aggregated by ticker and window_start.
    """
    if len(quotes_table) == 0:
        return pd.DataFrame()
    
    # Add window_start column
    quotes_table = quotes_table.append_column(
        "window_start",
        pa_compute.floor_temporal(
            quotes_table["sip_timestamp"],
            multiple=config.agg_timedelta.seconds,
            unit="second"
        ),
    )
    
    # Calculate spread
    quotes_table = quotes_table.append_column(
        "spread",
        pa_compute.subtract(quotes_table["ask_price"], quotes_table["bid_price"])
    )
    
    # Calculate mid price
    quotes_table = quotes_table.append_column(
        "mid_price",
        pa_compute.divide(
            pa_compute.add(quotes_table["ask_price"], quotes_table["bid_price"]),
            pa.scalar(2.0)
        )
    )
    
    # Convert to pandas for complex aggregations
    quotes_df = quotes_table.to_pandas()
    
    # Group by ticker and window_start
    grouped = quotes_df.groupby(['ticker', 'window_start'])
    
    # Calculate quote-based aggregates
    quote_aggs = grouped.agg({
        'bid_price': ['first', 'max', 'min', 'last'],
        'ask_price': ['first', 'max', 'min', 'last'],
        'spread': ['first', 'max', 'min', 'last', 'mean', 'std'],
        'bid_size': ['mean', 'max', 'sum'],
        'ask_size': ['mean', 'max', 'sum'],
        'mid_price': ['first', 'last'],
        'sip_timestamp': 'count',
    }).round(10)
    
    # Flatten column names
    quote_aggs.columns = ['_'.join(col).strip() for col in quote_aggs.columns]
    quote_aggs = quote_aggs.reset_index()
    
    # Rename to match schema
    quote_mapping = {
        'bid_price_first': 'bid_open',
        'bid_price_max': 'bid_high',
        'bid_price_min': 'bid_low', 
        'bid_price_last': 'bid_close',
        'ask_price_first': 'ask_open',
        'ask_price_max': 'ask_high',
        'ask_price_min': 'ask_low',
        'ask_price_last': 'ask_close',
        'spread_first': 'spread_open',
        'spread_max': 'spread_high',
        'spread_min': 'spread_low',
        'spread_last': 'spread_close',
        'spread_mean': 'spread_mean',
        'spread_std': 'spread_std',
        'bid_size_mean': 'bid_size_mean',
        'ask_size_mean': 'ask_size_mean',
        'bid_size_max': 'bid_size_max',
        'ask_size_max': 'ask_size_max',
        'bid_size_sum': 'total_bid_size',
        'ask_size_sum': 'total_ask_size',
        'sip_timestamp_count': 'quote_updates',
    }
    
    quote_aggs = quote_aggs.rename(columns=quote_mapping)
    
    # Calculate additional quote features
    quote_aggs['spread_weighted_mean'] = quote_aggs['spread_mean']  # simplified
    quote_aggs['bid_updates'] = quote_aggs['quote_updates'] * 0.5   # simplified
    quote_aggs['ask_updates'] = quote_aggs['quote_updates'] * 0.5   # simplified
    
    # Calculate mid price returns
    quote_aggs['mid_price_returns'] = (
        quote_aggs['mid_price_last'] - quote_aggs['mid_price_first']
    ) / quote_aggs['mid_price_first']
    
    # Drop temporary columns
    temp_cols = ['mid_price_first', 'mid_price_last']
    quote_aggs = quote_aggs.drop(columns=[col for col in temp_cols if col in quote_aggs.columns])
    
    return quote_aggs


def convert_trades_and_quotes_to_superbars(
    config: PolygonConfig, overwrite: bool = False
) -> str:
    """
    Convert trades and quotes data to superbars with extended features.
    """
    if overwrite:
        print("WARNING: overwrite not implemented/ignored.")
    
    # Create superbars directory
    superbars_dir = os.path.join(config.custom_asset_files_dir, "superbars")
    os.makedirs(superbars_dir, exist_ok=True)
    
    print(f"Generating superbars to {superbars_dir}")
    
    # Get trades and quotes data generators
    trades_generator = generate_csv_trades_tables(config, overwrite)
    quotes_generator = dict(generate_csv_quotes_tables(config, overwrite))
    
    def file_visitor(written_file):
        print(f"Written: {written_file.path}")
    
    for date, trades_table in trades_generator:
        # Filter to ordinary trades only
        filtered_trades = trades_table.filter(ordinary_trades_mask(trades_table))
        
        # Get corresponding quotes data
        quotes_table = quotes_generator.get(date)
        
        # Generate superbars
        superbars_table = trades_and_quotes_to_superbars(
            config, date, filtered_trades, quotes_table
        )
        
        if len(superbars_table) > 0:
            # Write to dataset
            pa_ds.write_dataset(
                superbars_table,
                filesystem=config.filesystem,
                base_dir=superbars_dir,
                partitioning=by_date_hive_partitioning(),
                format="parquet",
                existing_data_behavior="overwrite_or_ignore",
                file_visitor=file_visitor,
            )
        
        del trades_table
        if quotes_table is not None:
            del quotes_table
    
    print(f"Generated superbars to {superbars_dir}")
    return superbars_dir


def get_superbars_dates(config: PolygonConfig) -> set[datetime.date]:
    """Get dates that have superbars data available."""
    superbars_dir = os.path.join(config.custom_asset_files_dir, "superbars")
    
    file_info = config.filesystem.get_file_info(superbars_dir)
    if file_info.type == pa_fs.FileType.NotFound:
        return set()
    
    try:
        superbars_ds = pa_ds.dataset(
            superbars_dir,
            format="parquet",
            schema=superbars_schema(),
            partitioning=by_date_hive_partitioning(),
        )
        return set([
            pa_ds.get_partition_keys(fragment.partition_expression).get("date")
            for fragment in superbars_ds.get_fragments()
        ])
    except Exception:
        return set()


def superbars_dataset(config: PolygonConfig) -> pa_ds.Dataset:
    """Create a PyArrow dataset for superbars."""
    superbars_dir = os.path.join(config.custom_asset_files_dir, "superbars")
    
    return pa_ds.dataset(
        superbars_dir,
        format="parquet", 
        schema=superbars_schema(),
        partitioning=by_date_hive_partitioning(),
        filesystem=config.filesystem
    )
