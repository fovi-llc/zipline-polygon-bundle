from zipline_polygon_bundle.config import PolygonConfig
from zipline_polygon_bundle import get_ticker_universe
from zipline_polygon_bundle import by_date_hive_partitioning, custom_aggs_schema

import os
import argparse

import pyarrow as pa
from pyarrow import dataset as pa_ds
from pyarrow import compute as pc

import pandas as pd

import pandas_market_calendars
import numpy as np

import warnings


def get_valid_tickers(config: PolygonConfig):
    tickers = get_ticker_universe(config, fetch_missing=True)
    return pa.array([ticker for ticker in tickers.index.get_level_values('ticker').to_list() if "TEST" not in ticker])


def find_price_runs(df: pd.DataFrame, min_price: float = 0.5, min_gain: float = 15, min_traded_value: float = 100000, min_duration: int = 10):
    df = df.set_index('window_start').sort_index()
    session_index = pd.date_range(start=df.index[0],
                                  end=df.index[-1],
                                  freq=pd.Timedelta(seconds=60))
    df = df.reindex(session_index)
    prices = df['vwap'].values
    diffs = np.diff(prices)
    change_points = np.where(diffs < 0)[0] + 1

    # Add start and end points
    change_points = np.concatenate(([0], change_points, [len(prices)]))

    runs = []
    for i in range(len(change_points) - 1):
        start_idx = change_points[i]
        end_idx = change_points[i + 1] - 1
        if end_idx >= start_idx + 3:
            start_price = prices[start_idx]
            end_price = prices[end_idx]
            gain = 100 * (end_price - start_price) / start_price
            if (gain > min_gain) & (start_price > min_price):
                # TODO: Return up to ten prices and resample if longer to fit.
                start_dt = df.index[start_idx]
                end_dt = df.index[end_idx]
                price_series = df.loc[start_dt:end_dt]['vwap']
                volume_series = df.loc[start_dt:end_dt]["volume"]
                volume = volume_series.sum()
                traded_value = (price_series * volume_series).sum()
                if traded_value > min_traded_value:
                    previous_price = prices[start_idx-1] if (start_idx > 0) else None
                    next_price = prices[end_idx+1] if (end_idx + 1 < len(prices)) else None
                    if len(price_series) > min_duration:
                        price_series = price_series.iloc[::(len(price_series) // min_duration)]
                    runs.append({"gain": gain,
                                 "duration": end_idx - start_idx,
                                 "volume": volume,
                                 "traded_value": traded_value,
                                 "start_dt": start_dt,
                                 "end_dt": end_dt,
                                 "previous_price": previous_price,
                                 "start_price": start_price,
                                 "end_price": end_price,
                                 "next_price": next_price,
                                 "prices": price_series.round(3).to_list()
                                })
    if len(runs) > 0:
        return pd.DataFrame(runs).set_index("start_dt").sort_index()
    # This doesn't help with the empty warning.
    # return pd.DataFrame(data=[],
    #                     index=pd.DatetimeIndex([], name='start_dt'))
    return None


def label_price_runs_by_ticker(df: pd.DataFrame):
    # Ignoring this warning from the groupby call.
    # Tried suggested fixes but haven't figured out one that works.
    # FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. 
    # In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes.
    # To retain the old behavior, exclude the relevant entries before the concat operation.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        return df.set_index('ticker').sort_index().groupby(level=0).apply(find_price_runs, include_groups=False)


def label_price_runs_df(config: PolygonConfig, valid_tickers: pa.Array) -> pd.DataFrame:
    calendar = pandas_market_calendars.get_calendar(config.calendar_name)
    schedule = calendar.schedule(start_date=config.start_date,
                                 end_date=config.end_date,
                                 start="pre",
                                 end="post")
    labeled_aggs_dfs = []
    for date, sessions in schedule.iterrows():
        # print(f"{date=} {sessions=}")
        start_dt = sessions['pre']
        end_dt = sessions['market_open']
        print(f"{date=} {start_dt=} {end_dt=}")
        aggs_ds = pa_ds.dataset(config.custom_aggs_dir,
                                format="parquet",
                                schema=custom_aggs_schema(),
                                partitioning=by_date_hive_partitioning())
        date_filter_expr = ((pc.field('year') == date.year)
                            & (pc.field('month') == date.month)
                            & (pc.field('date') == date.to_pydatetime().date()))
        # print(f"{date_filter_expr=}")
        for fragment in aggs_ds.get_fragments(filter=date_filter_expr):
            session_filter = ((pc.field('window_start') >= start_dt)
                              & (pc.field('window_start') < end_dt)
                              & pc.is_in(pc.field('ticker'), valid_tickers)
                             )
            # Sorting table doesn't seem to avoid needing to sort the df.  Maybe use_threads=False on to_pandas would help?
            # table = fragment.to_table(filter=session_filter).sort_by([('ticker', 'ascending'), ('window_start', 'descending')])
            table = fragment.to_table(filter=session_filter)
            if table.num_rows > 0:
                days_runs_df = label_price_runs_by_ticker(table.to_pandas())
                if days_runs_df is not None:
                    days_runs_df.dropna(inplace=True)
                    if len(days_runs_df) > 0:
                        labeled_aggs_dfs.append(days_runs_df)

    return pd.concat(labeled_aggs_dfs)


# export POLYGON_TICKERS_DIR=/home/jovyan/work/data/tickers
# export CUSTOM_ASSET_FILES_DIR=/home/jovyan/work/data

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--calendar_name", default="NYSE")
    parser.add_argument("--start_date", default="2014-06-16")
    parser.add_argument("--end_date", default="2024-09-06")
    # parser.add_argument("--start_date", default="2020-01-01")
    # parser.add_argument("--end_date", default="2020-12-31")

    parser.add_argument("--agg_duration", default="1minute")

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
    )

    valid_tickers = get_valid_tickers(from_config)
    os.makedirs(to_config.aggs_dir, exist_ok=True)
    signals_file_path = os.path.join(
        to_config.aggs_dir,
        f"labels_{to_config.start_timestamp.date().isoformat()}_{to_config.end_timestamp.date().isoformat()}.parquet",
    )
    print(f"Will write labels to {signals_file_path=}")
    labels_df = label_price_runs_df(from_config, valid_tickers)
    labels_df.info()
    labels_df.to_parquet(signals_file_path)
    print(f"Wrote labels to {signals_file_path=}")
