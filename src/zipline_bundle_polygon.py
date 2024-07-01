from zipline.data.bundles import register
import pandas as pd
from os import listdir
from exchange_calendars import get_calendar


# Change the path to where you have your data
path = "/Users/jim/Projects/sabirjana-quant-blog/RSI/data/data"


def polygon_equities_bundle(
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
):

    # Get list of files from path
    # Slicing off the last part
    # 'example.csv'[:-4] = 'example'
    symbols = [f[:-4] for f in listdir(path)]

    if not symbols:
        raise ValueError("No symbols found in folder.")

    # Prepare an empty DataFrame for dividends
    divs = pd.DataFrame(
        columns=["sid", "amount", "ex_date", "record_date", "declared_date", "pay_date"]
    )

    # Prepare an empty DataFrame for splits
    splits = pd.DataFrame(columns=["sid", "ratio", "effective_date"])

    # Prepare an empty DataFrame for metadata
    metadata = pd.DataFrame(
        columns=("start_date", "end_date", "auto_close_date", "symbol", "exchange")
    )

    # Check valid trading dates, according to the selected exchange calendar
    sessions = calendar.sessions_in_range(start_session, end_session)
    # sessions = calendar.sessions_in_range('1995-05-02', '2020-05-27')

    # # Get data for all stocks and write to Zipline
    # daily_bar_writer.write(process_stocks(symbols, sessions, metadata, divs))

    # # Write the metadata
    # asset_db_writer.write(equities=metadata)

    # # Write splits and dividends
    # adjustment_writer.write(splits=splits, dividends=divs)


def register_polygon_equities_bundle(
    bundlename,
    start_session="2000-01-01",
    end_session="now",
    calendar_name="NYSE",
    symbol_list=None,
    watchlists=None,
    excluded_symbol_list=None,
):
    register(
        bundlename,
        polygon_equities_bundle,
        start_session=start_session,
        end_session=end_session,
        calendar_name=calendar_name,
    )
