import os


def get_data_dir():
    return os.environ.get("POLYGON_DATA_DIR", "data/polygon")


def get_asset_subdir():
    return os.environ.get("POLYGON_ASSET_SUBDIR", "us_stocks_sip")


def get_tickers_dir():
    return os.environ.get(
        "POLYGON_TICKERS_DIR",
        os.path.join(os.path.join(get_data_dir(), "tickers"), get_asset_subdir()),
    )


def get_tickers_csv_path(start_date, end_date):
    return os.environ.get('POLYGON_TICKERS_CSV', os.path.join(
        get_tickers_dir(),
        f"tickers_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.csv",
    ))


def get_flat_files_dir():
    return os.environ.get(
        "POLYGON_FLAT_FILES_DIR", os.path.join(get_data_dir(), "flatfiles")
    )


def get_asset_files_dir():
    return os.path.join(get_flat_files_dir(), get_asset_subdir())


def get_minute_aggs_dir():
    return os.path.join(get_asset_files_dir(), "minute_aggs_v1")
