import os


def get_data_dir():
    return os.environ.get("POLYGON_DATA_DIR", "data/polygon")


def get_flat_files_dir():
    return os.environ.get(
        "POLYGON_FLAT_FILES_DIR", os.path.join(get_data_dir(), "flatfiles")
    )


def get_asset_files_dir():
    return os.path.join(
        get_flat_files_dir(), os.environ.get("POLYGON_ASSET_SUBDIR", "us_stocks_sip")
    )
