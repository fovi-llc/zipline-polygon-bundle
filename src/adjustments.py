import os
import pandas as pd
import polygon
from typing import Iterator


def load_splits(tickers):
    polygon_client = polygon.RESTClient(api_key=os.environ.get("POLYGON_API_KEY"))
    splits_df = pd.DataFrame()
    # Iterate over the list tickers in slices of 500 each
    tickers = sorted(tickers)
    k = 400
    slices = [tickers[i : i + k] for i in range(0, len(tickers), k)]
    for slice in slices:
        response = polygon_client.list_splits(
            ticker_gte=slice[0],
            ticker_lte=slice[-1],
            sort="ticker",
            order="asc",
            limit=1000,
        )
        if isinstance(response, Iterator):
            splits_df = pd.concat([splits_df, pd.DataFrame(list(response))])
        else:
            print(
                f"Unexpected response getting splits for {slice[0]}..{slice[-1]} from Polygon.  Response: {response}"
            )
    # Index by ticker and date and group by ticker
    splits_df = splits_df.set_index(["ticker", "execution_date"]).sort_index()
    # Remove duplicates
    splits_df = splits_df[~splits_df.index.duplicated(keep="first")]
    return splits_df
