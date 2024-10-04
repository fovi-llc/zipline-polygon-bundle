# zipline-polygon-bundle
`zipline-polygon-bundle` is a `zipline-reloaded` (https://github.com/stefan-jansen/zipline-reloaded) data ingestion bundle for [Polygon.io](https://polygon.io/).

## GitHub
https://github.com/fovi-llc/zipline-polygon-bundle

## Resources

Get a subscription to https://polygon.io/ for an API key and access to flat files.

https://polygon.io/knowledge-base/article/how-to-get-started-with-s3

Quantopian's Zipline backtester revived by Stefan Jansen: https://github.com/stefan-jansen/zipline-reloaded

Stefan's excellent book *Machine Learning for Algorithmic Trading*: https://ml4trading.io/

*Trading Evolved* by Andreas Clenow is a gentler introduction to Zipline Reloaded: https://www.followingthetrend.com/trading-evolved/

Code from *Trading Evolved* with some small updates for convenience: https://github.com/fovi-llc/trading_evolved

One of the modifications I've made to that code is so that some of the notebooks can be run on Colab with a minimum of fuss: https://github.com/fovi-llc/trading_evolved/blob/main/Chapter%207%20-%20Backtesting%20Trading%20Strategies/First%20Zipline%20Backtest.ipynb

# Ingest data from Polygon.io into Zipline

## Set up your `rclone` (https://rclone.org/) configuration
```bash
export POLYGON_FILE_ENDPOINT=https://files.polygon.io/
rclone config create s3polygon s3 env_auth=false endpoint=$POLYGON_FILE_ENDPOINT \
  access_key_id=$POLYGON_S3_Access_ID secret_access_key=$POLYGON_Secret_Access_Key
```

## Get flat files (`*.csv.gz`) for US Stock daily aggregates.
The default asset dir is `us_stock_sip` but that can be overriden with the `POLYGON_ASSET_SUBDIR` 
environment variable if/when Polygon.io adds other markets to flat files.

```bash
export POLYGON_DATA_DIR=`pwd`/data/files.polygon.io
for year in 2024 2023 2022 2021; do \
    rclone copy -P s3polygon:flatfiles/us_stocks_sip/day_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/day_aggs_v1/$year; \
done
```

## `extension.py`

```python
from zipline_polygon_bundle import register_polygon_equities_bundle

# All tickers (>20K) are ingested.  Filtering is TBD.
# `start_session` and `end_session` can be set to ingest a range of dates (which must be market days).
register_polygon_equities_bundle(
    "polygon",
    calendar_name="XNYS",
    agg_time="day"
)
```

## Install the Zipline Polygon.io Bundle PyPi package and check that it works.
Listing bundles will show if everything is working correctly.
```bash
pip install zipline_polygon_bundle
zipline -e extension.py bundles
```
stdout:
```
csvdir <no ingestions>
polygon <no ingestions>
polygon-minute <no ingestions>
quandl <no ingestions>
quantopian-quandl <no ingestions>
```

## Ingest the Polygon.io data.  The API key is needed for the split and dividend data.

Note that ingest currently stores cached API data and shuffled agg data in the `POLYGON_DATA_DIR` directory (`flatfiles/us_stocks_sip/api_cache` and `flatfiles/us_stocks_sip/day_by_ticker_v1` respectively) so write access is needed at this stage.  After ingestion the data in `POLYGON_DATA_DIR` is not accessed.

```bash
export POLYGON_API_KEY=<your API key here>
zipline -e extension.py ingest -b polygon
```

### Cleaning up bad ingests
After a while you may wind up with old (or empty because of an error during ingestion) bundles cluttering
up the list and could waste space (although old bundles may be useful for rerunning old backtests).
To remove all but the last ingest (say after your first successful ingest after a number of false starts) you could use:
```bash
zipline -e extension.py clean -b polygon --keep-last 1
```

## Using minute aggregate flat files.
Minute aggs work too but everything takes more space and a lot longer to do.  

```bash
export POLYGON_DATA_DIR=`pwd`/data/files.polygon.io
for year in 2024 2023 2022 2021; do \
    rclone copy -P s3polygon:flatfiles/us_stocks_sip/minute_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/minute_aggs_v1/$year; \
done
```

If you set the `ZIPLINE_ROOT` environment variable (recommended and likely necessary because the default of `~/.zipline` is probably not what you'll want) and copy your `extension.py` config there then you don't need to put `-e extension.py` on the `zipline` command line.

This ingestion for 10 years of minute bars took around 10 hours on my Mac using an external hard drive (not SSD).  A big chunk of that was copying from the default tmp dir to the Zipline root (6.3million files for 47GB actual, 63GB used).  I plan to change that `shutil.copy2` to use `shutil.move` and to use a `tmp` dir in Zipline root for temporary files instead of the default which should save an hour or two.  Also the ingestion process is single threaded and could be sped up with some concurrency.

```bash
zipline ingest -b polygon-minute
```

# License is Affero General Public License v3 (AGPL v3)
The content of this project is Copyright (C) 2024 Fovi LLC and authored by James P. White (https://www.linkedin.com/in/jamespaulwhite/).  It is distributed under the terms of the GNU AFFERO GENERAL PUBLIC LICENSE (AGPL) Version 3 (See LICENSE file).

The AGPL doesn't put any restrictions on personal use but people using this in a service for others have obligations.  If you have commerical purposes and those distribution requirements don't work for you, feel free to contact me (mailto:jim@fovi.com) about other licensing terms.
