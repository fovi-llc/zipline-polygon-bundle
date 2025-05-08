# zipline-polygon-bundle
`zipline-polygon-bundle` is a `zipline-arrow` (https://github.com/fovi-llc/zipline-arrow) data ingestion bundle for [Polygon.io](https://polygon.io/).

Zipline Arrow is a fork of Zipline Reloaded `zipline-reloaded` (https://github.com/stefan-jansen/zipline-reloaded) which is only required if you want to use Polygon.io trades flatfiles.  So if you only need to use Polygon daily or minute agg flatfiles then you may want to use `zipline-polygon-bundle<0.2` which depends on `zipline-reloaded>=3.1`.

## GitHub
https://github.com/fovi-llc/zipline-polygon-bundle

## PyPi

https://pypi.org/project/zipline_polygon_bundle

## Resources

Get a subscription to https://polygon.io/ for an API key and access to flat files.

https://polygon.io/knowledge-base/article/how-to-get-started-with-s3

Quantopian's Zipline backtester revived by Stefan Jansen: https://github.com/stefan-jansen/zipline-reloaded

Stefan's excellent book *Machine Learning for Algorithmic Trading*: https://ml4trading.io/

*Trading Evolved* by Andreas Clenow is a gentler introduction to Zipline Reloaded: https://www.followingthetrend.com/trading-evolved/

Code from *Trading Evolved* with some small updates for convenience: https://github.com/fovi-llc/trading_evolved

One of the modifications I've made to that code is so that some of the notebooks can be run on Colab with a minimum of fuss: https://github.com/fovi-llc/trading_evolved/blob/main/Chapter%207%20-%20Backtesting%20Trading%20Strategies/First%20Zipline%20Backtest.ipynb

# Zipline Reloaded (`zipline-reloaded`) or Zipline Arrow (`zipline-arrow`)?

This bundle supports Polygon daily and minute aggregates and now trades too (quotes coming).  The trades are converted to minute and daily aggregates for all trading hours (extended both pre and post, as well as regular market).  But in order to support those extended hours I needed to change how Zipline handles `get_calendar` for Exchange Calendar (`exchange-calendar`) initialization.  To make that work I've forked `zipline-reloaded` as `zipline-arrow`.  The versions of this package before 0.2 depend on `zipline-reloaded>=3.1` and only support daily and minute flatfiles.  Versions >= 0.2 of `zipline-polygon-bundle` depend on `zipline-arrow` and will work with daily and minute flatfiles as well as trades flatfiles.

# Ingest data from Polygon.io into Zipline using `aws s3` CLI
Get AWS S3 CLI in the usual way: https://docs.aws.amazon.com/cli/latest/reference/s3/

This will get everything which is currently around 12TB.
```bash
aws s3 sync s3://flatfiles/us_stocks_sip $POLYGON_DATA_DIR/flatfiles/us_stocks_sip --checksum-mode ENABLED  --endpoint-url https://files.polygon.io
```

If you don't need quotes yet (and this bundle doesn't use them yet) then this will be faster (quotes about twice as big as trades):
```bash
aws s3 sync s3://flatfiles/us_stocks_sip/{subdir} $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/{subdir} --checksum-mode ENABLED  --endpoint-url https://files.polygon.io
```

# Alternative: Ingest data using `rclone`.
I've had problems with `rclone` on the larger files for trades and quotes so I recommend using `aws s3` CLI instead.

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

## Cython build setup

```bash
sudo apt-get update
sudo apt-get install python3-dev python3-poetry gcc-multilib

CFLAGS=$(python3-config --includes) pip install git+https://github.com/fovi-llc/zipline-arrow.git
```


## Install the Zipline Polygon.io Bundle PyPi package and check that it works.
Listing bundles will show if everything is working correctly.
```bash

pip install -U git+https://github.com/fovi-llc/zipline-reloaded.git@calendar
pip install -U git+https://github.com/fovi-llc/zipline-polygon-bundle.git

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

Note that ingest currently stores cached API data and shuffled agg ("by ticker") data in the `$CUSTOM_ASSET_FILES_DIR` directory which is `$ZIPLINE_ROOT/data/polygon_custom_assets` by default.

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

## Using trades flat files.
This takes a lot of space for the trades flatfiles (currently the 22 years of trades take around 4TB) and a fair bit of time to convert to minute aggregates.  The benefit though is the whole trading day is covered from premarket open to after hours close.  Also the current conversion logic ignores trade corrections, official close updates, and the TRF "dark pool" trades (because they are not reported when they occurred nor were they offered on the exchanges).  That is to make the aggregates be as good of a simulation of real-time as we can do for algo training and backtesting.  Details in the `trades_to_custom_aggs` function in `zipline_polygon_bundle/trades.py`.

The conversion process creates `.csv.gz` files in the same format as Polygon flatfiles in the custom assets dir, which is `$ZIPLINE_ROOT/data/polygon_custom_assets` by default.  So while `$ZIPLINE_ROOT` needs to be writable, the Polygon flatfiles (`$POLYGON_DATA_DIR`) can be read-only.

Get AWS S3 CLI in the usual way: https://docs.aws.amazon.com/cli/latest/reference/s3/

```bash
aws s3 sync s3://flatfiles/us_stocks_sip/trades_v1 $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1 --checksum-mode ENABLED  --endpoint-url https://files.polygon.io
```

## `extension.py`

If you set the `ZIPLINE_ROOT` environment variable (recommended and likely necessary because the default of `~/.zipline` is probably not what you'll want) and copy your `extension.py` config there then you don't need to put `-e extension.py` on the `zipline` command line.

If you leave out the `start_date` and/or `end_date` args then `register_polygon_equities_bundle` will scan for the dates of the first and last trade file in `$POLYGON_DATA_DIR` and use them respectively.

The `NYSE_ALL_HOURS` calendar (defined in `zipline_polygon_bundle/nyse_all_hours_calendar.py`) uses open and close times for the entire trading day from premarket open to after hours close.

Right now `agg_time="1min"` is the only supported aggregate duration because Zipline can only deal with day or minute duration aggregates.

```python
from zipline_polygon_bundle import register_polygon_equities_bundle, register_nyse_all_hours_calendar, NYSE_ALL_HOURS
from exchange_calendars.calendar_helpers import parse_date
# from zipline.utils.calendar_utils import get_calendar

# Register the NYSE_ALL_HOURS ExchangeCalendar.
register_nyse_all_hours_calendar()

register_polygon_equities_bundle(
    "polygon-trades",
    calendar_name=NYSE_ALL_HOURS,
    # start_date=parse_date("2020-01-03", raise_oob=False),
    # end_date=parse_date("2021-01-29", raise_oob=False),
    agg_time="1min",
    minutes_per_day=16 * 60,
)
```

As with the daily and minute aggs, the POLYGON_API_KEY is needed for the split and dividend data.  Also coming is SID assignment across ticker changes using the Polygon tickers API data.

```bash
zipline ingest -b polygon-trades
```

# License is Affero General Public License v3 (AGPL v3)
The content of this project is Copyright (C) 2024 Fovi LLC and authored by James P. White (https://www.linkedin.com/in/jamespaulwhite/).  It is distributed under the terms of the GNU AFFERO GENERAL PUBLIC LICENSE (AGPL) Version 3 (See LICENSE file).

The AGPL doesn't put any restrictions on personal use but people using this in a service for others have obligations.  If you have commerical purposes and those distribution requirements don't work for you, feel free to contact me (mailto:jim@fovi.com) about other licensing terms.
