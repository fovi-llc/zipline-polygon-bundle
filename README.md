# zipline_bundle_polygon
A zipline-reloaded (https://github.com/stefan-jansen/zipline-reloaded) data ingestion bundle for [Polygon.io](https://polygon.io/).

# Ingest data from Polygon into Zipline

## Resources

Get a subscription to https://polygon.io/ for an API key and access to flat files.

https://polygon.io/knowledge-base/article/how-to-get-started-with-s3

## Set up your rclone (https://rclone.org/) configuration
```bash
export POLYGON_FILE_ENDPOINT=https://files.polygon.io/
rclone config create s3polygon s3 env_auth=false access_key_id=$POLYGON_S3_Access_ID secret_access_key=$POLYGON_Secret_Access_Key endpoint=$POLYGON_FILE_ENDPOINT
```

## Get flat files (*.csv.gz) for US Stock daily aggregates.

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
```bash
pip install zipline_polygon_bundle
zipline -e extension.py bundles
```

## Ingest the Polygon.io data.  The API key is needed for the split and dividend data.
```bash
export POLYGON_API_KEY=<your API key here>
zipline -e extension.py ingest -b polygon
```

# License is Affero General Public License v3 (AGPL v3)
The content of this project is Copyright (C) 2024 Fovi LLC and authored by James P. White (https://www.linkedin.com/in/jamespaulwhite/).  It is distributed under the terms of the GNU AFFERO GENERAL PUBLIC LICENSE (AGPL) Version 3 (See LICENSE file).

The AGPL doesn't put any restrictions on personal use but people using this in a service for others have obligations.  If you have commerical purposes and those distribution requirements don't work for you, feel free to contact me (mailto:jim@fovi.com) about other licensing terms.
