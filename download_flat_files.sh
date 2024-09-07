# Install rclone
# brew install rclone

export POLYGON_FILE_ENDPOINT=https://files.polygon.io/
export POLYGON_DATA_DIR=/Volumes/Oahu/Mirror/files.polygon.io

# Set up your rclone configuration
rclone config create s3polygon s3 env_auth=false access_key_id=$POLYGON_S3_Access_ID secret_access_key=$POLYGON_Secret_Access_Key endpoint=$POLYGON_FILE_ENDPOINT

# List
rclone ls s3polygon:flatfiles

# Copy

mkdir -p $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/2020/01
rclone copy -P --transfers 8 s3polygon:flatfiles/us_stocks_sip/trades_v1/2020/01 $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/2020/01

for year in 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016; do \
    rclone copy -P s3polygon:flatfiles/us_stocks_sip/trades_v1/$year $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/$year ; \
done

POLYGON_YEAR=2023
rclone copy -P s3polygon:flatfiles/us_stocks_sip/trades_v1/$POLYGON_YEAR $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/$POLYGON_YEAR


find $POLYGON_DATA_DIR/flatfiles -type f -name "*.partial"
find $POLYGON_DATA_DIR/flatfiles -type f -name "*.partial" -exec rm -f {} \;


# New external location

export POLYGON_DATA_DIR=/Volumes/Oahu/Mirror/files.polygon.io

mkdir -p $POLYGON_DATA_DIR
cp -r /Users/jim/Projects/zipline-polygon-bundle/data/polygon/flatfiles $POLYGON_DATA_DIR

mc cp --recursive s3polygon/flatfiles/us_stocks_sip/minute_aggs_v1/2016 \
    s3polygon/flatfiles/us_stocks_sip/minute_aggs_v1/2017 \
    s3polygon/flatfiles/us_stocks_sip/minute_aggs_v1/2018 \
    s3polygon/flatfiles/us_stocks_sip/minute_aggs_v1/2019 \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/minute_aggs_v1

for year in 2016 2017 2018 2019 2020 2021 2022 2023 2024; do \
    mc mirror s3polygon/flatfiles/us_stocks_sip/day_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/day_aggs_v1/$year; \
    mc mirror s3polygon/flatfiles/us_stocks_sip/minute_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/minute_aggs_v1/$year; \
done



for year in 2016 2017 2018 2019 2020 2021 2022 2023 2024; do \
    mc mirror s3polygon/flatfiles/us_stocks_sip/trades_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/$year; \
done

mc mirror s3polygon/flatfiles/us_stocks_sip/trades_v1/2020 \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/2020;


