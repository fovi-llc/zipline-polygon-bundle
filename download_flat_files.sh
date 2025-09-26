# Using rclone to download Polygon flat files
# I tried using Minio Client (mc) but it had problems with some of the files.

# Install rclone
# brew install rclone

export MIRROR_DIR=/Volumes/Oahu/Mirror
export MIRROR_DIR=/media/nas20t1/Mirror

export POLYGON_FILE_ENDPOINT=https://files.polygon.io/
export POLYGON_DATA_DIR=$MIRROR_DIR/files.polygon.io


# Using AWS CLI because rclone fails with big files (>1GB)

aws configure set aws_access_key_id $POLYGON_S3_Access_ID
aws configure set aws_secret_access_key $POLYGON_Secret_Access_Key

aws configure set default.s3.max_bandwidth 50MB/s

aws s3 ls s3://flatfiles/us_stocks_sip/ --endpoint-url https://files.polygon.io


aws s3 cp s3://flatfiles/us_stocks_sip/quotes_v1/2023 $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/quotes_v1/2023 \
    --recursive --endpoint-url https://files.polygon.io

rclone copy -P --transfers 8 s3polygon:flatfiles/us_stocks_sip/trades_v1/2020/01 $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/2020/01



# Set up your rclone configuration
rclone config create s3polygon s3 env_auth=false access_key_id=$POLYGON_S3_Access_ID secret_access_key=$POLYGON_Secret_Access_Key endpoint=$POLYGON_FILE_ENDPOINT

# List dirs https://rclone.org/commands/rclone_lsd/ (ls is recursive by default)
rclone lsd s3polygon:flatfiles

# Total size
rclone ls s3polygon:flatfiles | awk '{sum += $1} END {print sum}' - 
# 71397095379572 71,397,095,379,572
rclone ls s3polygon:flatfiles/us_stocks_sip | awk '{sum += $1} END {print sum}' -
# 12738193813682 12,738,193,813,682 12.7 TB

rclone ls s3polygon:flatfiles/us_stocks_sip | awk '{sum += $1; count++} END {print "Total size:", sum/1000000000, "GB File count:", count}'

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


for year in 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024; do \
    rclone copy -P s3polygon:flatfiles/us_stocks_sip/day_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/day_aggs_v1/$year; \
    rclone copy -P s3polygon:flatfiles/us_stocks_sip/minute_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/minute_aggs_v1/$year; \
done



for year in 2016 2017 2018 2019 2020 2021 2022 2023 2024; do \
    rclone copy -cP s3polygon:flatfiles/us_stocks_sip/trades_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/$year; \
done

for year in 2024 2023; do \
    rclone copy -cP s3polygon:flatfiles/us_stocks_sip/quotes_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/quotes_v1/$year; \
done

rclone copy --check-first --checksum --immutable -P \
    s3polygon:flatfiles/us_stocks_sip/trades_v1/$POLYGON_YEAR $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/$POLYGON_YEAR

# For large files need to specify --checksum.  Otherwise they will be copied every time.
rclone copy --check-first --checksum --progress s3polygon:flatfiles/us_stocks_sip/trades_v1/2020 \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/2020;


export ZIPLINE_ROOT=/Volumes/Oahu/Workspace/zipline

rclone copy -P s3polygon:flatfiles/us_options_opra/day_aggs_v1/2023 \
    $POLYGON_DATA_DIR/flatfiles/us_options_opra/day_aggs_v1/2023

for year in 2024 2023 2022 2021 2020; do \
    rclone copy -P s3polygon:flatfiles/us_options_opra/day_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_options_opra/day_aggs_v1/$year; \
done

for year in 2024 2023 2022 2021 2020; do \
    rclone copy -P s3polygon:flatfiles/us_options_opra/minute_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_options_opra/minute_aggs_v1/$year; \
done

rclone copy -P s3polygon:flatfiles/us_options_opra/trades_v1/2023 \
    $POLYGON_DATA_DIR/flatfiles/us_options_opra/trades_v1/2023

for year in 2024 2023 2022 2021 2020; do \
    rclone copy -P s3polygon:flatfiles/us_options_opra/trades_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_options_opra/trades_v1/$year; \
done


for year in 2024; do \
    rclone copy -P s3polygon:flatfiles/us_stocks_sip/day_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/day_aggs_v1/$year; \
    rclone copy -P s3polygon:flatfiles/us_stocks_sip/minute_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/minute_aggs_v1/$year; \
    rclone copy -P s3polygon:flatfiles/us_options_opra/day_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_options_opra/day_aggs_v1/$year; \
    rclone copy -P s3polygon:flatfiles/us_options_opra/minute_aggs_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_options_opra/minute_aggs_v1/$year; \
    rclone copy -cP s3polygon:flatfiles/us_stocks_sip/trades_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/$year; \
    rclone copy -P s3polygon:flatfiles/us_options_opra/trades_v1/$year \
    $POLYGON_DATA_DIR/flatfiles/us_options_opra/trades_v1/$year; \
done


for year in 2020; do \
    aws s3 sync s3://flatfiles/us_stocks_sip/day_aggs_v1/$year \
        $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/day_aggs_v1/$year \
        --checksum-mode ENABLED --endpoint-url https://files.polygon.io;
    aws s3 sync s3://flatfiles/us_stocks_sip/minute_aggs_v1/$year \
        $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/minute_aggs_v1/$year \
        --checksum-mode ENABLED --endpoint-url https://files.polygon.io;
    aws s3 sync s3://flatfiles/us_stocks_sip/trades_v1/$year \
        $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1/$year \
        --checksum-mode ENABLED --endpoint-url https://files.polygon.io; 
    aws s3 sync s3://flatfiles/us_stocks_sip/quotes_v1/$year \
        $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/quotes_v1/$year \
        --checksum-mode ENABLED --endpoint-url https://files.polygon.io; 
done


for year in 2020; do \
    aws s3 sync s3://flatfiles/us_stocks_sip/day_aggs_v1/$year \
        $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/day_aggs_v1/$year \
        --checksum-mode ENABLED --endpoint-url https://files.polygon.io;
done


aws s3 sync s3://flatfiles/us_stocks_sip/day_aggs_v1 \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/day_aggs_v1 \
    --checksum-mode ENABLED --endpoint-url https://files.polygon.io;
aws s3 sync s3://flatfiles/us_stocks_sip/minute_aggs_v1 \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/minute_aggs_v1 \
    --checksum-mode ENABLED --endpoint-url https://files.polygon.io;
aws s3 sync s3://flatfiles/us_stocks_sip/trades_v1 \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/trades_v1 \
    --checksum-mode ENABLED --endpoint-url https://files.polygon.io;
aws s3 sync s3://flatfiles/us_stocks_sip/quotes_v1 \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/quotes_v1 \
    --checksum-mode ENABLED --endpoint-url https://files.polygon.io;



aws s3 sync s3://flatfiles/us_stocks_sip/day_aggs_v1 \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/day_aggs_v1 \
    --checksum-mode ENABLED  --endpoint-url https://files.polygon.io;

aws s3 sync s3://flatfiles/us_stocks_sip \
    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip \
    --checksum-mode ENABLED  --endpoint-url https://files.polygon.io;


curl "https://api.polygon.io/v3/reference/conditions?asset_class=stocks&order=asc&limit=1000&sort=data_types&apiKey=$POLYGON_API_KEY" > data/polygon/conditions.json
curl "https://api.polygon.io/v3/reference/conditions?asset_class=stocks&data_type=trade&order=asc&limit=1000&sort=id&apiKey=$POLYGON_API_KEY" > data/polygon/conditions-trade.json
curl "https://api.polygon.io/v3/reference/conditions?asset_class=stocks&data_type=nbbo&order=asc&limit=1000&sort=id&apiKey=$POLYGON_API_KEY" > data/polygon/conditions-quote.json

curl "https://api.polygon.io/v3/reference/exchanges?asset_class=stocks&locale=us&apiKey=$POLYGON_API_KEY" > data/polygon/exchanges.json
