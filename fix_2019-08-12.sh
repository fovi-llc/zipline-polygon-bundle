#!/bin/bash

# Define the file path
file_path=$POLYGON_DATA_DIR/flatfiles/us_stocks_sip/day_aggs_v1/2019/08/2019-08-12.csv.gz

# Define the incorrect and correct timestamps
incorrect_timestamp="1565668800000000000"
correct_timestamp="1565582400000000000"

# Save the bad old file
SAVE_DIR=$POLYGON_DATA_DIR/flatfiles/saved
mkdir -p $SAVE_DIR
bad_file=$SAVE_DIR/day_aggs_v1_2019-08-12.csv.gz

mv $file_path $bad_file

# Unzip, replace the incorrect timestamp, and zip back
gunzip -c "$bad_file" | sed "s/$incorrect_timestamp/$correct_timestamp/g" | gzip -c > "$file_path"

# aws s3 sync s3://flatfiles/us_stocks_sip/day_aggs_v1 \
#    $POLYGON_DATA_DIR/flatfiles/us_stocks_sip/day_aggs_v1 \
#    --checksum-mode ENABLED --endpoint-url https://files.polygon.io;
