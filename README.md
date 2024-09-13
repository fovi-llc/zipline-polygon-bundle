# zipline_bundle_polygon
A zipline-reloaded (https://github.com/stefan-jansen/zipline-reloaded) data provider bundle for [Polygon.io](https://polygon.io/).

# License is Affero General Public License v3 (AGPL v3)
The AGPL doesn't put any restrictions on personal use but people using this in a service for others have obligations.  If you have commerical purposes and the distribution requirements don't work for you, feel free to contact [me](https://www.linkedin.com/in/jamespaulwhite/) about other licensing terms.

# Ingest data from Polygon into Zipline

# TODO: complete example commands.  These are missing the configuration that needs to done.

```bash
git clone https://github.com/fovi-llc/zipline_polygon_bundle.git
cd zipline_polygon_bundle
pip install -e .
# Download Polygon flatfiles.  See download_flat_files.sh for how.
# Concatenate daily CSV files into Arrow Hive dataset.
python -m zipline_polygon_bundle concat_all_aggs.py
# Edit the extension.py to set your start/end session and calendar.
# Install the extension:
# cp extension.py ~/.zipline
# zipline ingest -b polygon
# or just use it from command (you don't need it except to ingest):
zipline -e extension.py ingest -b polygon
```

