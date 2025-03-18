from zipline_polygon_bundle import register_polygon_equities_bundle
from exchange_calendars.calendar_helpers import parse_date
from zipline.utils.calendar_utils import get_calendar

calendar = get_calendar("XNYS")

# By default the bundle will ingest all the data in the POLYGON_DATA_DIR/flatfiles aggregates.
# If you want to limit the ingest to a subset of those days, you can set start_session and end_session.
# Be sure to set POLYGON_API_KEY and POLYGON_DATA_DIR in your environment before running this.
# See the README.md for info on using `rclone` to sync data from S3 to your local machine.
register_polygon_equities_bundle(
    "polygon",
    start_date=parse_date("2016-01-05", calendar=calendar),
    end_date=parse_date("2024-12-31", calendar=calendar),
    calendar_name=calendar.name,
    agg_time="day"
)

register_polygon_equities_bundle(
    "polygon-minute",
    start_date=parse_date("2016-01-05", calendar=calendar),
    end_date=parse_date("2024-12-31", calendar=calendar),
    calendar_name=calendar.name,
    agg_time="minute"
)
