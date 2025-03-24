from zipline_polygon_bundle import register_polygon_equities_bundle, register_nyse_all_hours_calendar, NYSE_ALL_HOURS
from exchange_calendars.calendar_helpers import parse_date
# from zipline.utils.calendar_utils import get_calendar

# Register the NYSE_ALL_HOURS ExchangeCalendar.
register_nyse_all_hours_calendar()

# By default the bundle will ingest all the data in the POLYGON_DATA_DIR/flatfiles aggregates.
# If you want to limit the ingest to a subset of those days, you can set start_session and end_session.
# Be sure to set POLYGON_API_KEY and POLYGON_DATA_DIR in your environment before running this.
# See the README.md for info on using `rclone` to sync data from S3 to your local machine.
register_polygon_equities_bundle(
    "polygon",
    # start_session=parse_date("2016-01-05", raise_oob=False),
    # end_session=parse_date("2024-09-13", raise_oob=False),
    agg_time="day"
)

register_polygon_equities_bundle(
    "polygon-minute",
    # start_date=parse_date("2020-01-03", raise_oob=False),
    # end_date=parse_date("2021-01-29", raise_oob=False),
    agg_time="minute"
)

register_polygon_equities_bundle(
    "polygon-trades",
    calendar_name=NYSE_ALL_HOURS,
    # start_date=parse_date("2020-01-03", raise_oob=False),
    # end_date=parse_date("2021-01-29", raise_oob=False),
    agg_time="1min",
    minutes_per_day=16 * 60,
)
