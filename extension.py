from zipline_polygon_bundle import register_polygon_equities_bundle
from exchange_calendars.calendar_helpers import parse_date
from zipline.utils.calendar_utils import get_calendar

calendar = get_calendar("XNYS")

# This start and end dates need to be days when the market was open per the calendar.
# If you don't provide these dates, the bundle will use the first and last dates of the calendar.
# Be sure to set POLYGON_API_KEY and POLYGON_DATA_DIR in your environment before running this.
# See the README.md for info on using `rclone` to sync data from S3 to your local machine.
register_polygon_equities_bundle(
    "polygon",
    # start_session=parse_date("2016-01-05", calendar=calendar),
    # end_session=parse_date("2024-09-13", calendar=calendar),
    calendar_name=calendar.name,
    agg_time="day"
)

register_polygon_equities_bundle(
    "polygon-minute",
    # start_session=parse_date("2022-01-03", calendar=calendar),
    # end_session=parse_date("2022-12-30", calendar=calendar),
    calendar_name=calendar.name,
    agg_time="minute"
)
