from zipline_polygon_bundle import register_nyse_all_hours_calendar, NYSE_ALL_HOURS
from exchange_calendars.calendar_helpers import parse_date

from zipline_polygon_bundle.config import PolygonConfig

import os

register_nyse_all_hours_calendar()

start_date = parse_date("2020-01-03", raise_oob=False)
end_date = parse_date("2021-01-29", raise_oob=False)

config = PolygonConfig(
    environ=os.environ,
    calendar_name=NYSE_ALL_HOURS,
    start_date=start_date,
    end_date=end_date,
    agg_time="1min",
)

calendar = config.calendar

print(f"{calendar.name=} {start_date=} {end_date=} {calendar.tz=} {calendar.tz.key=}")
print(f"{calendar.first_session=} {calendar.last_session=}")
print(f"{calendar.date_to_session(end_date, direction='previous')=}")
print(f"{calendar.session_last_minute(end_date)=}")
print(f"{calendar.sessions_in_range(start_date, end_date)[-4:]=}")
print(f"{calendar.sessions_minutes(start_date, end_date)[-4:]=}")
