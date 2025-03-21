import datetime
from exchange_calendars.calendar_utils import get_calendar_names, register_calendar_type
from exchange_calendars.exchange_calendar_xnys import XNYSExchangeCalendar


NYSE_ALL_HOURS = "NYSE_ALL_HOURS"


class USExtendedHoursExchangeCalendar(XNYSExchangeCalendar):
    """
    A calendar for extended hours which runs from 4 AM to 8 PM.
    """

    name = NYSE_ALL_HOURS

    open_times = ((None, datetime.time(4)),)

    close_times = ((None, datetime.time(20)),)

    regular_early_close = datetime.time(13)


def register_nyse_all_hours_calendar():
    if NYSE_ALL_HOURS not in get_calendar_names():
        register_calendar_type(NYSE_ALL_HOURS, USExtendedHoursExchangeCalendar)
