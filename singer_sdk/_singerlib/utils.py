import datetime

import dateutil.parser
import pytz

DATETIME_FMT = "%04Y-%m-%dT%H:%M:%S.%fZ"
DATETIME_FMT_SAFE = "%Y-%m-%dT%H:%M:%S.%fZ"


def strptime_to_utc(dtimestr: str) -> datetime:
    """Parses a provide datetime string into a UTC datetime object.

    Args:
        dtimestr: a string representation of a datetime

    Returns:
        A UTC datetime object
    """
    d_object = dateutil.parser.parse(dtimestr)
    if d_object.tzinfo is None:
        return d_object.replace(tzinfo=pytz.UTC)
    else:
        return d_object.astimezone(tz=pytz.UTC)


def strftime(dtime: datetime, format_str: str = DATETIME_FMT) -> str:
    """Formats a provided datetime object as a string.

    Args:
        dtime: a datetime
        format_str: output format specification

    Returns:
        A string in the specified format

    Raises:
        Exception: if the datetime is not UTC (if it has a nonzero time zone offset)
    """
    if dtime.utcoffset() != datetime.timedelta(0):
        raise Exception("datetime must be pegged at UTC tzoneinfo")

    try:
        dt_str = dtime.strftime(format_str)
        if dt_str.startswith("4Y"):
            return dtime.strftime(DATETIME_FMT_SAFE)
    except ValueError:
        return dtime.strftime(DATETIME_FMT_SAFE)
