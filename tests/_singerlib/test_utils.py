from datetime import datetime

import pytest
import pytz

from singer_sdk._singerlib import strftime, strptime_to_utc


def test_small_years():
    assert (
        strftime(datetime(90, 1, 1, tzinfo=pytz.UTC)) == "0090-01-01T00:00:00.000000Z"
    )


def test_round_trip():
    now = datetime.utcnow().replace(tzinfo=pytz.UTC)
    dtime = strftime(now)
    parsed_datetime = strptime_to_utc(dtime)
    formatted_datetime = strftime(parsed_datetime)
    assert dtime == formatted_datetime
