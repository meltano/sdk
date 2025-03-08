from __future__ import annotations

from datetime import datetime, timezone

import pytest
import pytz

from singer_sdk._singerlib import strftime, strptime_to_utc
from singer_sdk._singerlib.utils import NonUTCDatetimeError


def test_small_years():
    assert (
        strftime(datetime(90, 1, 1, tzinfo=pytz.UTC)) == "0090-01-01T00:00:00.000000Z"
    )


def test_round_trip():
    now = datetime.now(tz=pytz.UTC)
    dtime = strftime(now)
    parsed_datetime = strptime_to_utc(dtime)
    formatted_datetime = strftime(parsed_datetime)
    assert dtime == formatted_datetime


@pytest.mark.parametrize(
    "dtimestr",
    [
        "2021-01-01T00:00:00.000000Z",
        "2021-01-01T00:00:00.000000+00:00",
        "2021-01-01T00:00:00.000000+06:00",
        "2021-01-01T00:00:00.000000-04:00",
        "2021-01-01T00:00:00.000000",
    ],
    ids=["Z", "offset+0", "offset+6", "offset-4", "no offset"],
)
def test_strptime_to_utc(dtimestr):
    assert strptime_to_utc(dtimestr).tzinfo == timezone.utc


def test_stftime_non_utc():
    now = datetime.now(tz=pytz.timezone("America/New_York"))
    with pytest.raises(NonUTCDatetimeError):
        strftime(now)
