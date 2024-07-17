from __future__ import annotations  # noqa: INP001

import pytest

from singer_sdk._singerlib.encoding._msgspec import dec_hook, enc_hook


@pytest.mark.parametrize(
    "test_type,test_value,expected_value,expected_type",
    [
        pytest.param(
            int,
            1,
            "1",
            str,
            id="int-to-str",
        ),
    ],
)
def test_dec_hook(test_type, test_value, expected_value, expected_type):
    returned = dec_hook(type=test_type, obj=test_value)
    returned_type = type(returned)

    assert returned == expected_value
    assert returned_type == expected_type


@pytest.mark.parametrize(
    "test_value,expected_value",
    [
        pytest.param(
            1,
            "1",
            id="int-to-str",
        ),
    ],
)
def test_enc_hook(test_value, expected_value):
    returned = enc_hook(obj=test_value)

    assert returned == expected_value
