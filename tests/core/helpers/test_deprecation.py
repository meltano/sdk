from __future__ import annotations

import warnings

from singer_sdk.helpers._deprecation import deprecate_row_param


class Deprecated:
    @deprecate_row_param
    def check_row(self, record):
        pass


class StaleSubclass(Deprecated):
    def check_row(self, row):
        return super().check_row(row=row)


class OkSubclass(Deprecated):
    def check_row(self, row):
        return super().check_row(row)


class NewSubclass(Deprecated):
    def check_row(self, record):
        return super().check_row(record)


def test_deprecated_row_parameter():
    d = Deprecated()

    # No warning should be raised
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        d.check_row("test")

    # No warning should be raised
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        d.check_row(record="test")

    # Warning should be raised
    _assert_deprecation_warning(d)

    s = StaleSubclass()
    _assert_deprecation_warning(s)

    o = OkSubclass()
    # No warning should be raised
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        o.check_row(row="test")

    n = NewSubclass()
    # No warning should be raised
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        n.check_row(record="test")


def _assert_deprecation_warning(instance: Deprecated):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        instance.check_row(row="test")
        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)
        assert str(w[-1].message) == (
            "The 'row' parameter for 'Deprecated.check_row' is deprecated. "
            "Use 'record' instead."
        )
