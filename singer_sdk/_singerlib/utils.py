"""Deprecated."""

from __future__ import annotations

import warnings

from singer_sdk.helpers._compat import SingerSDKDeprecationWarning


def __getattr__(name: str):  # noqa: ANN202
    import singer_sdk.singerlib.utils  # noqa: PLC0415

    warnings.warn(
        "The module `singer_sdk._singerlib.utils` is deprecated and will be removed "
        "by August 2025. "
        "Please use `singer_sdk.singerlib.utils` instead.",
        SingerSDKDeprecationWarning,
        stacklevel=2,
    )

    return getattr(singer_sdk.singerlib.utils, name)
