# Migrate your REST tap to use the new pagination classes!

This guide will walk you through the process of migrating your REST tap to use the new pagination classes.

## What's changed?

In a future release of the SDK, the `RESTStream.get_next_page_token` method will no longer be
called as part of the stream pagination process. It is replaced by the
[`RESTStream.get_new_paginator`](singer_sdk.RESTStream.get_new_paginator).

In the meantime, the `get_next_page_token` method will still be called if you define it
in your stream class, but a deprecation warning will be logged, if
[warnings are enabled](https://docs.python.org/3/using/cmdline.html#envvar-PYTHONWARNINGS).

```{note}
If you have not overridden the `get_next_page_token` method in your stream class, you do not need
to make any changes.
```

## Why is this change being made?

The new pagination classes are designed to be more flexible, easier to understand, and easier to test. They also align better with the [Single Responsibility Principle](https://en.wikipedia.org/wiki/Single-responsibility_principle).

## How to migrate

### Example: HATEOAS pagination, a.k.a. "next" links

A common pattern in REST APIs is to use a `next` field in the response to indicate the
URL of the next page of results. The [`BaseHATEOASPaginator`](../../classes/singer_sdk.pagination.BaseHATEOASPaginator)
class can be used to handle this pattern.

```python
# Original implementation
from urllib.parse import parse_qsl

class MyStream(RESTStream):
    def get_next_page_token(self, response, previous_token):
        data = response.json()
        return data.get("next")

    def get_url_params(self, context, next_page_token):
        params = {}

        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        return params
```

```python
# New implementation

from singer_sdk.pagination import BaseHATEOASPaginator

class MyPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        data = response.json()
        return data.get("next")


class MyStream(RESTStream):
    def get_new_paginator(self):
        return MyPaginator()

    def get_url_params(self, context, next_page_token):
        params = {}

        # Next page token is a URL, so we can to parse it to extract the query string
        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        return params
```

### Example: Offset pagination

Another common pattern is to use an `offset` parameter to indicate the starting point of the next
page of results. The [`BaseOffsetPaginator`](../../classes/singer_sdk.pagination.BaseOffsetPaginator)
class can be used to handle this pattern.

```python
# New implementation

from singer_sdk.pagination import BaseOffsetPaginator


class MyStream(RESTStream):
    def get_new_paginator(self):
        return BaseOffsetPaginator(start_value=0, page_size=250)

    def get_url_params(self, context, next_page_token):
        params = {}

        # Next page token is an offset
        if next_page_token:
            params["offset"] = next_page_token

        return params
```
