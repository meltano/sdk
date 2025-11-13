"""REST client handling, including {{ cookiecutter.source_name }}Stream base class."""

from __future__ import annotations

import decimal
import sys
{% if cookiecutter.auth_method in ("OAuth2", "JWT") -%}
from functools import cached_property
{% endif -%}
from typing import TYPE_CHECKING, Any, ClassVar

{% if cookiecutter.auth_method  == "API Key" -%}
from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

from {{ cookiecutter.library_name }} import schemas

{% elif cookiecutter.auth_method  == "Bearer Token" -%}
from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

from {{ cookiecutter.library_name }} import schemas

{% elif cookiecutter.auth_method == "Basic Auth" -%}
from requests.auth import HTTPBasicAuth
from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

from {{ cookiecutter.library_name }} import schemas

{% elif cookiecutter.auth_method == "Custom or N/A" -%}
from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

from {{ cookiecutter.library_name }} import schemas

{% elif cookiecutter.auth_method in ("OAuth2", "JWT") -%}
from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

from {{ cookiecutter.library_name }} import schemas
from {{ cookiecutter.library_name }}.auth import {{ cookiecutter.source_name }}Authenticator

{% endif -%}

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    {%- if cookiecutter.auth_method in ("OAuth2", "JWT") %}
    from singer_sdk.helpers.types import Auth, Context
    {%- else %}
    from singer_sdk.helpers.types import Context
    {%- endif %}


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = SchemaDirectory(schemas)


class {{ cookiecutter.source_name }}Stream({{ cookiecutter.stream_type }}Stream):
    """{{ cookiecutter.source_name }} stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    schema: ClassVar[StreamSchema] = StreamSchema(SCHEMAS_DIR)

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return "https://api.mysample.com"

{%- if cookiecutter.auth_method in ("OAuth2", "JWT") %}

    @override
    @cached_property
    def authenticator(self) -> Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return {{ cookiecutter.source_name }}Authenticator(
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            auth_endpoint="TODO: OAuth Endpoint URL",
            oauth_scopes="TODO: OAuth Scopes",
        )

{%- elif cookiecutter.auth_method == "API Key" %}

    @override
    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator(
            key="x-api-key",
            value=self.config.get("auth_token", ""),
            location="header",
        )

{%- elif cookiecutter.auth_method == "Bearer Token" %}

    @override
    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator(token=self.config.get("auth_token", ""))

{%- elif cookiecutter.auth_method == "Basic Auth" %}

    @override
    @property
    def authenticator(self) -> HTTPBasicAuth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return HTTPBasicAuth(
            username=self.config.get("username", ""),
            password=self.config.get("password", ""),
        )

{%- endif %}

    @property
    @override
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
{%- if cookiecutter.auth_method not in ("OAuth2", "JWT") %}
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
{%- endif %}
        return {}

    @override
    def get_new_paginator(self) -> BaseAPIPaginator | None:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance, or ``None`` to indicate pagination
            is not supported.
        """
        return super().get_new_paginator()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Note: As of SDK v0.47.0, this method is automatically executed for all stream types.
        You should not need to call this method directly in custom `get_records` implementations.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row
