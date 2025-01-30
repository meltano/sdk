"""REST client handling, including {{ cookiecutter.source_name }}Stream base class."""

from __future__ import annotations

import decimal
import typing as t
{% if cookiecutter.auth_method in ("OAuth2", "JWT") -%}
from functools import cached_property
{% endif -%}
from importlib import resources

{% if cookiecutter.auth_method  == "API Key" -%}
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

{% elif cookiecutter.auth_method  == "Bearer Token" -%}
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

{% elif cookiecutter.auth_method == "Basic Auth" -%}
from requests.auth import HTTPBasicAuth
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

{% elif cookiecutter.auth_method == "Custom or N/A" -%}
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

{% elif cookiecutter.auth_method in ("OAuth2", "JWT") -%}
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

from {{ cookiecutter.library_name }}.auth import {{ cookiecutter.source_name }}Authenticator

{% endif -%}

if t.TYPE_CHECKING:
    import requests
    {%- if cookiecutter.auth_method in ("OAuth2", "JWT") %}
    from singer_sdk.helpers.types import Auth, Context
    {%- else %}
    from singer_sdk.helpers.types import Context
    {%- endif %}


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class {{ cookiecutter.source_name }}Stream({{ cookiecutter.stream_type }}Stream):
    """{{ cookiecutter.source_name }} stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return "https://api.mysample.com"

{%- if cookiecutter.auth_method in ("OAuth2", "JWT") %}

    @cached_property
    def authenticator(self) -> Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return {{ cookiecutter.source_name }}Authenticator.create_for_stream(self)

{%- elif cookiecutter.auth_method == "API Key" %}

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="x-api-key",
            value=self.config.get("auth_token", ""),
            location="header",
        )

{%- elif cookiecutter.auth_method == "Bearer Token" %}

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config.get("auth_token", ""),
        )

{%- elif cookiecutter.auth_method == "Basic Auth" %}

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

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return super().get_new_paginator()

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
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

    def prepare_request_payload(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ARG002, ANN401
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

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
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

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row
