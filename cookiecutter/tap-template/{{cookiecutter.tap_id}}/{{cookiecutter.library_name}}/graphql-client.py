"""GraphQL client handling, including {{ cookiecutter.source_name }}Stream base class."""

from __future__ import annotations

import decimal
import typing as t

import requests  # noqa: TC002
from singer_sdk.streams import {{ cookiecutter.stream_type }}Stream

{%- if cookiecutter.auth_method in ("OAuth2", "JWT") %}

from {{ cookiecutter.library_name }}.auth import {{ cookiecutter.source_name }}Authenticator
{%- endif %}

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class {{ cookiecutter.source_name }}Stream({{ cookiecutter.stream_type }}Stream):
    """{{ cookiecutter.source_name }} stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return "https://api.mysample.com"

{%- if cookiecutter.auth_method in ("OAuth2", "JWT") %}

    @property
    def authenticator(self) -> {{ cookiecutter.source_name }}Authenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return {{ cookiecutter.source_name }}Authenticator.create_for_stream(self)

{%- endif %}

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
{%- if cookiecutter.auth_method not in ("OAuth2", "JWT") %}
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
{%- endif %}
        return {}

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        resp_json = response.json(parse_float=decimal.Decimal)
        yield from resp_json.get("<TODO>")

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
