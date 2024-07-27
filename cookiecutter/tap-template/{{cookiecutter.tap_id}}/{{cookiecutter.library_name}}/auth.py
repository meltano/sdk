"""{{ cookiecutter.source_name }} Authentication."""

from __future__ import annotations

{%- if cookiecutter.auth_method not in ("Basic Auth", "OAuth2", "JWT") %}

# TODO: Delete this file or add custom authentication logic as needed.

{%- elif cookiecutter.auth_method == "OAuth2" %}

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class {{ cookiecutter.source_name }}Authenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for {{ cookiecutter.source_name }}."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the AutomaticTestTap API.

        Returns:
            A dict with the request body
        """
        # TODO: Define the request body needed for the API.
        return {
            "resource": "https://analysis.windows.net/powerbi/api",
            "scope": self.oauth_scopes,
            "client_id": self.client_id,
            "grant_type": "password",
        }
{%- elif cookiecutter.auth_method == "JWT" %}

from singer_sdk.authenticators import OAuthJWTAuthenticator


class {{ cookiecutter.source_name }}Authenticator(OAuthJWTAuthenticator):
    """Authenticator class for {{ cookiecutter.source_name }}."""
{%- endif %}
