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
            "client_id": self.config["client_id"],
            "username": self.config["username"],
            "password": self.config["password"],
            "grant_type": "password",
        }

    @classmethod
    def create_for_stream(cls, stream) -> {{ cookiecutter.source_name }}Authenticator:  # noqa: ANN001
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            auth_endpoint="TODO: OAuth Endpoint URL",
            oauth_scopes="TODO: OAuth Scopes",
        )
{%- elif cookiecutter.auth_method == "JWT" %}

from singer_sdk.authenticators import OAuthJWTAuthenticator


class {{ cookiecutter.source_name }}Authenticator(OAuthJWTAuthenticator):
    """Authenticator class for {{ cookiecutter.source_name }}."""

    @classmethod
    def create_for_stream(
        cls,
        stream,  # noqa: ANN001
    ) -> {{ cookiecutter.source_name }}Authenticator:
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            auth_endpoint="TODO: OAuth Endpoint URL",
            oauth_scopes="TODO: OAuth Scopes",
        )
{%- endif %}
