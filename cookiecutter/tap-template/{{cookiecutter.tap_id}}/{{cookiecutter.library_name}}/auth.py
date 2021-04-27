"""{{ cookiecutter.source_name }} Authentication."""

{% if cookiecutter.auth_method not in ("Simple", "OAuth2", "JWT") %}
# TODO: Delete this file or add custom authentication logic as needed.
{% elif cookiecutter.auth_method == "Simple" %}
from singer_sdk.authenticators import SimpleAuthenticator


class {{ cookiecutter.source_name }}Authenticator(SimpleAuthenticator):
    """Authenticator class for {{ cookiecutter.source_name }}."""

    @classmethod
    def create_for_stream(cls, stream):
        return cls(
            stream=stream,
            auth_headers={
                "Private-Token": stream.config.get("auth_token")
            }
        )

{% elif cookiecutter.auth_method == "OAuth2" %}
from singer_sdk.authenticators import OAuthAuthenticator


class {{ cookiecutter.source_name }}Authenticator(OAuthAuthenticator):
    """Authenticator class for {{ cookiecutter.source_name }}."""

    @classmethod
    def create_for_stream(cls, stream):
        return cls(
            stream=stream,
            auth_endpoint="TODO: OAuth Endpoint URL",
            oauth_scopes="TODO: OAuth Scopes",
        )

{% elif cookiecutter.auth_method == "JWT" %}
from singer_sdk.authenticators import OAuthJWTAuthenticator


class {{ cookiecutter.source_name }}Authenticator(OAuthJWTAuthenticator):
    """Authenticator class for {{ cookiecutter.source_name }}."""

    @classmethod
    def create_for_stream(cls, stream):
        return cls(
            stream=stream,
            auth_endpoint="TODO: OAuth Endpoint URL",
            oauth_scopes="TODO: OAuth Scopes",
        )

{% endif %}
