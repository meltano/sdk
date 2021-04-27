"""{{ cookiecutter.source_name }} Authentication."""

{% if cookiecutter.auth_method not in ("Simple", "OAuth2", "JWT") %}
# TODO: Delete this file or add custom authentication logic as needed.
{% else %}

from typing import TypeVar, Type

from singer_sdk.authenticators import (
    APIAuthenticatorBase,
    SimpleAuthenticator,
    OAuthAuthenticator,
    OAuthJWTAuthenticator
)

# Create a generic variable that can be 'Parent', or any subclass.
FactoryType = TypeVar('FactoryType', bound='{{ cookiecutter.source_name }}Authenticator')

{% if cookiecutter.auth_method == "Simple" %}
class {{ cookiecutter.source_name }}Authenticator(SimpleAuthenticator):
    """Authenticator class for {{ cookiecutter.source_name }}."""

    @classmethod
    def create_for_stream(cls: Type[FactoryType], stream) -> FactoryType:
        return cls(
            stream=stream,
            auth_headers={
                "Private-Token": stream.config.get("auth_token")
            }
        )

{% elif cookiecutter.auth_method == "OAuth2" %}
class {{ cookiecutter.source_name }}Authenticator(OAuthAuthenticator):
    """Authenticator class for {{ cookiecutter.source_name }}."""

    @classmethod
    def create_for_stream(cls: Type[FactoryType], stream) -> FactoryType:
        return cls(
            stream=stream,
            auth_endpoint="TODO: OAuth Endpoint URL",
            oauth_scopes="TODO: OAuth Scopes",
        )

{% elif cookiecutter.auth_method == "JWT" %}
class {{ cookiecutter.source_name }}Authenticator(OAuthJWTAuthenticator):
    """Authenticator class for {{ cookiecutter.source_name }}."""

    @classmethod
    def create_for_stream(cls: Type[FactoryType], stream) -> FactoryType:
        return cls(
            stream=stream,
            auth_endpoint="TODO: OAuth Endpoint URL",
            oauth_scopes="TODO: OAuth Scopes",
        )

{% endif %}
{% endif %}
