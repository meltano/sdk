"""Classes to assist in authenticating to APIs."""

from __future__ import annotations

import base64
import datetime
import logging
import math
import typing as t
import warnings
from types import MappingProxyType
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import requests

from singer_sdk.helpers._compat import SingerSDKDeprecationWarning, deprecated
from singer_sdk.helpers._util import utc_now

if t.TYPE_CHECKING:
    from singer_sdk.streams.rest import _HTTPStream


def _add_parameters(initial_url: str, extra_parameters: dict) -> str:
    """Add parameters to an URL and return the new URL.

    Args:
        initial_url: The URL to add parameters to.
        extra_parameters: The parameters to add.

    Returns:
        The new URL with the parameters added.
    """
    scheme, netloc, path, query_string, fragment = urlsplit(initial_url)
    query_params = parse_qs(query_string)
    query_params.update(
        {
            parameter_name: [parameter_value]
            for parameter_name, parameter_value in extra_parameters.items()
        },
    )

    new_query_string = urlencode(query_params, doseq=True)

    return str(urlunsplit((scheme, netloc, path, new_query_string, fragment)))


class SingletonMeta(type):
    """A general purpose singleton metaclass."""

    def __init__(cls, name: str, bases: tuple[type], dic: dict) -> None:
        """Init metaclass.

        The single instance is saved as an attribute of the the metaclass.

        Args:
            name: Name of the derived class.
            bases: Base types of the derived class.
            dic: Class dictionary of the derived class.
        """
        cls.__single_instance = None
        super().__init__(name, bases, dic)

    def __call__(cls, *args: t.Any, **kwargs: t.Any) -> t.Any:  # noqa: ANN401
        """Create or reuse the singleton.

        Args:
            args: Class constructor positional arguments.
            kwargs: Class constructor keyword arguments.

        Returns:
            A singleton instance of the derived class.
        """
        if cls.__single_instance:
            return cls.__single_instance  # type: ignore[unreachable]
        single_obj = cls.__new__(cls, None)  # type: ignore[call-overload]
        single_obj.__init__(*args, **kwargs)
        cls.__single_instance = single_obj
        return single_obj


def _get_stream_param(*args: t.Any, **kwargs: t.Any) -> _HTTPStream | None:
    """Get the stream parameter from the arguments or keyword arguments.

    Args:
        args: Positional arguments.
        kwargs: Keyword arguments.

    Returns:
        The stream parameter value if it is found, otherwise None.
    """
    from singer_sdk.streams.rest import _HTTPStream  # noqa: PLC0415

    if len(args) >= 1 and isinstance(args[0], _HTTPStream):
        return args[0]

    return kwargs.get("stream")


def _warn_stream_param_deprecation() -> None:
    warnings.warn(
        (
            "The `stream` parameter is deprecated and will be removed in a "
            "future version"
        ),
        SingerSDKDeprecationWarning,
        stacklevel=2,
    )


class APIAuthenticatorBase:
    """Base class for offloading API auth.

    Attributes:
        auth_headers: HTTP headers for authentication.
        auth_params: URL query parameters for authentication.
    """

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Init authenticator.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        self._tap_name: str
        self._config: dict[str, t.Any]

        if stream := _get_stream_param(*args, **kwargs):
            _warn_stream_param_deprecation()
            self._tap_name = stream.tap_name
            self._config = dict(stream.config)
        else:
            self._tap_name = "tap"
            self._config = {}

        self.auth_headers: dict[str, t.Any] = {}
        self.auth_params: dict[str, t.Any] = {}
        self.logger: logging.Logger = logging.getLogger(__name__)

    @property
    @deprecated(
        (
            "The `tap_name` property is deprecated and will be removed in a "
            "future version"
        ),
        category=SingerSDKDeprecationWarning,
    )
    def tap_name(self) -> str:
        """The tap name."""
        return self._tap_name  # pragma: no cover

    @tap_name.setter
    def tap_name(self, name: str) -> None:
        """Set the tap name."""
        self._tap_name = name

    @property
    @deprecated(
        "The `config` property is deprecated and will be removed in a future version",
        category=SingerSDKDeprecationWarning,
    )
    def config(self) -> t.Mapping[str, t.Any]:
        """Get stream or tap config.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        return MappingProxyType(self._config)

    def authenticate_request(
        self,
        request: requests.PreparedRequest,
    ) -> requests.PreparedRequest:
        """Authenticate a request.

        Args:
            request: A :class:`requests.PreparedRequest` object.

        Returns:
            The authenticated request object.
        """
        request.headers.update(self.auth_headers)

        if request.url:
            request.url = _add_parameters(request.url, self.auth_params)

        return request

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        """Authenticate a request.

        Calls
        :meth:`~singer_sdk.authenticators.APIAuthenticatorBase.authenticate_request`
        and returns the result.

        Args:
            r: A :class:`requests.PreparedRequest` object.

        Returns:
            The authenticated request object.
        """
        return self.authenticate_request(r)


class SimpleAuthenticator(APIAuthenticatorBase):
    """DEPRECATED: Please use a more specific authenticator.

    This authenticator will merge a key-value pair to the stream
    in either the request headers or query parameters.
    """

    def __init__(
        self,
        *args: t.Any,
        auth_headers: dict | None = None,
        **kwargs: t.Any,
    ) -> None:
        """Create a new authenticator.

        If auth_headers is provided, it will be merged with http_headers specified on
        the stream.

        Args:
            *args: Positional arguments.
            auth_headers: Authentication headers.
            **kwargs: Keyword arguments.
        """
        if stream := _get_stream_param(*args, **kwargs):
            _warn_stream_param_deprecation()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r"The `stream` parameter is deprecated.*",
                    category=SingerSDKDeprecationWarning,
                )
                super().__init__(stream=stream)
        else:
            super().__init__(*args, **kwargs)

        if self.auth_headers is None:  # pragma: no cover
            self.auth_headers = {}  # type: ignore[unreachable]
        if auth_headers:
            self.auth_headers.update(auth_headers)


class APIKeyAuthenticator(APIAuthenticatorBase):
    """Implements API key authentication for REST Streams.

    This authenticator will merge a key-value pair with either the
    HTTP headers or query parameters specified on the stream. Common
    examples of key names are "x-api-key" and "Authorization" but
    any key-value pair may be used for this authenticator.
    """

    def __init__(
        self,
        *args: t.Any,
        key: str,
        value: str,
        location: t.Literal["header", "params"] = "header",
        **kwargs: t.Any,
    ) -> None:
        """Create a new authenticator.

        Args:
            *args: Positional arguments.
            key: API key parameter name.
            value: API key value.
            location: Where the API key is to be added. Either 'header' or 'params'.
            **kwargs: Keyword arguments.

        Raises:
            ValueError: If the location value is not 'header' or 'params'.
        """
        if stream := _get_stream_param(*args, **kwargs):
            _warn_stream_param_deprecation()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r"The `stream` parameter is deprecated.*",
                    category=SingerSDKDeprecationWarning,
                )
                super().__init__(stream=stream)
        else:
            super().__init__(*args, **kwargs)

        auth_credentials = {key: value}

        if location not in {"header", "params"}:
            msg = "`type` must be one of 'header' or 'params'."
            raise ValueError(msg)

        if location == "header":
            if self.auth_headers is None:  # pragma: no cover
                self.auth_headers = {}  # type: ignore[unreachable]
            self.auth_headers.update(auth_credentials)
        elif location == "params":
            if self.auth_params is None:  # pragma: no cover
                self.auth_params = {}  # type: ignore[unreachable]
            self.auth_params.update(auth_credentials)

    @classmethod
    @deprecated(
        (
            "The `create_for_stream` method is deprecated and will be removed in a "
            "future version"
        ),
        category=SingerSDKDeprecationWarning,
    )
    def create_for_stream(
        cls: type[APIKeyAuthenticator],
        stream: _HTTPStream,
        key: str,
        value: str,
        location: t.Literal["header", "params"],
    ) -> APIKeyAuthenticator:
        """Create an Authenticator object specific to the Stream class.

        Args:
            stream: The stream instance to use with this authenticator.
            key: API key parameter name.
            value: API key value.
            location: Where the API key is to be added. Either 'header' or 'params'.

        Returns:
            APIKeyAuthenticator: A new
                :class:`singer_sdk.authenticators.APIKeyAuthenticator` instance.
        """
        return cls(stream=stream, key=key, value=value, location=location)


class BearerTokenAuthenticator(APIAuthenticatorBase):
    """Implements bearer token authentication for REST Streams.

    This Authenticator implements Bearer Token authentication. The token
    is a text string, included in the request header and prefixed with
    'Bearer '. The token will be merged with HTTP headers on the stream.
    """

    def __init__(self, *args: t.Any, token: str, **kwargs: t.Any) -> None:
        """Create a new authenticator.

        Args:
            *args: Positional arguments.
            token: Authentication token.
            **kwargs: Keyword arguments.
        """
        if stream := _get_stream_param(*args, **kwargs):
            _warn_stream_param_deprecation()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r"The `stream` parameter is deprecated.*",
                    category=SingerSDKDeprecationWarning,
                )
                super().__init__(stream=stream)
        else:
            super().__init__(*args, **kwargs)

        auth_credentials = {"Authorization": f"Bearer {token}"}

        if self.auth_headers is None:  # pragma: no cover
            self.auth_headers = {}  # type: ignore[unreachable]
        self.auth_headers.update(auth_credentials)

    @classmethod
    @deprecated(
        (
            "The `create_for_stream` method is deprecated and will be removed in a "
            "future version"
        ),
        category=SingerSDKDeprecationWarning,
    )
    def create_for_stream(
        cls: type[BearerTokenAuthenticator],
        stream: _HTTPStream,
        token: str,
    ) -> BearerTokenAuthenticator:
        """Create an Authenticator object specific to the Stream class.

        Args:
            stream: The stream instance to use with this authenticator.
            token: Authentication token.

        Returns:
            BearerTokenAuthenticator: A new
                :class:`singer_sdk.authenticators.BearerTokenAuthenticator` instance.
        """
        return cls(stream=stream, token=token)


@deprecated(
    "BasicAuthenticator is deprecated and will be removed by 2026-01-01. "
    "Use `requests.auth.HTTPBasicAuth` instead.",
    category=SingerSDKDeprecationWarning,
)
class BasicAuthenticator(APIAuthenticatorBase):
    """Implements basic authentication for REST Streams.

    .. deprecated:: 0.36.0
       Use :class:`requests.auth.HTTPBasicAuth` instead.

    This Authenticator implements basic authentication by concatenating a
    username and password then base64 encoding the string. The resulting
    token will be merged with any HTTP headers specified on the stream.
    """

    def __init__(
        self,
        *args: t.Any,
        username: str,
        password: str,
        **kwargs: t.Any,
    ) -> None:
        """Create a new authenticator.

        Args:
            *args: Positional arguments.
            username: API username.
            password: API password.
            **kwargs: Keyword arguments.
        """
        if stream := _get_stream_param(*args, **kwargs):
            _warn_stream_param_deprecation()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r"The `stream` parameter is deprecated.*",
                    category=SingerSDKDeprecationWarning,
                )
                super().__init__(stream=stream)
        else:
            super().__init__(*args, **kwargs)

        credentials = f"{username}:{password}".encode()
        auth_token = base64.b64encode(credentials).decode("ascii")
        auth_credentials = {"Authorization": f"Basic {auth_token}"}

        if self.auth_headers is None:  # pragma: no cover
            self.auth_headers = {}  # type: ignore[unreachable]
        self.auth_headers.update(auth_credentials)

    @classmethod
    @deprecated(
        (
            "The `create_for_stream` method is deprecated and will be removed in a "
            "future version"
        ),
        category=SingerSDKDeprecationWarning,
    )
    def create_for_stream(
        cls: type[BasicAuthenticator],
        stream: _HTTPStream,
        username: str,
        password: str,
    ) -> BasicAuthenticator:
        """Create an Authenticator object specific to the Stream class.

        Args:
            stream: The stream instance to use with this authenticator.
            username: API username.
            password: API password.

        Returns:
            BasicAuthenticator: A new
                :class:`singer_sdk.authenticators.BasicAuthenticator` instance.
        """
        return cls(stream=stream, username=username, password=password)


class OAuthAuthenticator(APIAuthenticatorBase):
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        *args: t.Any,
        auth_endpoint: str | None = None,
        oauth_scopes: str | None = None,
        default_expiration: int | None = None,
        oauth_headers: dict | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        **kwargs: t.Any,
    ) -> None:
        """Create a new authenticator.

        Args:
            *args: Positional arguments.
            stream: The stream instance to use with this authenticator.
            auth_endpoint: The OAuth 2.0 authorization endpoint.
            oauth_scopes: A comma-separated list of OAuth scopes.
            default_expiration: Default token expiry in seconds.
            oauth_headers: An optional dict of headers required to get a token.
            client_id: The client ID to use in authentication.
            client_secret: The client secret to use in authentication.
            **kwargs: Keyword arguments.
        """
        if stream := _get_stream_param(*args, **kwargs):
            _warn_stream_param_deprecation()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r"The `stream` parameter is deprecated.*",
                    category=SingerSDKDeprecationWarning,
                )
                super().__init__(stream=stream)
            self._client_id = self.config.get("client_id")
            self._client_secret = self.config.get("client_secret")
        else:
            super().__init__(*args, **kwargs)
            self._client_id = client_id
            self._client_secret = client_secret

        self._auth_endpoint = auth_endpoint
        self._default_expiration = default_expiration
        self._oauth_scopes = oauth_scopes
        self._oauth_headers = oauth_headers or {}

        # Initialize internal tracking attributes
        self.access_token: str | None = None
        self.refresh_token: str | None = None
        self.last_refreshed: datetime.datetime | None = None
        self.expires_in: int | None = None

    def authenticate_request(
        self,
        request: requests.PreparedRequest,
    ) -> requests.PreparedRequest:
        """Authenticate an OAuth request.

        Args:
            request: A :class:`requests.PreparedRequest` object.

        Returns:
            The authenticated request object.
        """
        if not self.is_token_valid():
            self.update_access_token()

        self.auth_headers["Authorization"] = f"Bearer {self.access_token}"
        return super().authenticate_request(request)

    @property
    def auth_endpoint(self) -> str:
        """Get the authorization endpoint.

        Returns:
            The API authorization endpoint if it is set.

        Raises:
            ValueError: If the endpoint is not set.
        """
        if not self._auth_endpoint:
            msg = "Authorization endpoint not set."
            raise ValueError(msg)
        return self._auth_endpoint

    @property
    def oauth_scopes(self) -> str | None:
        """Get OAuth scopes.

        Returns:
            String of OAuth scopes, or None if not set.
        """
        return self._oauth_scopes

    @property
    def oauth_request_payload(self) -> dict:
        """Get request body.

        Returns:
            A plain (OAuth) or encrypted (JWT) request body.
        """
        return self.oauth_request_body

    @property
    def oauth_request_body(self) -> dict:
        """Get formatted body of the OAuth authorization request.

        Sample implementation:

        .. highlight:: python
        .. code-block:: python

            @property
            def oauth_request_body(self) -> dict:
                return {
                    "grant_type": "password",
                    "scope": "https://api.powerbi.com",
                    "resource": "https://analysis.windows.net/powerbi/api",
                    "client_id": self.client_id,
                    "username": self.username,
                    "password": self.password,
                }

        Raises:
            NotImplementedError: If derived class does not override this method.
        """
        msg = "The `oauth_request_body` property was not defined in the subclass."
        raise NotImplementedError(msg)

    @property
    def client_id(self) -> str | None:
        """Get client ID string to be used in authentication.

        Returns:
            Optional client secret from stream config if it has been set.
        """
        return self._client_id

    @property
    def client_secret(self) -> str | None:
        """Get client secret to be used in authentication.

        Returns:
            Optional client secret from stream config if it has been set.
        """
        return self._client_secret

    def is_token_valid(self) -> bool:
        """Check if token is valid.

        Returns:
            True if the token is valid (fresh).
        """
        if self.last_refreshed is None:
            return False
        if not self.expires_in:
            return True
        return self.expires_in > (utc_now() - self.last_refreshed).total_seconds()

    # Authentication and refresh
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        self.logger.info("Requesting new access token")
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        token_response = requests.post(
            self.auth_endpoint,
            headers=self._oauth_headers,
            data=auth_request_payload,
            timeout=60,
        )
        try:
            token_response.raise_for_status()
        except requests.HTTPError as ex:
            msg = f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            raise RuntimeError(msg) from ex

        self.logger.debug("OAuth authorization attempt was successful")

        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        expiration = token_json.get("expires_in", self._default_expiration)
        self.expires_in = int(expiration) if expiration else None
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in received in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires.",
            )
        self.last_refreshed = request_time


class OAuthJWTAuthenticator(OAuthAuthenticator):
    """API Authenticator for OAuth 2.0 flows which utilize a JWT refresh token."""

    def __init__(
        self,
        *args: t.Any,
        private_key: str | None = None,
        private_key_passphrase: str | None = None,
        **kwargs: t.Any,
    ) -> None:
        """Create a new JWT authenticator."""
        if stream := _get_stream_param(*args, **kwargs):
            _warn_stream_param_deprecation()
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=r"The `stream` parameter is deprecated.*",
                    category=SingerSDKDeprecationWarning,
                )
                super().__init__(stream=stream)
            self._private_key = self.config.get("private_key")
            self._private_key_passphrase = self.config.get("private_key_passphrase")
        else:
            super().__init__(*args, **kwargs)
            self._private_key = private_key
            self._private_key_passphrase = private_key_passphrase

    @property
    def private_key(self) -> str | None:
        """Return the private key to use in encryption.

        Returns:
            Private key from stream config.
        """
        return self._private_key

    @property
    def private_key_passphrase(self) -> str | None:
        """Return the private key passphrase to use in encryption.

        Returns:
            Passphrase for private key from stream config.
        """
        return self._private_key_passphrase

    @property
    def oauth_request_body(self) -> dict:
        """Return request body for OAuth request.

        Returns:
            Request body mapping for OAuth.
        """
        request_time = utc_now()
        return {
            "iss": self.client_id,
            "scope": self.oauth_scopes,
            "aud": self.auth_endpoint,
            "exp": math.floor((request_time + datetime.timedelta(hours=1)).timestamp()),
            "iat": math.floor(request_time.timestamp()),
        }

    @property
    def oauth_request_payload(self) -> dict:
        """Return request payload for OAuth request.

        Returns:
            Payload object for OAuth.

        Raises:
            RuntimeError: If the JWT dependencies are not installed.
            ValueError: If the private key is not set.
        """
        try:
            import jwt  # noqa: PLC0415
            from cryptography.hazmat.backends import default_backend  # noqa: PLC0415
            from cryptography.hazmat.primitives import serialization  # noqa: PLC0415
        except ModuleNotFoundError as ex:  # pragma: no cover
            msg = "Install singer-sdk[jwt] to use OAuthJWTAuthenticator."
            raise RuntimeError(msg) from ex

        if not self.private_key:  # pragma: no cover
            msg = "Missing 'private_key' property for OAuth payload."
            raise ValueError(msg)

        private_key: bytes | t.Any = bytes(self.private_key, "UTF-8")
        if self.private_key_passphrase:
            passphrase = bytes(self.private_key_passphrase, "UTF-8")
            private_key = serialization.load_pem_private_key(
                private_key,
                password=passphrase,
                backend=default_backend(),
            )
        private_key_string: str | t.Any = private_key.decode("UTF-8")
        return {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt.encode(
                self.oauth_request_body,
                private_key_string,
                "RS256",
            ),
        }
