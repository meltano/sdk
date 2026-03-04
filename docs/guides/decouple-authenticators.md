# Decoupling authenticators from streams

In v0.58 the `stream` constructor parameter and all related shim API on authenticator
classes will be removed. Authenticators are now constructed directly with explicit
parameters, making them easier to test in isolation without a stream instance.
This guide walks through each affected class and shows the new construction pattern.

## Background

The old pattern passed a stream instance directly to the authenticator constructor so
it could read `stream.config` and `stream.tap_name` internally:

```python
# Old (deprecated)
class MyStream(RESTStream):
    @cached_property
    def authenticator(self):
        return APIKeyAuthenticator(
            stream=self, key="x-api-key", value=self.config["api_key"]
        )
```

Authenticators no longer hold a reference to the stream. Config values are passed
explicitly at construction time, which makes authenticators easier to test in isolation
and removes a hidden dependency on the stream lifecycle.

## What's removed

| Deprecated | Replacement |
|---|---|
| `stream` constructor parameter (all authenticator classes) | Construct directly with explicit kwargs |
| `APIKeyAuthenticator.create_for_stream()` | Construct `APIKeyAuthenticator` directly |
| `BearerTokenAuthenticator.create_for_stream()` | Construct `BearerTokenAuthenticator` directly |
| `BasicAuthenticator.create_for_stream()` | Construct `BasicAuthenticator` directly |
| `APIAuthenticatorBase.tap_name` property | Access via your tap/stream directly |
| `APIAuthenticatorBase.config` property | Access via your tap/stream directly |

## Simple authenticators

`APIKeyAuthenticator`, `BearerTokenAuthenticator`, and `BasicAuthenticator` are the
easiest to migrate — drop `stream=` and the `create_for_stream` factory:

```python
# Old (deprecated)
APIKeyAuthenticator(stream=self, key="x-api-key", value="secret")
APIKeyAuthenticator.create_for_stream(stream=self, key="x-api-key", value="secret")

BearerTokenAuthenticator(stream=self, token="my-token")
BearerTokenAuthenticator.create_for_stream(stream=self, token="my-token")

BasicAuthenticator(stream=self, username="user", password="pass")
BasicAuthenticator.create_for_stream(stream=self, username="user", password="pass")

# New
APIKeyAuthenticator(key="x-api-key", value="secret")
BearerTokenAuthenticator(token="my-token")
BasicAuthenticator(username="user", password="pass")
```

In your stream's `authenticator` property, read config values directly from `self.config`:

```python
@cached_property
def authenticator(self) -> APIKeyAuthenticator:
    return APIKeyAuthenticator(key="x-api-key", value=self.config["api_key"])
```

## OAuth authenticators

`OAuthAuthenticator` and `OAuthJWTAuthenticator` require a few more steps because the
old `stream=` path read `client_id`, `client_secret` (and `private_key` for JWT) from
`stream.config` automatically. These must now be passed explicitly.

### `OAuthAuthenticator`

```python
# Old (deprecated)
class MyAuthenticator(OAuthAuthenticator):
    @classmethod
    def create_for_stream(cls, stream):
        return cls(
            stream=stream,
            auth_endpoint="https://api.example.com/oauth/token",
        )

    @property
    def oauth_request_body(self) -> dict:
        return {
            "client_id": self.client_id,  # populated from stream.config
            "client_secret": self.client_secret,  # populated from stream.config
            "grant_type": "client_credentials",
        }


# In your stream:
@cached_property
def authenticator(self):
    return MyAuthenticator.create_for_stream(self)


# New — remove create_for_stream; pass config values from the stream
class MyAuthenticator(OAuthAuthenticator):
    @property
    def oauth_request_body(self) -> dict:
        return {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }


# In your stream:
@cached_property
def authenticator(self):
    return MyAuthenticator(
        auth_endpoint="https://api.example.com/oauth/token",
        client_id=self.config["client_id"],
        client_secret=self.config["client_secret"],
    )
```

### `OAuthJWTAuthenticator`

The same pattern applies; pass `private_key` and `private_key_passphrase` explicitly:

```python
# Old (deprecated)
class MyJWTAuthenticator(OAuthJWTAuthenticator):
    @classmethod
    def create_for_stream(cls, stream):
        return cls(stream=stream, auth_endpoint="https://api.example.com/oauth/token")


# In your stream:
@cached_property
def authenticator(self):
    return MyJWTAuthenticator.create_for_stream(self)


# New
# In your stream:
@cached_property
def authenticator(self):
    return MyJWTAuthenticator(
        auth_endpoint="https://api.example.com/oauth/token",
        client_id=self.config["client_id"],
        client_secret=self.config["client_secret"],
        private_key=self.config["private_key"],
        private_key_passphrase=self.config.get("private_key_passphrase"),
    )
```

## Subclasses that access `self.config` or `self.tap_name`

If your authenticator subclass reads `self.config` or `self.tap_name` in methods like
`oauth_request_body`, store those values in `__init__` instead:

```python
# Old (deprecated) — reads self.config inside the authenticator
class MyAuthenticator(OAuthAuthenticator):
    @property
    def oauth_request_body(self) -> dict:
        return {
            "audience": self.config["audience"],  # will no longer work
            "grant_type": "client_credentials",
        }


# New — receive the value at construction time
class MyAuthenticator(OAuthAuthenticator):
    def __init__(self, *, audience: str, **kwargs: t.Any) -> None:
        super().__init__(**kwargs)
        self._audience = audience

    @property
    def oauth_request_body(self) -> dict:
        return {
            "audience": self._audience,
            "grant_type": "client_credentials",
        }


# In your stream:
@cached_property
def authenticator(self):
    return MyAuthenticator(
        auth_endpoint="https://api.example.com/oauth/token",
        client_id=self.config["client_id"],
        client_secret=self.config["client_secret"],
        audience=self.config["audience"],
    )
```
