# Signals

This guide will show you how to use the built-in [Blinker](inv:blinker:std:doc#index) signals in the Singer SDK.

## Settings write-back

The SDK provides a signal that allows you to write back settings to the configuration file. This is useful if you want to update the configuration file with new settings that were set during the run, like a `refresh_token`.

```python
import requests
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.plugin_base import PluginBase


class RefreshTokenAuthenticator(OAuthAuthenticator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.refresh_token = self.config["refresh_token"]

    @property
    def oauth_request_body(self):
        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "user_type": "Location",
        }

    def update_access_token(self):
        token_response = requests.post(
            self.auth_endpoint,
            headers=self._oauth_headers,
            data=auth_request_payload,
            timeout=60,
        )
        token_response.raise_for_status()
        token_json = token_response.json()

        self.access_token = token_json["access_token"]
        self.refresh_token = token_json["refresh_token"]
        PluginBase.config_updated.send(self, refresh_token=self.refresh_token)
```

In the example above, the `RefreshTokenAuthenticator` class is a subclass of `OAuthAuthenticator` that calls `PluginBase.config_updated.send` to send a signal to update the `refresh_token` in tap's configuration.

```{note}
Only when a single file is passed via the `--config` command line option, the SDK will write back the settings to the same file.
```
