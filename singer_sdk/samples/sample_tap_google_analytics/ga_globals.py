"""Global settings and helper functions."""

PLUGIN_NAME = "sample-tap-google-analytics"
ACCEPTED_CONFIG_OPTIONS = [
    "view_id",
    "client_email",
    "private_key",
]
REQUIRED_CONFIG_SETS = [["view_id", "client_email", "private_key",]]
