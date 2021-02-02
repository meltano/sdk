"""Global tap config file."""

PLUGIN_NAME = "sample-tap-snowflake"
ACCEPTED_CONFIG_OPTIONS = [
    "account",
    "dbname",
    "warehouse",
    "user",
    "password",
    "tables",
]
REQUIRED_CONFIG_SETS = [["account", "dbname", "warehouse", "user", "password"]]
