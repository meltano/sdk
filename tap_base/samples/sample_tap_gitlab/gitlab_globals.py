"""Global settings and helper functions."""

PLUGIN_NAME = "sample-tap-gitlab"
ACCEPTED_CONFIG_OPTIONS = ["auth_token", "project_ids", "start_date"]
REQUIRED_CONFIG_SETS = [["auth_token", "project_ids", "start_date"]]
