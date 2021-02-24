"""Global tap config file."""

PLUGIN_NAME = "{{ cookiecutter.tap_id }}"
ACCEPTED_CONFIG_KEYS = [{{cookiecutter.config_options}}]
REQUIRED_CONFIG_OPTIONS = [
    [{{cookiecutter.config_options}}],
]
