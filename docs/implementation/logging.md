# Logging

Logs are configurable by the environment variables `<PLUGIN_NAME>_LOGLEVEL` (preferred) or `LOGLEVEL`. Use `LOGLEVEL` when you intend to control the log output for all taps and targets running within the environment. In contrast, we recommend setting `<PLUGIN_NAME>_LOGLEVEL` for more granual control of each tap or target individually.

From most verbose to least verbose, the accepted values for logging level are `debug`, `info`, `warning`, and `error`. Logging level inputs are case insensitive.
