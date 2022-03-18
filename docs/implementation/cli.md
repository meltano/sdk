# [SDK Implementation Details](./README.md) - Tap CLI

This page describes how taps and targets can be invoked via the command line interface, or "CLI".

## List of Built-in Execution Flags

### `--state` (taps only)

Used to specify the path to a state file. The state file is used for resuming incremental progress on subsequent executions of the tap.

### `--catalog` (taps only)

Used to specify the path to a catalog file. The state file is used for resuming incremental progress on subsequent executions of the tap.

#### Input Catalog Stream Selection

The SDK automatically applies selection logic as described by the
[Singer Spec](https://hub.meltano.com/singer/spec#metadata).

Selection rules are applied at three levels:

1. **Streams** are filtered out if they are deselected or ommitted in the input catalog.
2. **RECORD messages** are filtered based upon selection rules in the input catalog.
3. **SCHEMA messages** are filtered based upon selection rules in the input catalog.

### `--test` or `--test schema` (taps only)

When invoked on its own, the `--test` flag will cause the tap to emit one record per stream and then exit.

When invoked with the `schema` option, such as `--test=schema` or `--test schema`, the tap will only emit `SCHEMA` messages downstream and will skip emitting any `RECORD` messages. This option is helpful if you want to the target to pre-create all target tables without inserting any records.

### `--discover` (taps only)

Runs the tap in discovery mode and then exits without syncing any data.

### `--help` (taps and targets)

Prints information about the tap or target, including a full list of supported CLI options.

### `--version` (taps and targets)

Prints the version of the tap or target along with the SDK version and then exits.

### `--about` (taps and targets)

Prints important information about the tap or target, including the list of supported CLI commands, the `--version` metadata, and list of supported capabilities.

By default, the format of `--about` is plain text. You can invoke `--about` in combination with the `--format` option to have metadata targeted to specific use cases such as `markdown` (for use in a README) or `json` to optimize for machine-readability.

#### `--format` (used in combination with `--about`)

When `--format=json` is specified, the `--about` information will be printed as `json` in order to easily process the metadata in automated workflows.

When `--format=markdown` is specified, the `--about` information will be printed as Markdown, optimized for copy-pasting into the maintainer's `README.md` file. Among other helpful guidance, this automatically creates a markdown table of all settings, their descriptions, and their default values.

### `--config` (taps and targets)

The SDK supports one or more `--config` inputs when run from the CLI.

- If one of the supplied inputs is `--config ENV` (or `--config=ENV` according to the user's preference), the environment variable parsing rules will be applied to ingest config values from environment variables.
- One or more files can also be sent to `--config`. If multiple files are sent, they will be processed in sequential order.
If one or more files conflict for a given setting, the latter provided files will override earlier provided files.
  - This behavior allows to you easily inject environment overrides by adding `--config=path/to/overrides.json` at the end of the CLI command text.
- If `--config=ENV` is set and one or more files conflict with an environment variable setting, the environment variable setting will always have precedence, regardless of ordering.
- One benefit of this approach is that credentials and other secrets can be stored completely separately from general settings: either by having two distinct `config.json` files or by using environment variables for secure settings and `config.json` files for the rest.

#### Parsing config from environment variables

When `--config=ENV` is specified, the SDK will automatically capture and pass along any
values from environment variables which match the exact name of a setting, along with a
prefix determined by the plugin name.

> For example: For a sample plugin named `tap-my-example` and settings named "username" and "access_key", the SDK will automatically scrape
> the settings from environment variables `TAP_MY_EXAMPLE_USERNAME` and
> `TAP_MY_EXAMPLE_ACCESS_KEY`, if they exist.
