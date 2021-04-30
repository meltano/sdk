# [Singer SDK Implementation Details](/.README.md) - Tap CLI

The Singer SDK automatically adds Tap CLI handling.

## Configuration (`--config`)

The SDK supports one or more `--config` inputs when run from the CLI.

- If one of the supplied inputs is `--config ENV` (or `--config=ENV` according to the user's preference), the environment variable parsing rules will be applied to ingest config values from environment variables.
- One or more files can also be sent to `--config`. If multiple files are sent, they will be processed in sequential order.
If one or more files conflict for a given setting, the latter provided files will override earlier provided files.
  - This behavior allows to you easily inject environment overrides by adding `--config=path/to/overrides.json` at the end of the CLI command text.
- If `--config=ENV` is set and one or more files conflict with an environment variable setting, the environment variable setting will always have precedence, regardless of ordering.
- One benefit of this approach is that credentials and other secrets can be stored completely separately from general settings: either by having two distinct `config.json` files or by using environment variables for secure settings and `config.json` files for the rest.

### Parsing Environment Variables

When `--config=ENV` is specified, the SDK will automatically capture and pass along any
values from environment variables which match the exact name of a setting, along with a
prefix determined by the plugin name.

> For example: For a sample plugin named `tap-my-example` and settings named "username" and "access_key", the SDK will automatically scrape
> the settings from environment variables `TAP_MY_EXAMPLE_USERNAME` and
> `TAP_MY_EXAMPLE_ACCESS_KEY`, if they exist.

## Input Catalog (`--catalog`)

If provided, an input catalog will be ingested and passed along to the tap during
initialization. This is called 'input_catalog' to distinguish from the discovered catalog.
If applicable, the provided input catalog will be merged with the
[discovered catalog](./discovery.md) in the `Tap.apply_catalog()` method. Developers can
override this method to achieve custom handling.

### Input Catalog Stream Selection

The SDK automatically applies selection logic as described by the
[Singer Spec](https://meltano.com/docs/singer-spec.html#metadata).

Selection rules are applied at three levels:

1. **Streams** are filtered out if they are deselected or ommitted in the input catalog.
2. **RECORD messages** are filtered based upon selection rules in the input catalog.
3. **SCHEMA messages** are filtered based upon selection rules in the input catalog.
