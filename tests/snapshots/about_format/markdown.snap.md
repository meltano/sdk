# `tap-example`

Example tap for Singer SDK

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `catalog`
* `discover`
* `state`

## Settings

| Setting   | Required | Default | Description |
|:----------|:--------:|:-------:|:------------|
| start_date| False    | None    | Start date for the tap to extract data from. |
| api_key   | True     | None    | API key for the tap to use. |

A full list of supported settings and capabilities is available by running: `tap-example --about`
