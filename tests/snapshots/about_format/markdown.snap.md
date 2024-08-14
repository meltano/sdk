# `tap-example`

Example tap for Singer SDK

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `catalog`
* `discover`
* `state`

## Supported Python Versions

* 3.8
* 3.9
* 3.10
* 3.11
* 3.12
* 3.13

## Settings

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| start_date | False    | None    | Start date for the tap to extract data from. |
| api_key | True     | None    | API key for the tap to use. |
| complex_setting | False    | None    | A complex setting, with sub-settings. |
| complex_setting.sub_setting | False    | None    | A sub-setting. |

A full list of supported settings and capabilities is available by running: `tap-example --about`
