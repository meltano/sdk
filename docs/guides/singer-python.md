# Migrating from `singer-python`

If you maintain a tap or target that depends on
[`singer-python`](https://github.com/singer-io/singer-python) or
[`pipelinewise-singer-python`](https://github.com/transferwise/pipelinewise-singer-python)
but isn't ready to port to the full Meltano Singer SDK, you can swap in
[`meltano-singer-python`](https://pypi.org/project/meltano-singer-python/) instead. It
ships the top-level `singer` package and is a (mostly) drop-in replacement, maintained
as part of this project and released in lockstep with `singer-sdk`.

## Migration

````{tab} uv
```console
uv remove singer-python  # or pipelinewise-singer-python
uv add meltano-singer-python
```
````

````{tab} Poetry
```console
poetry remove singer-python  # or pipelinewise-singer-python
poetry add meltano-singer-python
```
````

No import changes are required — `import singer` keeps working. See the
[package README](https://github.com/meltano/sdk/tree/main/packages/meltano-singer-python)
for the exact supported surface and known differences (no `Transformer`/`transform`, no
`--properties` flag, and the `singer.metrics` logger includes a `pid` tag).

```{warning}
Because this package also provides the top-level `singer` module, it cannot be
installed alongside `singer-python` or `pipelinewise-singer-python`.
```

## Relationship to the full SDK

`singer_sdk.singerlib` is a stable alias for `singer` from this package — the
Meltano Singer SDK depends on `meltano-singer-python` and re-exports it, rather than
maintaining a separate implementation. If you later decide to port your tap or target
to the full SDK, the `singer_sdk.singerlib` names you'll find there (`Catalog`,
`RecordMessage`, `resolve_schema_references`, etc.) are the same objects you were
already using through `singer`.
