# Running your package with Meltano

The cookiecutter templates come with a `meltano.yml` for you to try your package with Meltano. To use it, you'll have to declare settings and their types.

For example:

```yaml
plugins:
  extractors:
  - name: my-tap
    namespace: my_tap
    executable: ./my-tap.sh
    capabilities:
    - state
    - catalog
    - discover
    settings:
    - name: base_url
      kind: string
    - name: api_key
      kind: password
```

Here's a summary of `poetry` commands and their Meltano equivalents.

|                     |                          `meltano`                           | `poetry`                                                     |
| ------------------- | :----------------------------------------------------------: | ------------------------------------------------------------ |
| Configuration store | `meltano.yml`, `.env`, environment variables, or Meltano's system db | Config JSON file (`--config=path/to/config.json`) or environment variables (`--config=ENV`) |
| Simple invocation   |                   `meltano invoke my-tap`                    | `poetry run my-tap --config=...`                             |
| Other CLI options   |        `meltano invoke my-tap --about --format=json`         | `poetry run my-tap --about --format=json`                    |
| ELT                 |              `meltano elt my-tap target-jsonl`               | `poetry run my-tap --config=... \| path/to/target-jsonl --config=...` |
