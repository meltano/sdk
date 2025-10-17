# SDK Target Template

To use this cookie cutter template:

```bash
# Install uv (see https://docs.astral.sh/uv/getting-started/installation/ for more options)
curl -LsSf https://astral.sh/uv/install.sh | sh
# You may need to reopen your shell at this point
uv tool install cookiecutter
```

Initialize Cookiecutter template directly from Git:

```bash
cookiecutter https://github.com/meltano/sdk --directory="cookiecutter/target-template"
```

Or locally from an already-cloned `sdk` repo:

```bash
cookiecutter ./sdk/cookiecutter/target-template
```

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html).
