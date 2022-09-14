# Singer Tap Template

To use this cookie cutter template:

```bash
pip3 install pipx
pipx ensurepath
# You may need to reopen your shell at this point
pipx install cookiecutter
```

Initialize Cookiecutter template directly from Git:

```bash
cookiecutter https://github.com/meltano/sdk --directory="cookiecutter/tap-template"
```

Or locally from an already-cloned `sdk` repo:

```bash
cookiecutter ./sdk/cookiecutter/tap-template
```

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html).
