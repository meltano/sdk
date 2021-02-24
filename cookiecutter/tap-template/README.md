_**NOTE:** This framework is still in early exploration and development phases. For more
information and to be updated on this project, please feel free to subscribe to our
[original Meltano thread](https://gitlab.com/meltano/meltano/-/issues/2401) and the
[initial PR for the underlying framework](https://gitlab.com/meltano/tap-base/-/merge_requests/1)._

--------------------------------

# {{cookiecutter.tap_name}} Singer Tap


To use this cookie cutter template:

```bash
pip3 install pipx
pipx ensurepath
pipx install cookiecutter
```

Initialize Cookiecutter template directly from Git:

```bash
cookiecutter https://gitlab.com/meltano/singer-sdk --directory="cookiecutter/tap-template"
```

Or locally from a cloned `singer-sdk` repo:

```bash
cookiecutter ./singer-sdk/cookiecutter/tap-template
```

See the [dev guide](https://gitlab.com/meltano/tap-base/-/blob/feature/initial-base-classes/docs/dev_guide.md).
