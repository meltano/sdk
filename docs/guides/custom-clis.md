# Custom CLIs

## Overview

By default, packages created with the Singer SDK will have a single command, e.g. `tap-my-source`, which will run the application in a Singer-compatible way. However, you may want to add additional commands to your package. For example, you may want to add a command to initialize the database or platform with certain attributes required by the application to run properly.

## Adding a custom command

To add a custom command, you will need to add a new method to your plugin class that returns an instance of [`click.Command`](https://click.palletsprojects.com/en/8.1.x/api/#commands) (or a subclass of it) and decorate it with the `singer_sdk.cli.plugin_cli` decorator. Then you will need to add the command to the `[tool.poetry.scripts]` or `[project.scripts]` section of your `pyproject.toml` file.

```python
# tap_shortcut/tap.py

class ShortcutTap(Tap):
    """Shortcut tap class."""

    @plugin_cli
    def update_schema(cls) -> click.Command:
        """Update the OpenAPI schema for this tap."""
        @click.command()
        def update():
            response = requests.get(
                "https://developer.shortcut.com/api/rest/v3/shortcut.swagger.json",
                timeout=5,
            )
            with Path("tap_shortcut/openapi.json").open("w") as f:
                f.write(response.text)

        return update
```

````{tab} Poetry
```toml
# pyproject.toml

[tool.poetry.scripts]
tap-shortcut = "tap_shortcut.tap:ShortcutTap.cli"
tap-shortcut-update-schema = "tap_shortcut.tap:ShortcutTap.update_schema"
```
````

````{tab} uv
```toml
# pyproject.toml

[project.scripts]
tap-shortcut = "tap_shortcut.tap:ShortcutTap.cli"
tap-shortcut-update-schema = "tap_shortcut.tap:ShortcutTap.update_schema"
```
````
