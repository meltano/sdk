"""Dev CLI."""

from __future__ import annotations

from os import mkdir
from pathlib import Path

import click
import yaml

from singer_sdk.helpers._meltano import meltano_yaml_str


@click.group()
def dev_cli() -> None:
    """Run the dev CLI."""


@dev_cli.command()
@click.option("--from-file", type=click.Path(exists=True), required=True)
@click.option("--out-dir", type=click.Path(exists=False), required=False)
def analyze(
    from_file: str,
    out_dir: str | None = None,
) -> None:
    """Print helpful information about a tap or target.

    To use this command, first save the tap or targets `--about` output using:
        `my-connector --about --format=json > my-connector.about.json`

    Currently the `analyze` command generates a single `

    Args:
        from_file: Source file, in YAML or JSON format (string).
        out_dir: Destination directory for generated files. If not specified, the
            working directory will be used.
    """
    json_info = yaml.load(Path(from_file).read_text(encoding="utf-8"), yaml.SafeLoader)
    connector_name = json_info["name"]
    meltano_yml = meltano_yaml_str(
        json_info["name"], json_info["capabilities"], json_info["settings"]
    )
    out_dir = out_dir or "."
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    for file_desc, out_path, file_text in [
        (
            "Meltano plugin definition",
            Path(out_dir) / f"{connector_name}.meltano.yml",
            meltano_yml,
        )
    ]:
        print(f"{file_desc}: {out_path}")
        Path(out_path).write_text(file_text, encoding="utf-8")


if __name__ == "__main__":
    dev_cli()
