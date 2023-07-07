"""About information for a plugin."""

from __future__ import annotations

import abc
import dataclasses
import json
import typing as t
from collections import OrderedDict
from textwrap import dedent

if t.TYPE_CHECKING:
    from singer_sdk.helpers.capabilities import CapabilitiesEnum

__all__ = [
    "AboutInfo",
    "AboutFormatter",
    "JSONFormatter",
    "MarkdownFormatter",
]


@dataclasses.dataclass
class AboutInfo:
    """About information for a plugin."""

    name: str
    description: str | None
    version: str
    sdk_version: str
    supported_python_versions: list[str] | None

    capabilities: list[CapabilitiesEnum]
    settings: dict


class AboutFormatter(abc.ABC):
    """Abstract base class for about formatters."""

    formats: t.ClassVar[dict[str, type[AboutFormatter]]] = {}
    format_name: str

    def __init_subclass__(cls, format_name: str) -> None:
        """Initialize subclass.

        Args:
            format_name: Name of the format.
        """
        cls.formats[format_name] = cls
        super().__init_subclass__()

    @classmethod
    def get_formatter(cls, name: str) -> AboutFormatter:
        """Get a formatter by name.

        Args:
            name: Name of the formatter.

        Returns:
            A formatter.
        """
        return cls.formats[name]()

    @abc.abstractmethod
    def format_about(self, about_info: AboutInfo) -> str:
        """Render about information.

        Args:
            about_info: About information.
        """
        ...


class TextFormatter(AboutFormatter, format_name="text"):
    """About formatter for text output."""

    def format_about(self, about_info: AboutInfo) -> str:
        """Render about information.

        Args:
            about_info: About information.

        Returns:
            A formatted string.
        """
        return dedent(
            f"""\
            Name: {about_info.name}
            Description: {about_info.description}
            Version: {about_info.version}
            SDK Version: {about_info.sdk_version}
            Supported Python Versions: {about_info.supported_python_versions}
            Capabilities: {about_info.capabilities}
            Settings: {about_info.settings}""",
        )


class JSONFormatter(AboutFormatter, format_name="json"):
    """About formatter for JSON output."""

    def __init__(self) -> None:
        """Initialize a JSONAboutFormatter."""
        self.indent = 2
        self.default = str

    def format_about(self, about_info: AboutInfo) -> str:
        """Render about information.

        Args:
            about_info: About information.

        Returns:
            A formatted string.
        """
        data = OrderedDict(
            [
                ("name", about_info.name),
                ("description", about_info.description),
                ("version", about_info.version),
                ("sdk_version", about_info.sdk_version),
                ("supported_python_versions", about_info.supported_python_versions),
                ("capabilities", [c.value for c in about_info.capabilities]),
                ("settings", about_info.settings),
            ],
        )
        return json.dumps(data, indent=self.indent, default=self.default)


class MarkdownFormatter(AboutFormatter, format_name="markdown"):
    """About formatter for Markdown output."""

    def format_about(self, about_info: AboutInfo) -> str:
        """Render about information.

        Args:
            about_info: About information.

        Returns:
            A formatted string.
        """
        max_setting_len = t.cast(
            int,
            max(len(k) for k in about_info.settings["properties"]),
        )

        # Set table base for markdown
        table_base = (
            f"| {'Setting':{max_setting_len}}| Required | Default | Description |\n"
            f"|:{'-' * max_setting_len}|:--------:|:-------:|:------------|\n"
        )

        # Empty list for string parts
        md_list = []
        # Get required settings for table
        required_settings = about_info.settings.get("required", [])

        # Iterate over Dict to set md
        md_list.append(
            f"# `{about_info.name}`\n\n"
            f"{about_info.description}\n\n"
            f"Built with the [Meltano Singer SDK](https://sdk.meltano.com).\n\n",
        )

        # Process capabilities and settings

        capabilities = "## Capabilities\n\n"
        capabilities += "\n".join([f"* `{v}`" for v in about_info.capabilities])
        capabilities += "\n\n"
        md_list.append(capabilities)

        setting = "## Settings\n\n"

        for k, v in about_info.settings.get("properties", {}).items():
            md_description = v.get("description", "").replace("\n", "<BR/>")
            table_base += (
                f"| {k}{' ' * (max_setting_len - len(k))}"
                f"| {'True' if k in required_settings else 'False':8} | "
                f"{v.get('default', 'None'):7} | "
                f"{md_description:11} |\n"
            )

        setting += table_base
        setting += (
            "\n"
            + "\n".join(
                [
                    "A full list of supported settings and capabilities "
                    f"is available by running: `{about_info.name} --about`",
                ],
            )
            + "\n"
        )
        setting += "\n"
        md_list.append(setting)

        # Process Supported Python Versions

        if about_info.supported_python_versions:
            supported_python_versions = "## Supported Python Versions\n\n"
            supported_python_versions += "\n".join(
                [f"* {v}" for v in about_info.supported_python_versions],
            )
            supported_python_versions += "\n"
            md_list.append(supported_python_versions)

        return "".join(md_list)
