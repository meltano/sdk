"""About information for a plugin."""

from __future__ import annotations

import abc
import dataclasses
import json
import typing as t
from textwrap import dedent

from packaging.specifiers import SpecifierSet
from packaging.version import Version

if t.TYPE_CHECKING:
    import sys

    from singer_sdk.helpers.capabilities import CapabilitiesEnum

    if sys.version_info >= (3, 10):
        from importlib.metadata import PackageMetadata
    else:
        from email.message import Message as PackageMetadata


__all__ = [
    "AboutFormatter",
    "AboutInfo",
    "JSONFormatter",
    "MarkdownFormatter",
]

# Keep these in sync with the supported Python versions in pyproject.toml
_PY_MIN_VERSION = 9
_PY_MAX_VERSION = 14


def _get_min_version(specifiers: SpecifierSet) -> int:
    min_version: list[int] = []
    for specifier in specifiers:
        if specifier.operator == ">=":
            min_version.append(Version(specifier.version).minor)
        if specifier.operator == ">":
            min_version.append(Version(specifier.version).minor + 1)
    return min(min_version, default=_PY_MIN_VERSION)


def _get_max_version(specifiers: SpecifierSet) -> int:
    max_version: list[int] = []
    for specifier in specifiers:
        if specifier.operator == "<=":
            max_version.append(Version(specifier.version).minor)
        if specifier.operator == "<":
            max_version.append(Version(specifier.version).minor - 1)
    return max(max_version, default=_PY_MAX_VERSION)


def get_supported_pythons(
    requires_python: str,
    *,
    classifiers: list[str] | None = None,
) -> t.Generator[str, None, None]:
    """Get the supported Python versions from a requires_python string and classifiers.

    Args:
        requires_python: The requires_python string from the package metadata.
        classifiers: The classifiers from the package metadata.

    Yields:
        A generator of supported Python versions.
    """
    if classifiers:
        yield from classifiers
        return

    specifiers = SpecifierSet(requires_python)
    min_version = _get_min_version(specifiers)
    max_version = _get_max_version(specifiers)

    yield from specifiers.filter(f"3.{v}" for v in range(min_version, max_version + 1))


def python_versions(package_metadata: PackageMetadata) -> list[str]:
    """Get the supported Python versions from a package metadata dictionary.

    Args:
        package_metadata: The package metadata dictionary.

    Returns:
        A list of supported Python versions.
    """
    requires_python = package_metadata.get("Requires-Python", f">={_PY_MIN_VERSION}")
    classifiers = [
        classifier.split("::")[-1].strip()
        for classifier in package_metadata.get_all("Classifier", [])
        if classifier.startswith("Programming Language :: Python ::")
    ]

    return list(
        get_supported_pythons(
            requires_python,
            classifiers=classifiers,
        )
    )


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
    env_var_prefix: str


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

    def format_about(self, about_info: AboutInfo) -> str:  # noqa: PLR6301
        """Render about information.

        Args:
            about_info: About information.

        Returns:
            A formatted string.
        """
        output = dedent(
            f"""\
            Name: {about_info.name}
            Description: {about_info.description}
            Version: {about_info.version}
            SDK Version: {about_info.sdk_version}"""
        )

        if about_info.supported_python_versions:
            output += "\nSupported Python Versions:\n"
            output += "\n".join(
                [f"  - {v}" for v in about_info.supported_python_versions]
            )

        output += "\nCapabilities:\n"
        output += "\n".join([f"  - {c}" for c in about_info.capabilities])

        output += "\nSettings:\n"
        for setting, schema in about_info.settings.get("properties", {}).items():
            env_var = about_info.env_var_prefix + setting.upper().replace("-", "_")
            json_type = schema.get("type")
            output += f"  - Name: {setting}\n"
            output += f"    Type: {json_type}\n"
            output += f"    Environment Variable: {env_var}\n"

        return output


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
        data = {
            "name": about_info.name,
            "description": about_info.description,
            "version": about_info.version,
            "sdk_version": about_info.sdk_version,
            "supported_python_versions": about_info.supported_python_versions,
            "capabilities": [c.value for c in about_info.capabilities],
            "settings": about_info.settings,
        }
        return json.dumps(data, indent=self.indent, default=self.default)


class MarkdownFormatter(AboutFormatter, format_name="markdown"):
    """About formatter for Markdown output."""

    def _generate_property_row(
        self,
        name: str,
        schema: dict[str, t.Any],
        *,
        required: bool,
        parent_name: str | None = None,
    ) -> t.Generator[str, None, None]:
        """Generate a row for this property and for nested properties, if any.

        Args:
            name: The property name.
            schema: The property schema.
            required: Whether the property is required to be present.
            parent_name: The parent property name, if any.

        Yields:
            One markdown table row for the setting, and one for each sub-property.
        """
        setting_name = f"{parent_name}.{name}" if parent_name else name
        md_description = schema.get("description", "").replace("\n", "<BR/>")
        yield (
            f"| {setting_name} "
            f"| {'True' if required else 'False'} "
            f"| {schema.get('default', 'None')} "
            f"| {md_description} |"
        )
        if "properties" in schema:
            yield from self._generate_property_rows(schema, parent_name=setting_name)

    def _generate_property_rows(
        self,
        schema: dict[str, t.Any],
        *,
        parent_name: str = "",
    ) -> t.Generator[str, None, None]:
        """Generate a row for each property in the schema.

        Args:
            schema: A JSON object schema.
            parent_name: The parent property name, if any.

        Yields:
            One markdown table row for each property.
        """
        required_settings = schema.get("required", [])
        for name, sub_schema in schema.get("properties", {}).items():
            yield from self._generate_property_row(
                name,
                sub_schema,
                required=name in required_settings,
                parent_name=parent_name,
            )

    def format_about(self, about_info: AboutInfo) -> str:
        """Render about information.

        Args:
            about_info: About information.

        Returns:
            A formatted string.
        """
        # Header
        output = dedent(f"""\
            # `{about_info.name}`\n
            {about_info.description}\n
            Built with the [Meltano Singer SDK](https://sdk.meltano.com).\n
        """)

        # Process capabilities
        output += "## Capabilities\n\n"
        output += "\n".join([f"- `{v}`" for v in about_info.capabilities])
        output += "\n\n"

        # Process Supported Python Versions
        if about_info.supported_python_versions:
            output += "## Supported Python Versions\n\n"
            output += "\n".join(
                [f"- {v}" for v in about_info.supported_python_versions],
            )
            output += "\n\n"

        # Process settings
        output += "## Settings\n\n"
        output += (
            "| Setting | Required | Default | Description |\n"
            "|:--------|:--------:|:-------:|:------------|\n"
        )
        output += "\n".join(self._generate_property_rows(about_info.settings))
        output += (
            "\n\n"
            + "\n".join(
                [
                    "A full list of supported settings and capabilities "
                    f"is available by running: `{about_info.name} --about`",
                ],
            )
            + "\n"
        )

        return output
