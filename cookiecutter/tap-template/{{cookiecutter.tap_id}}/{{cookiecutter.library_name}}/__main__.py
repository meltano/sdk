"""{{ cookiecutter.source_name }} entry point."""

from __future__ import annotations

from {{ cookiecutter.library_name }}.tap import Tap{{ cookiecutter.source_name }}

Tap{{ cookiecutter.source_name }}.cli()
