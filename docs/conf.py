# isort: dont-add-imports

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#

import sys
from pathlib import Path

sys.path.insert(0, str(Path("..").resolve()))


# -- Project information -----------------------------------------------------

project = "Meltano Singer SDK"
copyright = "2021, Meltano Core Team and Contributors"  # noqa: A001
author = "Meltano Core Team and Contributors"

# The full version, including alpha/beta/rc tags
release = "0.28.0"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.autosummary",
    "sphinx_copybutton",
    "myst_parser",
    "sphinx_reredirects",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Show typehints in the description, along with parameter descriptions
autodoc_typehints = "signature"
autodoc_class_signature = "separated"

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_logo = "_static/img/logo.svg"
html_theme = "furo"
html_theme_options = {
    # general
    "source_repository": "https://github.com/meltano/sdk/",
    "source_branch": "main",
    "source_directory": "docs/",
    "sidebar_hide_name": True,
    "announcement": '<a href"https://meltano.com/cloud/?utm_campaign=top_banner_sdk">Sign up for Public Beta today</a>! Get a 20% discount on purchases before 27th of July</a>!',
    # branding
    "light_css_variables": {
        "font-stack": "Hanken Grotesk,-apple-system,Helvetica,sans-serif",
        "color-announcement-background": "#3A64FA",
        "color-announcement-text": "#EEEBEE",
        "color-foreground-primary": "#080216",
        "color-background-primary": "#E9E5FB",
        "color-link": "#3A64FA",
        "color-link-underline": "transparent",
        "color-link--hover": "#3A64FA",
        "color-link-underline--hover": "#3A64FA",
        # brand
        "color-brand-primary": "#311772",
        "color-brand-content": "#311772",
        # sidebar
        "color-sidebar-background": "#311772",
        "color-sidebar-search-background": "#E9E5FB",
        "color-sidebar-item-background--hover": "#18c3fa",
        "color-sidebar-item-expander-background--hover": "#311772",
        "color-sidebar-brand-text": "white",
        "color-sidebar-caption-text": "rgba(255, 255, 255, 0.7)",
        "color-sidebar-link-text": "white",
        "color-sidebar-link-text--top-level": "white",
    },
    "dark_css_variables": {
        "color-background-primary": "#080216",
        "color-link": "#18c3fa",
        "color-link--hover": "#18c3fa",
        "color-link-underline--hover": "#18c3fa",
        # brand
        "color-brand-content": "rgba(255, 255, 255, 0.7)",
        # sidebar
        "color-sidebar-search-background": "#080216",
    },
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

html_css_files = [
    "css/custom.css",
]

myst_heading_anchors = 3

redirects = {
    "porting.html": "guides/porting.html",
}
