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

import os
import sys

sys.path.insert(0, os.path.abspath(".."))
# sys.path.insert(0, os.path.abspath("../singer_sdk"))
# sys.path.insert(0, os.path.abspath("/Users/ajsteers/Source/sdk"))


# -- Project information -----------------------------------------------------

project = "Meltano Singer SDK"
copyright = "2021, Meltano Core Team and Contributors"
author = "Meltano Core Team and Contributors"

# The full version, including alpha/beta/rc tags
release = "0.22.0"


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
    # branding
    "light_css_variables": {
        "color-brand-primary": "#3438bf",
        "color-brand-content": "#3438bf",
        # sidebar
        "color-sidebar-background": "#3438bf",
        "color-sidebar-item-background--hover": "#3438bf",
        "color-sidebar-brand-text": "white",
        "color-sidebar-caption-text": "white",
        "color-sidebar-link-text": "white",
        "color-sidebar-link-text--top-level": "white",
    },
    "dark_css_variables": {
        "color-brand-primary": "#3438bf",
        "color-brand-content": "#3438bf",
        # siderbar
        "color-sidebar-background": "#3438bf",
        "color-sidebar-item-background--hover": "#3438bf",
    },
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

myst_heading_anchors = 3

redirects = {
    "porting.html": "guides/porting.html",
}
