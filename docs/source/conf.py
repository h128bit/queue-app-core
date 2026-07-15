import os
import sys

sys.path.insert(0, os.path.abspath('../../src')) 

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'queue_app_core'
copyright = '2026, h128bit'
author = 'h128bit'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',  
    'sphinx.ext.viewcode',  
]

templates_path = ['_templates']
exclude_patterns = []

autoclass_content = 'both' 


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output


html_theme = 'furo'

html_theme_options = {
    "sidebar_hide_name": False,
    "light_css_variables": {
        "color-brand-primary": "#3367bb",
        "color-brand-content": "#1d85d0",
    },
    "dark_css_variables": {
        "color-brand-primary": "#121c86",
        "color-brand-content": "#18739a",
    },
}

html_static_path = ['_static']


autodoc_mock_imports = [
    "dotenv",
    "pika",
    "aio_pika",
    "yarl",
    "requests",
    "tenacity",
]
