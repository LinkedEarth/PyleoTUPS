# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import sys
import os
import pytups as pytups
# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
package_path = os.path.abspath('../..')
os.environ['PYTHONPATH']=':'.join(((package_path), os.environ.get('PYTHONPATH','')))
sys.path.insert(0,os.path.abspath('../pytups'))

autodoc_mock_imports = ["_tkinter"]
project = 'pytups'
copyright = '2025, Dhiren Oswal, Diborah Khider, Jay Pujara'
author = 'Dhiren Oswal, Diborah Khider, Jay Pujara'
release = '0.0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
]


templates_path = ['_templates']
exclude_patterns = []

language = 'English'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
