
import os, sys

# Add the parent directory (package folder) to sys.path so that 'pytups' is importable.
package_path = os.path.abspath('..')
sys.path.insert(0, package_path)
os.environ['PYTHONPATH'] = ':'.join((package_path, os.environ.get('PYTHONPATH', '')))

# Now import pytups safely.
import pytups

project = 'PyTUPS'
copyright = '2025, Dhiren Oswal, Deborah Khider, Jay Pujara'
author = 'Dhiren Oswal, Deborah Khider, Jay Pujara'
release = pytups.__version__
version = pytups.__version__


autodoc_mock_imports = ["_tkinter"]
# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.napoleon',
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.viewcode',
    'sphinx.ext.coverage',
    'sphinx.ext.mathjax',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'matplotlib.sphinxext.plot_directive',
    'numpydoc',
    'nbsphinx',
    # 'IPython.sphinxext.ipython_console_highlighting',
    # 'IPython.sphinxext.ipython_directive',
    # 'sphinx_search.extension',
    # 'jupyter_sphinx',
    # 'sphinx_copybutton'
]

# mathjax_config = {
#     'tex2jax': {
#         'inlineMath': [ ["\\(","\\)"] ],
#         'displayMath': [["\\[","\\]"] ],
#     },
# }

# mathjax3_config = {
#   "tex": {
#     "inlineMath": [['\\(', '\\)']],
#     "displayMath": [["\\[", "\\]"]],
#   }
# }

# plot_include_source = True
# plot_formats = [("png", 90)]
# plot_html_show_formats = True
# plot_html_show_source_link = True
autosummary_generate = True
numpydoc_show_class_members = False

# templates_path = ['_templates']
# exclude_patterns = []

version = pytups.__version__
# The full version, including alpha/beta/rc tags.
release = pytups.__version__

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'

htmlhelp_basename = 'Pytupsdoc'
html_static_path = ['_static']
master_doc = 'index'
# man_pages = [
#     (master_doc, 'pyleoclim', 'Pyleoclim Documentation',
#      [author], 1)
# ]