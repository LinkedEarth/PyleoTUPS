# pytups/__init__.py

# Importing key classes and functions to expose them at the package level
from .core import *
import pytups.utils as utils

# get the version
from importlib.metadata import version
__version__ = version('pytups')