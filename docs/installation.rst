.. _installation:

.. note::

   PyleoTUPS requires the use of Python 3.11 or above. We recommend using Python 3.12 to work with `Pyleoclim <https://pyleoclim-util.readthedocs.io/en/latest/>`_. 

Installing PyleoTUPS
====================

PyleoTUPS can be easily installed through `PyPI <https://pypi.org/project/pyleotups/>`_. We recommend using a dedicated Anaconda or Miniconda environment to manage dependencies.

Creating an environment and installing PyleoTUPS
------------------------------------------------

Before installing PyleoTUPS, we recommend creating a dedicated conda environment:

.. code-block:: bash

  conda create --name pyleotups python=3.12
  conda activate pyleotups

Install PyleoTUPS through PyPI, which contains the most stable version of PyleoTUPS:

.. code-block:: bash

  pip install pyleotups

This will install the latest official release of PyleoTUPS. To install the development version, use:

.. code-block:: bash

  pip install git+https://github.com/LinkedEarth/pyleotups.git

This version may contain bugs not caught by our continuous integration test suite; if so, please report them via `github issues <https://github.com/LinkedEarth/pyleotups/issues>`_.

If you wish to contribute to PyLiPD, see our `contributing guide <https://pyleotups.readthedocs.io/en/latest/contribution_guide.html>`_ for complete instructions on building from the git source tree.

To remove the conda environment:

.. code-block:: bash

  conda remove --name pyleotups --all

More information about managing conda environments can be found `here <https://docs.conda.io/en/latest/user-guide/tasks/manage-environments.html>`_.

Running the test suite
-----------------------

PyleoTUPS comes with a set of unit tests. To run these, you need to install `pytest` in the same environment as PyleoTUPS via `pip install pytest`. To run the tests from a Python terminal, navigate to the tests folder on your computer and run:

.. code-block:: bash

  pytest

Dependencies
-------------

PyleoTUPS depends on several Python packages:
 * pandas
 * numpy
 * requests
 * pybtex
 * tqdm
 * nbstripout
 * xlrd
 * openpyxl
 * pangaeapy
 * bibtextparser
 * doi2bib

These dependencies will be automatically installed when you install PyleoTUPS.

