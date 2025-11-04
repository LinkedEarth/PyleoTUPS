.. PyleoTUPS documentation master file, created by
   sphinx-quickstart on Mon Mar 31 18:23:47 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PyleoTUPS: Automated Paleoclimate Data Extraction and Processing
================================================================

PyleoTUPS is a Python package designed to streamline paleoclimate data workflows by automating the extraction and processing of datasets from major paleoclimate repositories. The package addresses a critical bottleneck in paleoclimate research: the time-consuming manual process of accessing, extracting, and formatting data from diverse file formats and repositories.

**Key Features**:

* **Automated Data Extraction**: Leverages table understanding techniques to automatically extract data tables from complex text files, including NOAA Paleoclimate templates that have evolved over decades with varying formats and structures.
* **Multi-Repository Access**: Provides unified access to datasets from two major paleoclimate repositories - NOAA NCEI Paleoclimate and PANGAEA (coming soon!) - through their respective APIs and direct file processing capabilities.
* **Format Flexibility**: Handles multiple input formats including structured text files, CSV, and Excel files, automatically parsing embedded metadata and data tables regardless of template variations.
* **Scientific Python Integration**: Returns extracted data as pandas DataFrames with preserved metadata attributes, ensuring seamless integration with the broader Python scientific ecosystem including NumPy, SciPy, and specialized paleoclimate libraries.
* **Metadata Preservation**: Maintains comprehensive metadata linkage, storing dataset-level information (location, authors, publications) as dictionaries while preserving column-level metadata as DataFrame attributes.
* **FAIR Data Compliance**: Supports community standards for Findable, Accessible, Interoperable, and Reusable (FAIR) data practices, with built-in compatibility with the `Linked PaleoData format (LiPD) <http://lipd.net>`_ format and `NOAA PaST Thesaurus vocabulary <https://www.ncei.noaa.gov/products/paleoclimatology>`_.

Getting Started
===============

.. toctree::
   :maxdepth: 1
   :caption: Working with PyleoTUPS

   installation.rst
   api.rst
   tutorials.rst

The :ref:`PyleoTUPS APIs <api>` make use of specialized methods which are described in details in advanced functionalities. 

.. toctree::
   :maxdepth: 1
   :caption: Advanced Functions

   advanced.rst

Getting Involved
================

.. toctree::
   :Hidden:
   :caption: Getting Involved
   :maxdepth: 1

   contribution_guide.rst
   citation.rst

PyleoTUPS has been made freely available under the terms of the `Apache 2.0 License <https://github.com/LinkedEarth/PyleoTUPS/blob/master/license>`_, and follows an open development model.
There are many ways to get :ref:`involved in the development of PyleoTUPS <contributing_to_pyleotups>`:

  * If you write a paper making use of PyleoTUPS, please cite it :ref:`thus <citing_PyleoTUPS>`.
  * Report bugs and problems with the code or documentation to our `GitHub repository <https://github.com/LinkedEarth/PyleoTUPS/issues>`_. Please make sure that there is not outstanding issues that cover the problem you're experiencing.
  * Contribute bug fixes
  * Contribute enhancements and new features
  * Contribute to the code documentation, and share your PyleoTUPS-supported scientific workflow as a (`PaleoBook <http://linked.earth/PaleoBooks/>`_).


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
