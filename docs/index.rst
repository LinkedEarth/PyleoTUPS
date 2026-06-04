.. PyleoTUPS documentation master file, created by
   sphinx-quickstart on Mon Mar 31 18:23:47 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

PyleoTUPS: Automated Paleoclimate Data Extraction and Processing
================================================================

PyleoTUPS is a Python package developed by LinkedEarth to simplify access to paleoclimate datasets from major public repositories. It provides a unified interface for searching, retrieving, and processing data from `NOAA NCEI for Paleoclimatology <https://www.ncei.noaa.gov/products/paleoclimatology>`_ and `PANGAEA <https://www.pangaea.de>`_, two repositories that differ substantially in structure, metadata organization, search syntax, and file formats.

The package is designed to reduce a common bottleneck in paleoclimate research: manually locating datasets, navigating repository-specific APIs, downloading files, parsing legacy formats, and reshaping results for analysis. PyleoTUPS handles these steps through a consistent Python API and returns results as pandas DataFrames with associated metadata.

**Key Features**:

**Unified Repository Interface**  
Work with NOAA NCEI Paleoclimatology and PANGAEA using similar methods and parameter names, including `search_studies()`, `get_summary()`, `get_publications()`, `get_funding()`, `get_geo()`, and `get_data()`.

**Multi-Repository Search**  
Search paleoclimate datasets by text, investigator, variable, geographic bounding box, and other criteria. PyleoTUPS translates common Python parameters into the appropriate query syntax for each repository.

**Automated NOAA Data Extraction**  
NOAA paleoclimate data are often stored in text, CSV, Excel, and legacy template formats. PyleoTUPS uses dedicated parsers to extract data tables from these files and preserve relevant metadata.

**PANGAEA Integration**  
PANGAEA datasets are accessed through their repository infrastructure, with PyleoTUPS providing a consistent workflow for searching datasets, retrieving metadata, and loading data tables.

**Consistent Data Outputs**  
Search results, metadata summaries, geographic information, publications, funding records, and measurement data are returned as pandas DataFrames, making them ready for analysis with the scientific Python ecosystem.

**Metadata Preservation**  
PyleoTUPS preserves dataset-level and column-level metadata where available, including information about study identifiers, locations, authors, publications, variables, units, and data sources.

**Support for FAIR Paleoclimate Workflows**  
The package supports reusable and interoperable paleoclimate data practices, including workflows that connect repository data to formats such as the `Linked PaleoData format (LiPD) <http://lipd.net>`_ format and `NOAA PaST Thesaurus vocabulary <https://www.ncei.noaa.gov/products/paleoclimatology>`_.

Typical Workflow
----------------

A typical PyleoTUPS workflow is:

1. Choose a data provider, such as `NOAADataset()` or `PangaeaDataset()`.
2. Search for studies using common parameters such as text, location, investigator, or variable name.
3. Review summaries, publications, funding sources, and geographic metadata.
4. Retrieve measurement data as pandas DataFrames.
5. Use the resulting data in downstream paleoclimate, statistical, or visualization workflows.

For example:

.. code-block:: python
   import pyleotups as pt

   ds = pt.NOAADataset()
   results = ds.search_studies(
      search_text="tree rings",
      min_lat=30,
      max_lat=40,
      limit=20
      )

   summary = ds.get_summary()
   data = ds.get_data("<dataset identifier>")


Getting Started
===============

.. toctree::
   :maxdepth: 1
   :caption: Working with PyleoTUPS

   installation.rst
   api.rst
   tutorials.rst

Install PyleoTUPS from PyPi using pip:
```bash
pip install pyleotups
```

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
