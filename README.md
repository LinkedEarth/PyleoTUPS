[![PyPI version](https://badge.fury.io/py/pyleotups.svg)](https://badge.fury.io/py/pyleotups)
[![license](https://img.shields.io/github/license/linkedearth/PyleoTUPS.svg)]()
[![NSF-2411267](https://img.shields.io/badge/NSF-2411267-blue.svg)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2411267)
[![DOI](https://zenodo.org/badge/869071109.svg)](https://doi.org/10.5281/zenodo.16009164)


<p align="center">
<img src="https://github.com/LinkedEarth/Logos/blob/master/PyleoTUPS/pyleotups_logo.png?raw=true" width="50%">
</p>


# PyleoTUPS: Automated Paleoclimate Data Extraction and Processing

PyleoTUPS is a Python package designed to streamline paleoclimate data workflows by automating the extraction and processing of datasets from major paleoclimate repositories. The package addresses a critical bottleneck in paleoclimate research: the time-consuming manual process of accessing, extracting, and formatting data from diverse file formats and repositories.

## Key Features

* **Automated Data Extraction**: Leverages table understanding techniques to automatically extract data tables from complex text files, including NOAA Paleoclimate templates that have evolved over decades with varying formats and structures.
* **Multi-Repository Access**: Provides unified access to datasets from two major paleoclimate repositories - NOAA NCEI Paleoclimate and PANGAEA (coming soon!) - through their respective APIs and direct file processing capabilities.
* **Format Flexibility**: Handles multiple input formats including structured text files, CSV, and Excel files, automatically parsing embedded metadata and data tables regardless of template variations.
* **Scientific Python Integration**: Returns extracted data as pandas DataFrames with preserved metadata attributes, ensuring seamless integration with the broader Python scientific ecosystem including NumPy, SciPy, and specialized paleoclimate libraries.
* **Metadata Preservation**: Maintains comprehensive metadata linkage, storing dataset-level information (location, authors, publications) as dictionaries while preserving column-level metadata as DataFrame attributes.
* **FAIR Data Compliance**: Supports community standards for Findable, Accessible, Interoperable, and Reusable (FAIR) data practices, with built-in compatibility with the [Linked Paleo Data (LiPD)](https://lipd.net) format and [NOAA PaST Thesaurus vocabulary](https://www.ncei.noaa.gov/products/paleoclimatology/paleoenvironmental-standard-terms-thesaurus).

## Target Users

PyleoTUPS is designed for paleoclimate researchers, Earth system modelers, and data scientists working with paleoclimate observations. Whether you're conducting systematic data synthesis, model evaluation, or exploratory analysis, PyleoTUPS reduces the technical barriers to accessing and utilizing paleoclimate datasets.

## Versions

See our [releases page](https://github.com/LinkedEarth/PyleoTUPS/releases) for details on what's included in each version.


## Development

PyleoTUPS development takes place on GitHub: https://github.com/LinkedEarth/PyleoTUPS

Please submit any reproducible bugs you encounter to the [issue tracker](https://github.com/LinkedEarth/PyleoTUPS/issues). For usage questions, please use [Discourse](https://discourse.linked.earth).


## License

The project is licensed under the Apache 2.0 license. Please refer to the file call license.
If you use the code in publications, please credit the work using the citation file. 


### Disclaimer

This material is based upon work supported by the National Science Foundation under Grant Number CSSI-2411267. Any opinions, findings, and conclusions or recommendations expressed in this material are those of the investigators and do not necessarily reflect the views of the National Science Foundation.

