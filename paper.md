---
title: 'PyleoTUPS: Automated Paleoclimate Data Extraction and Processing'
tags:
  - Python
  - paleoclimatology
  - data handling
authors:
  - name: Dhiren Oswal
    orcid: 0009-0001-2495-2626
    affiliation: "1, 2" # (Multiple affiliations must be quoted)
  - name: Deborah Khider
    corresponding: true
    orcid: 0000-0001-7501-8430
    affiliation: 1
  - name: Jay Pujara
    orcid: 0000-0001-6921-1744
    affiliation: "1, 2"
  - name: Nicholas McKay
    orcid: 0000-0003-3598-5113
    affiliation: 3
  - name: David Edge
    orcid: 0000-0001-6938-2850
    affiliation: 3
affiliations:
 - name: Information Sciences Institute, University of Southern California
   index: 1
 - name: Department of Computer Science, University of Southern California 
   index: 2
 - name: School of Earth and Sustainability, Northern Arizona University
   index: 3
date: 23 June 2026
bibliography: paper.bib

---

# Summary

# Summary

Paleoclimate researchers use measurements from natural archives, such as tree rings, corals, ice cores, and lake or marine sediments, to study how Earth’s climate varied before the instrumental period. Many of these datasets are archived in the US National Oceanic and Atmospheric Administration (NOAA) National Centers for Environmental Information (NCEI) for Paleoclimatology and PANGAEA, the two main repositories used worldwide by the paleoclimate community. These repositories are essential resources, but they differ in their search interfaces, metadata models, and data organization.

`PyleoTUPS` is a Python package designed to support the discovery, retrieval, and inspection of paleoclimate data across these repositories. The package provides repository-specific dataset objects, `NOAADataset` and `PangaeaDataset`, that expose a common set of methods for searching records, retrieving dataset-level metadata, and loading associated data tables. Results are returned as `pandas.DataFrame` objects that can be inspected, documented, and passed to downstream paleoclimate workflows. By harmonizing access patterns while preserving scientifically meaningful repository-specific metadata, `PyleoTUPS` supports reproducible cross-repository data discovery for paleoclimate synthesis, data-model comparison, climate field reconstruction, and related research applications.

# Statement of need

# Statement of need

NOAA NCEI for Paleoclimatology and PANGAEA are the two main repositories used worldwide by the paleoclimate community to archive and access data. NOAA NCEI for Paleoclimatology is dedicated to paleoclimate data, whereas PANGAEA is a broader Earth and environmental science repository. Because datasets routinely used in paleoclimate research may be archived in either or both repositories, researchers often need to search across both systems, particularly for synthesis efforts that compile hundreds of records across regions, archive types, proxy systems, or time intervals.

This cross-repository access problem affects more than software convenience. Paleoclimate scientists use existing records to compare with new observations, build syntheses, generate climate field reconstructions, and address questions that require large compilations, such as changes in climate variability or tipping points in the climate system. Differences in repository search interfaces, metadata models, and returned search results can affect which records are identified, how complete a compilation is, and whether the search process can be transparently documented and reproduced.

`PyleoTUPS` fills the gap between repository-level discovery and analysis-ready paleoclimate workflows by providing consistent Python access to NOAA NCEI for Paleoclimatology and PANGAEA. The package harmonizes common access patterns, including search parameters and functions for retrieving publications, geographic metadata, data tables, and variable-level metadata, while preserving repository-specific information when scientific meaning would be lost through full homogenization. Results are returned as `pandas.DataFrame` objects that can be inspected, filtered, documented, and passed to downstream analysis workflows.

# State of the field

Researchers can currently access NOAA NCEI for Paleoclimatology through its web interface and API [@Morrill2021], and PANGAEA through its web interface and tools such as `pangaeapy` [@pangaeapy]. These resources provide authoritative access to repository holdings, but they are repository-specific and expose different search parameters, metadata structures, and data-return formats. As a result, researchers who need records from both repositories often rely on custom scripts to discover records, retrieve metadata, and load associated data tables. These scripts address the first step in many paleoclimate workflows: finding and extracting relevant records from public archives.

Once records have been identified and retrieved, they may need to be curated into a standardized representation before they can be reused in synthesis or analysis workflows. The Linked Paleo Data format (LiPD) [@McKay2016] and `PyLiPD` [@Ratnakar2025] address this later step. LiPD provides an intermediate archival format in which paleoclimate data and metadata can be normalized, aligned with controlled vocabularies, and represented consistently across datasets. `PyLiPD` provides Python tools for reading, querying, editing, and writing datasets once they have been encoded in this format. `PyleoTUPS` operates upstream of this standardization step by helping researchers discover and retrieve records from NOAA NCEI for Paleoclimatology and PANGAEA before manual curation or transformation into LiPD.

Other packages, such as `Pyleoclim` [@Khider2022] and `cfr` [@Zhu2024], support downstream paleoclimate analysis, including time-series analysis and climate field reconstruction. These tools require data that have already been discovered, retrieved, and organized into analysis-ready structures. `PyleoTUPS` complements them by providing the repository discovery and retrieval layer needed to build those inputs reproducibly.

The decision to build `PyleoTUPS` rather than contribute only to `pangaeapy`, `PyLiPD`, or a NOAA-specific wrapper reflects this gap in the current software ecosystem. Existing tools support repository-specific access, standardized archival representation, or downstream analysis. `PyleoTUPS` provides a paleoclimate-oriented access layer that exposes similar search and retrieval patterns across the two main repositories while preserving scientifically meaningful differences in their metadata models.


# Software design

`PyleoTUPS` is built on top of the NOAA NCEI for Paleoclimatology API [@Morrill2021] and the Python `pangaeapy` library [@pangaeapy]. The package provides two primary dataset interfaces, `NOAADataset` and `PangaeaDataset`, that encapsulate repository-specific access methods while exposing a common set of user-facing functions. This two-object design reflects an explicit trade-off: `PyleoTUPS` harmonizes search and retrieval workflows where possible, but avoids forcing NOAA NCEI for Paleoclimatology and PANGAEA records into a single semantic model when doing so could obscure scientifically meaningful metadata differences.

For NOAA records, search queries are submitted through the NOAA API, which returns study-level metadata in JSON format. `PyleoTUPS` uses these metadata to identify associated paleoclimate data tables, then parses the tables into `pandas.DataFrame` objects. For PANGAEA records, `PyleoTUPS` builds on `pangaeapy` while providing a paleoclimate-oriented Python interface for constructing search queries and retrieving matching records through a workflow similar to the NOAA interface.

Both dataset objects provide common methods, including `get_publications`, `get_geo`, `get_data`, and `get_summary`. The `get_summary` method retrieves and flattens dataset-level metadata commonly needed in paleoclimate workflows, including geographic information, publication metadata, variable descriptions, and temporal coverage. Because these fields are represented differently across repositories, the implementation uses repository-specific logic. For example, temporal coverage is available directly from NOAA metadata, whereas for PANGAEA records it must be inferred from the associated data tables. Outputs are returned as `pandas.DataFrame` objects, preserving repository-specific columns while supporting inspection, filtering, documentation, and interoperability with downstream Python workflows.

# Research impact statement

`PyleoTUPS` is released through [PyPI](https://pypi.org/project/pyleotups/) and [GitHub](https://github.com/LinkedEarth/PyleoTUPS) and is accompanied by [public documentation](https://pyleotups.readthedocs.io/en/latest/) and a tutorial Jupyter Book [@pyleotups_tutorials]. The tutorials include introductory material, examples for working with `NOAADataset` and `PangaeaDataset` objects, and scientific workflows that demonstrate how repository data can be retrieved, inspected, and used in paleoclimate analyses. The package has been downloaded approximately 1.4k times from PyPI and is under active development, with issue-driven testing and refinement on GitHub.

Although `PyleoTUPS` is a new package, it is already being tested in an active paleoclimate data-compilation workflow. An undergraduate researcher with no prior experience in Python or paleoclimate science is using the package to help retrieve and transform repository data into LiPD format for an update to the PAGES2k compilation [@PAGES2k2017]. This ongoing use case suggests that `PyleoTUPS` can lower the technical barrier to cross-repository data access and support workflows that feed into community-scale paleoclimate products.

`PyleoTUPS` is also designed to connect repository discovery with downstream scientific software. The tutorials demonstrate interoperability with `Pyleoclim` [@Khider2022], and the same `pandas.DataFrame`-based outputs can support workflows using packages such as `cfr` [@Zhu2024]. The package is being integrated into PaleoPAL, an AI-assisted paleoclimate workflow system, where it will provide repository-level access to paleoclimate data for automated and reproducible analyses.

# AI usage disclosure

AI code completion was used to help generate API documentation and write unit tests. Generative AI tools, including Claude and ChatGPT, were used to help draft the manuscript and tighten the prose in the tutorials. All tutorial code was written manually by the authors to test and demonstrate scientific use of the package.


# Acknowledgements

The authors were supported by NSF Award #2411267 and #2411268. We thank Dr Georgina Falster at The University of Adelaide for testing the alpha version of this software and providing feedback on the early implementation. 


# References