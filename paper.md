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

`PyleoTUPS` is a Python package designed to support the discovery, retrieval, and inspection of paleoclimate data archived in the US National Oceanic and Atmospheric Administration (NOAA) National Centers for Environmental Information (NCEI) for Paleoclimatology and PANGAEA. These repositories are the two main archives used by the paleoclimate community, but they differ in their search interfaces, metadata models, and data organization. `PyleoTUPS` provides repository-specific dataset objects, `NOAADataset` and `PangaeaDataset`, that expose a common set of methods for searching records, retrieving dataset-level metadata, and loading associated data tables. Results are returned as `pandas.DataFrame` objects that can be inspected, documented, and passed to downstream paleoclimate workflows. By harmonizing access patterns while preserving scientifically meaningful repository-specific metadata, `PyleoTUPS` supports reproducible cross-repository data discovery for paleoclimate synthesis, data-model comparison, climate field reconstruction, and related research applications.

# Statement of need

NOAA NCEI for Paleoclimatology and PANGAEA are the two main repositories used worldwide by the paleoclimate community to archive and access data. NOAA NCEI for Paleoclimatology is dedicated to paleoclimate data, whereas PANGAEA is a broader data repository that includes many Earth and environmental science datasets. Some datasets routinely used in paleoclimate studies are archived in both repositories, while others are available in only one. As a result, researchers often need to search and retrieve data from both repositories, particularly for synthesis efforts that compile hundreds of records across regions, archive types, proxy systems, or time intervals.

Accessing data across these repositories is not only a technical issue, but also a scientific reproducibility challenge. Paleoclimate scientists routinely need existing records to compare with new observations, build regional or global syntheses, generate climate field reconstructions, or address scientific questions that require compilations of many records, such as changes in climate variability or tipping points in the climate system. Differences in repository search interfaces, metadata models, and returned search results can therefore affect which records are identified, how complete a compilation is, and whether the search and retrieval process can be transparently documented and reproduced.

These differences are particularly important because NOAA NCEI for Paleoclimatology and PANGAEA expose paleoclimate-relevant information in different ways. NOAA NCEI for Paleoclimatology includes paleoclimate-oriented metadata, including information supported by the NOAA NOAA Paleoenvironmental Standard Terms (PaST) Thesaurus [@Morrill2021], which enables more precise searches for variables and related metadata. PANGAEA, by contrast, is a general-purpose Earth and environmental science repository, so some paleoclimate-relevant concepts are not always represented in directly searchable form. For example, time information exposed through repository search may not correspond to the paleoclimate time range needed to determine whether a record is relevant to a particular study. In such cases, researchers may need to retrieve and inspect the associated data tables before evaluating the record.

PyleoTUPS fills the gap between repository-level data discovery and analysis-ready paleoclimate workflows by providing consistent Python access to NOAA NCEI for Paleoclimatology and PANGAEA while preserving scientifically meaningful differences in their metadata models. The package harmonizes common access patterns, including search parameters and functions for retrieving publications, geographic metadata, data tables, and variable-level metadata. At the same time, it avoids forcing repository outputs into a single semantic model when doing so could obscure important archival or scientific context. The returned information is organized as pandas.DataFrame objects, making it easier to inspect, filter, document, and pass to downstream analysis workflows.


# Implementation

`PyleoTUPS` is built on top of the NOAA NCEI for Paleoclimatology API, whose documentation is available on [their website](https://www.ncei.noaa.gov/access/paleo-search/api) [@Morrill2021], and the Python `pangaeapy` library [@pangaeapy], which supports retrieving data and metadata from PANGAEA.

The package currently provides two primary dataset interfaces: `NOAADataset` and `PangaeaDataset`. These objects encapsulate repository-specific access methods while exposing a common set of user-facing functions. Through these interfaces, users can perform search queries, retrieve metadata, including associated publications, geographic information, variable-level descriptions, and temporal coverage, and load tabular data into Python data structures suitable for further analysis.

For NOAA NCEI for Paleoclimatology records, `PyleoTUPS` combines API-based discovery with table parsing. Search queries are submitted through the NOAA API, which returns study-level metadata in JSON format. `PyleoTUPS` uses the returned metadata to identify the associated paleoclimate data tables, then parses those tables into `pandas.DataFrame` objects. For PANGAEA records, `PyleoTUPS` builds on `pangaeapy` [@pangaeapy] while providing a more consistent, paleoclimate-oriented Python interface for constructing search queries. This interface abstracts over repository-specific search fields and parameters, allowing users to specify search terms programmatically and retrieve matching records through a workflow and parameter structure similar to those used for NOAA records.

Both dataset objects provide a `get_summary` method that retrieves and flattens dataset-level metadata commonly needed in paleoclimate workflows. These summaries include information such as geographic location, publication metadata, variable descriptions, and temporal coverage. Because NOAA NCEI for Paleoclimatology and PANGAEA represent these fields differently, `PyleoTUPS` retrieves them through repository-specific logic. For example, temporal coverage is available directly from NOAA metadata, whereas for PANGAEA records it must be inferred from the associated data tables.

Because NOAA NCEI for Paleoclimatology and PANGAEA differ in their metadata models, search interfaces, and data organization, `PyleoTUPS` intentionally avoids fully homogenizing their outputs. Instead, the package provides a common set of user-facing methods, including `get_publications`, `get_geo`, `get_data`, and `get_summary`, while preserving repository-specific structures when those differences carry scientific or archival meaning. The accompanying tutorials [@pyleotups_tutorials] document these schema differences and demonstrate how to build repository-aware workflows.

# Research Applications

In addition to the minimal working examples in the documentation, more comprehensive tutorials [@pyleotups_tutorials] are available as a [Jupyter Book](https://linked.earth/pyleotupsTutorials/). These tutorials explain the schema differences between the NOAA NCEI for Paleoclimatology and PANGAEA databases, introduce the `NOAADataset` and `PangaeaDataset` objects and their functionalities, illustrate how to perform search queries on these databases, and provide scientific use cases of the toolbox. The scientific use cases were identified by the community. We anticipate a growing number of these use cases as the toolbox becomes more widely adopted. 

`PyleoTUPS` can be used in several common paleoclimate research workflows. For data discovery, users can query public repositories and inspect returned metadata. For synthesis studies, the package can help researchers retrieve comparable records across regions, archives, or proxy systems. For reproducible publications, `PyleoTUPS` can be embedded in notebooks that document how datasets were discovered, filtered, and loaded. The package returns information as pandas.DataFrame objects, making it interoperable with the broader scientific Python ecosystem, including paleoclimate analysis packages such as Pyleoclim [@Khider2022] and cfr [@Zhu2024].

# Availability

`PyleoTUPS` is an open-source software released under the Apache 2.0 license and is actively maintained as part of the LinkedEarth project. It is available through [PyPi](https://pypi.org/project/pyleotups/) and [GitHub](https://github.com/LinkedEarth/PyleoTUPS). Documentation is available through [readthedocs](https://pyleotups.readthedocs.io/en/latest/). 

# AI usage disclosure

AI code completion was used to help generate API documentation and write unit tests. Generative AI tools, including Claude and ChatGPT, were used to help draft the manuscript and tighten the prose in the tutorials. All tutorial code was written manually by the authors to test and demonstrate scientific use of the package.


# Acknowledgements

The authors were supported by NSF Award #2411267 and #2411268. We thank Dr Georgina Falster at The University of Adelaide for testing the alpha version of this software and providing feedback on the early implementation. 


# References