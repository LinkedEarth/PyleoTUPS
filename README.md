# NOAAStudies Module
Welcome to the NOAAStudies module, a Python package designed to interface with the NOAA (National Centers for Environmental Information) database for paleoclimatology studies. This guide will walk you through setting up and using the NOAAStudies module to access and analyze climate study data effectively.

### Configuration

No initial configuration is necessary to start using the module with the default settings. The module uses pre-defined endpoints to interact with the NOAA API.

## Usage

This section will guide you through basic operations you can perform with the NOAAStudies module.

### Searching for Studies

To search for paleoclimatology studies by various parameters, you can use the `search_studies` method. Here's an example of how to search for studies based on the investigator's name:

```python
from NOAAStudies import NOAAStudies

# Create an instance of the NOAAStudies class
noaa = NOAAStudies()

# Search for studies where 'khider' is an investigator
noaa.search_studies(investigators='khider')

# Display the results
print(noaa.display_responses())
```

### Displaying Publications

If you're interested in the publications of a specific study, you can retrieve and display them as follows:

```python
# Display publications for a specific study ID
publications_df = noaa.display_publications('some_study_id')
print(publications_df)
```

### Fetching Data from Sites

To access detailed information about sites within a study, use the `display_sites` method:

```python
# Display site details for a given study ID
sites_info_df = noaa.display_sites('some_study_id')
print(sites_info_df)
```

### Fetching Raw Data

If you need to download raw data files associated with a study's data table, the `get_data` method can be used:

```python
# Fetch data using a data table ID
data_df = noaa.get_data(dataTableID='some_data_table_id')
print(data_df)
