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

The search_studies() method can take multiple arguments like: noaa_id, data_type_id, keywords, investigators, min_lat/max_lat, min_lon/max_lon, location, species, search_text

For this class the data_publisher is set to defaul as 'NOAA'

### Get Publications

If you're interested in viweing the publications of (a) specific study/studies, you can retrieve and display them as follows:

```python
# Display publications for a specific study ID
publications_df = noaa.get_publications(['some_study_id_1', 'some_study_id_2'])
print(publications_df)
```

If output_format == 'bibtex', the an BiblioGraphy Data object will be returned.


### Fetching Sites from Studies

To access detailed information about sites within a study/studies, use the `get_sites` method:

```python
# Display site details for a given study ID
sites_info_df = noaa.get_sites(['some_study_id_1', 'some_study_id_2'])
print(sites_info_df)
```

### Fetching the Paleo Data

If you need to access the paleo data files associated with a study's data table, the `get_data` method can be used:

```python
# Fetch data using a data table ID
data_df = noaa.get_data(dataTableID=['some_data_table_id_1', 'some_data_table_id_2'])
print(data_df)
```

If the url of file containing data is already known, the url can directly be passed using parameter 'file_urls' 
```python
# Fetch data using a data table ID
data_df = noaa.get_data(file_urls=[
    'https://www.ncei.noaa.gov/pub/data/paleo/paleocean/atlantic/rubbelke2023/rubbelke2023-leafwax-iso.txtl_1', 
    'https://www.ncei.noaa.gov/pub/data/paleo/contributions_by_author/khider2011/khider2011.txt'
    ])
print(data_df)
```