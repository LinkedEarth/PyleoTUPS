import requests
import re
import pandas as pd

def search_studies(params):
    BASE_URL = "https://www.ncei.noaa.gov/access/paleo-search/study/search.json"
    params = {k: v for k, v in params.items() if v}
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching studies: {response.status_code}")
    
def params_validators(self, key, value):
    """
    - Validations: 
        - minLon, maxLon, minLat, maxLat:
            Latitude Shall be in the -90 to +90 range
            Longitude shall be in the -180 to +180 range
        - Parse multiple inputs: 
            Implement parsing for more than one keyword, DataTypeId
        - species:
            Limit till 4 letter code
        - earliestYear/ latestYear:
        Validate Year

    """
    pass

def assert_list(input_item):
    """
    Ensure the input is a list. If it's not a list, convert it into a single-element list.

    Parameters:
        input_item: A single element or a list.

    Returns:
        list: A list containing the input item(s).
    """
    if isinstance(input_item, list):
        return input_item
    elif input_item is not None:
        return [input_item]
    else:
        return []

def get_citation_key(publication):
    """
    Generate a citation key for a publication.

    Parameters:
        publication (dict): A dictionary containing publication details.

    Returns:
        str: A unique citation key.
    """
    # Extract last name from the author field
    last_name = publication.get('author', 'Unknown Author').split()[-1]

    # Extract first significant word from the title
    title = publication.get('title', 'Unknown Title')
    words = re.findall(r'\w+', title)
    first_significant_word = next((word.capitalize() for word in words if len(word) > 2 and word.lower() != 'the'), "Unknown")

    # Get the year and NOAAStudyId
    pub_year = publication.get('year', 'Unknown Year')
    study_id = publication.get('NOAAStudyId', 'UnknownID')

    # Create citation key
    return f"{last_name}_{first_significant_word}_{pub_year}_{study_id}".replace(" ", "")

def fetch_data(file_url):
    """
    Helper method to fetch data from a file URL and return it as a DataFrame.
    """
    print(file_url)
    response = requests.get(file_url)
    if response.status_code == 200:
        lines = response.text.split('\n')
        data_lines = [line for line in lines if not line.startswith('#') and line.strip()]
        if data_lines:
            headers = data_lines[0].split('\t')
            data = [line.split('\t') for line in data_lines[1:]]
            return pd.DataFrame(data, columns=headers)
    print(f"Failed to fetch data from {file_url}.")
    return pd.DataFrame()