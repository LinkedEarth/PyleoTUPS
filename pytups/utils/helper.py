import requests
import re
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

def search_studies(params):
    """
    Perform a search for NOAA studies using specified parameters.

    Parameters
    ----------
    params : dict
        A dictionary of search parameters where keys are parameter names and
        values are their corresponding values.

    Returns
    -------
    dict
        A JSON response containing the results of the study search.

    Raises
    ------
    Exception
        If the HTTP request fails or returns a status code other than 200.
    """
    BASE_URL = "https://www.ncei.noaa.gov/access/paleo-search/study/search.json"
    params = {k: v for k, v in params.items() if v}
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching studies: {response.status_code}")
    
def params_validators(self, key, value):
    """
    Validate and process search parameters for NOAA studies.

    Validations
    -----------
    - Latitude and longitude:
        - `minLat` and `maxLat` must be within the range [-90, 90].
        - `minLon` and `maxLon` must be within the range [-180, 180].
    - Keywords and data types:
        - Parse multiple inputs for `keywords` and `dataTypeId`.
    - Species:
        - Ensure species codes are limited to 4 characters.
    - Year:
        - Validate `earliestYear` and `latestYear` for correctness.

    Parameters
    ----------
    key : str
        The name of the parameter to validate.
    value : Any
        The value of the parameter to validate.

    Returns
    -------
    None
    """
    pass

def assert_list(input_item):
    """
    Ensure the input is a list. If it is not a list, convert it into a single-element list.

    Parameters
    ----------
    input_item : Any
        The input item that may or may not be a list.

    Returns
    -------
    list
        A list containing the input item(s).
    """
    if isinstance(input_item, list):
        return input_item
    elif input_item is not None:
        return [input_item]
    else:
        return []

def get_citation_key(publication):
    """
    Generate a unique citation key for a publication.

    Parameters
    ----------
    publication : dict
        A dictionary containing publication details, including:
        - `author` (str): The author(s) of the publication.
        - `title` (str): The title of the publication.
        - `year` (str): The publication year.
        - `NOAAStudyId` (str): The unique identifier for the study.

    Returns
    -------
    str
        A citation key formatted as: "<LastName>_<FirstSignificantWord>_<Year>_<NOAAStudyId>".

    Notes
    -----
    - The first significant word in the title excludes words like "the".
    - Spaces in the citation key are removed.
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
    Fetch and parse data from a file URL.

    Parameters
    ----------
    file_url : str
        The URL of the file to fetch. Supported formats include:
        - `.xls` / `.xlsx` for Excel files.
        - `.txt` for tab-delimited text files.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the parsed data. If the file cannot be read
        or parsed, an empty DataFrame is returned.

    Raises
    ------
    Exception
        If there is an error while reading or parsing the file.

    Notes
    -----
    - Excel files (.xls, .xlsx):
        - Reads all sheets into DataFrames and returns them as a list.
        - If there is an error reading the file, an empty DataFrame is returned.
    - Text files (.txt):
        - Parses tab-delimited lines and skips lines starting with `#`.
        - The first non-comment line is treated as the header.
        - If there is an error reading or parsing the file, an empty DataFrame is returned.
    - Unsupported formats:
        - Returns an empty DataFrame and prints a warning message.
    """
    print(file_url)
    if file_url.endswith(".xls") or file_url.endswith(".xlsx"):
        try:
            excel_data = pd.read_excel(file_url, sheet_name=None, comment='#', header = 0)
            df = list(excel_data.values())
            print(f"Extracted {len(df)} DataFrames from {file_url.rsplit('/', 1)[-1]}")
            return df
        except Exception as e:
            print(f"Error reading {file_url}: {e}")
            return pd.DataFrame()
        
    elif file_url.endswith(".txt"):
        
        response = requests.get(file_url)
        if response.status_code == 200:
            try:
                lines = re.split(r'\r\n', response.text)
                data_lines = [line for line in lines if not line.startswith('#') and line.strip()]
                if data_lines:
                    headers = data_lines[0].split('\t')
                    # print(headers)
                    data = [line.split('\t') for line in data_lines[1:]]
                    return pd.DataFrame(data, columns=headers)
            except Exception as e:
                print(f"Error parsing text file: {e}")
                return pd.DataFrame()
        
        else:
            print(f"Failed to fetch data from {file_url}.")
    
    else:
        print(f"Unsupported file format: {file_url}")

    return pd.DataFrame()