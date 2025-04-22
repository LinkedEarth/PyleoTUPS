__all__ = ['Dataset']

import requests
import pandas as pd
import warnings
from ..utils.NOAADataset import NOAADataset
from ..utils.helpers import assert_list
from ..utils.Parser.StandardParser import DataFetcher, StandardParser

class UnsupportedFileTypeError(Exception):
    """Raised when a file type is not supported by the parser."""
    pass


class Dataset:
    """
    A wrapper class for interacting with the NOAA Studies API.

    Manages the retrieval, parsing, and aggregation of NOAA study data,
    and provides methods to access summaries, publications, sites, and external data files.

    Attributes
    ----------
    BASE_URL : str
        The NOAA API endpoint URL.
    studies : dict
        A mapping from NOAAStudyId to NOAADataset instances.
    data_table_index : dict
        A mapping from dataTableID to associated study, site, and paleo data.

    Methods
    -------
    __init__()
        Initializes the Dataset.
    search_studies(...)
        Searches for studies using provided parameters and parses the response.
    _fetch_api(params)
        Internal method to make an HTTP GET request to the NOAA API.
    _parse_response(data)
        Internal method to parse the JSON response and populate studies.
    get_summary()
        Returns a DataFrame summarizing all loaded studies.
    get_publications()
        Returns a DataFrame of publications aggregated from studies.
    get_sites()
        Returns a DataFrame of sites aggregated from studies.
    get_data(dataTableIDs, file_urls)
        Fetches and returns external data based on data table IDs or file URLs.
    """
    BASE_URL = "https://www.ncei.noaa.gov/access/paleo-search/study/search.json"
    _PROPRIETARY_TYPES = {'crn', 'rwl', 'fhx', 'lpd'}

    def __init__(self):
        """
        Initialize the Dataset instance.

        Attributes are set to their default empty values.
        """
        self.studies = {}               # NOAAStudyId -> NOAADataset instance
        self.data_table_index = {}      # dataTableID -> dict with study, site, paleo_data
        self.file_url_to_datatable = {} # file_url -> dataTableID
    def search_studies(self, xml_id=None, noaa_id=None, data_publisher="NOAA", data_type_id=None,
                       keywords=None, investigators=None, max_lat=None, min_lat=None, max_lon=None,
                       min_lon=None, location=None, publication=None, search_text=None, earliest_year=None,
                       latest_year=None, cv_whats=None, recent=False):
        """
        Search for NOAA studies using the provided parameters.

        At least one parameter must be specified for a search to be initiated.  

        Parameters
        ----------
        xml_id : str, optional
            XML identifier for a study.
        noaa_id : str, optional
            NOAA study identifier.
        data_publisher : str, optional
            Publisher of the data, default is "NOAA".
        data_type_id : str, optional
            Data type identifier.
        keywords : str, optional
            Keywords for the search.
        investigators : str, optional
            Investigator names.
        max_lat : float, optional
            Maximum latitude.
        min_lat : float, optional
            Minimum latitude.
        max_lon : float, optional
            Maximum longitude.
        min_lon : float, optional
            Minimum longitude.
        location : str, optional
            Location description.
        publication : str, optional
            Publication details.
        search_text : str, optional
            Additional text to search within the study.
        earliest_year : int, optional
            Earliest year of study.
        latest_year : int, optional
            Latest year of study.
        cv_whats : str, optional
            Controlled vocabulary term.
        recent : bool, optional
            Flag to filter recent studies.

        Returns
        -------
        None
            The method populates internal attributes with the retrieved data.
            Requires at least one single parameter. Parameter validation to be implemented soon. 
        """
        if noaa_id:
            params = {'NOAAStudyId': noaa_id}
        elif xml_id:
            params = {'xmlId': xml_id}
        else:
            params = {
                'dataPublisher': data_publisher,
                'dataTypeId': data_type_id,
                'keywords': keywords,
                'investigators': investigators,
                'minLat': min_lat,
                'maxLat': max_lat,
                'minLon': min_lon,
                'maxLon': max_lon,
                'locations': location,
                'searchText': search_text,
                'cvWhats': cv_whats,
                'earliestYear': earliest_year,
                'latestYear': latest_year,
                'recent': recent,
            }
            params = {k: v for k, v in params.items() if v is not None}

        response_json = self._fetch_api(params)
        self._parse_response(response_json)
        self.get_summary()

    def _fetch_api(self, params):
        """
        Fetch data from the NOAA API using the given parameters.

        Parameters
        ----------
        params : dict
            A dictionary of query parameters.

        Returns
        -------
        dict
            The JSON response from the NOAA API.

        Raises
        ------
        Exception
            If the API response status is not 200.
        """
        
        response = requests.get(self.BASE_URL, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error fetching studies: {response.status_code}")

    def _parse_response(self, data):
        """
        Parse the JSON response and populate the internal studies and data_table_index.

        Parameters
        ----------
        data : dict
            The JSON data returned from the NOAA API.

        Returns
        -------
        None
        """
    
        self.studies.clear()
        self.data_table_index.clear()
        self.file_url_to_datatable.clear()
        # self.sites.clear()
        for study_data in data.get('study', []):
            study_obj = NOAADataset(study_data)
            self.studies[study_obj.study_id] = study_obj
            # print(study_obj.study_id)
            # Process each site in the study.
            for site in study_obj.sites:
                # self.sites[site.site_id] = site
                # Build index for each PaleoData object and map file URL to dataTableID.
                # print(site.site_id)
                for paleo in site.paleo_data:
                    self.data_table_index[paleo.datatable_id] = {
                        'study_id': study_obj.study_id,
                        'site_id': site.site_id,
                        'paleo_data': paleo
                    }
                    if paleo.file_url:
                        self.file_url_to_datatable[paleo.file_url] = paleo.datatable_id


    def get_summary(self):
        """
        Get a DataFrame summarizing all loaded studies.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with a summary of study metadata and components.
        """
        data = [study.to_dict() for study in self.studies.values()]
        return pd.DataFrame(data)

    def get_publications(self):
        """
        Get a DataFrame of all publications aggregated from the studies.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing publication details with study context.
        """
        publications_data = []
        for study in self.studies.values():
            for pub in study.publications:
                pub_dict = pub.to_dict()
                pub_dict['StudyID'] = study.study_id
                pub_dict['StudyName'] = study.metadata.get("studyName")
                publications_data.append(pub_dict)
        return pd.DataFrame(publications_data)

    def get_sites(self):
        """
        Get a DataFrame of all sites aggregated from the studies, including paleo data.
        
        Returns
        -------
        pandas.DataFrame
            A DataFrame containing site details with study context and paleo data.
        """
        records = []
        for study in self.studies.values():
            study_id = study.study_id
            study_name = study.metadata.get("studyName")
            for site in study.sites:
                site_dict = site.to_dict()
                # Remove PaleoData from site_dict so it doesn't duplicate the paleo records.
                paleo_data = site_dict.pop('PaleoData', None)
                
                if paleo_data and isinstance(paleo_data, list) and len(paleo_data) > 0:
                    # For each paleo record in the list, create a merged record.
                    for paleo_record in paleo_data:
                        # Merge site data and paleo record. If paleo_record contains an ID,
                        # you might extract and set it as NOAADataTableId.
                        record = {**site_dict, **paleo_record}
                        record.update({
                            'StudyID': study_id,
                            'StudyName': study_name,
                        })
                        records.append(record)
                else:
                    # If no paleo data is present, record the site as is.
                    site_dict.update({
                        'StudyID': study_id,
                        'StudyName': study_name
                    })
                    records.append(site_dict)
        return pd.DataFrame(records)



    def get_data_deprecated(self, dataTableIDs=None, file_urls=None):
        """
        Fetch external data for given dataTableIDs or file URLs and attach study/site metadata.

        Parameters
        ----------
        dataTableIDs : list or str, optional
            One or more NOAA data table IDs.
        file_urls : list or str, optional
            One or more file URLs.

        Returns
        -------
        list of pandas.DataFrame
            A list of DataFrames, each corresponding to fetched data.
        """

        if dataTableIDs:
            dataTableIDs = assert_list(dataTableIDs)
            dfs = []
            for dt_id in dataTableIDs:
                mapping = self.data_table_index.get(dt_id)
                if not mapping:
                    print(f"Data Table ID {dt_id} not found or no associated file URL.")
                    continue
                file_url = mapping['paleo_data'].file_url
                if not file_url:
                    print(f"No file URL for Data Table ID {dt_id}.")
                    continue
                fetched_data = DataFetcher.fetch_data(file_url)
                if isinstance(fetched_data, list):
                    for df in fetched_data:
                        df.attrs['NOAAStudyId'] = mapping['study_id']
                        df.attrs['SiteID'] = mapping['site_id']
                        study_obj = self.studies.get(mapping['study_id'], {})
                        df.attrs['StudyName'] = study_obj.metadata.get("studyName") if hasattr(study_obj, 'metadata') else None
                        publications = study_obj.publications if hasattr(study_obj, 'publications') else None
                        print(len(publications))
                        for pub in publications:
                            if hasattr(pub, "doi"):
                                doi = pub.doi if pub.doi else None
                                df.attrs['PublicationDOI'].append(doi)                                
                        dfs.append(df)
                else:
                    fetched_data.attrs['NOAAStudyId'] = mapping['study_id']
                    fetched_data.attrs['SiteID'] = mapping['site_id']
                    study_obj = self.studies.get(mapping['study_id'], {})
                    fetched_data.attrs['StudyName'] = study_obj.metadata.get("studyName") if hasattr(study_obj, 'metadata') else None
                    dfs.append(fetched_data)
            return dfs
        if file_urls:
            file_urls = assert_list(file_urls)
            dfs = [DataFetcher.fetch_data(url) for url in file_urls]
            return dfs
        print("No dataTableID or file URL provided.")
        return pd.DataFrame()
    

    def _process_file(self, file_url, mapping=None):
        """
        Process a single file URL: validate the file type, parse the file, and attach metadata.
        Extended metadata now includes site details if available.

        Parameters
        ----------
        file_url : str
            The URL of the file to process.
        mapping : dict, optional
            The mapping information containing study and site metadata.
        
        Returns
        -------
        list of pandas.DataFrame
            A list of DataFrames parsed from the file.
        
        Raises
        ------
        ValueError
            For proprietary or invalid file types, or missing file URL.
        Exception
            For any parsing errors encountered by StandardParser.
        """
        if not file_url:
            raise ValueError("Faulty input: file URL is missing.")

        file_type = file_url.split('.')[-1].lower()
        if file_type in self._PROPRIETARY_TYPES:
             raise UnsupportedFileTypeError(f"Pytups works with .txt files only. "
                             "File type '{file_type}' can be processed with a {proprietary software}.") #{proprietary software} shall be replaced by respective mapping. 
        if file_type != 'txt':
            raise UnsupportedFileTypeError(f"Invalid file type '{file_type}'. Only .txt files are supported.")

        try:
            # print(file_url, type(file_url))
            parsed_data = StandardParser(file_url).parse()
        except Exception as e:
            raise e

        def attach_metadata(df, mapping):
            # Attach study metadata.
            df.attrs['NOAAStudyId'] = mapping.get('study_id')
            study_obj = self.studies.get(mapping.get('study_id'), {})
            df.attrs['StudyName'] = study_obj.metadata.get("studyName") if hasattr(study_obj, 'metadata') else None
            # Attach site metadata if available.
            # site_obj = self.sites.get(mapping.get('site_id'))
            # if site_obj:
            #     df.attrs['SiteID'] = site_obj.site_id
            #     df.attrs['SiteName'] = site_obj.site_name
            #     df.attrs['LocationName'] = site_obj.location_name
            #     df.attrs['Latitude'] = site_obj.lat
            #     df.attrs['Longitude'] = site_obj.lon
            #     df.attrs['MinElevation'] = site_obj.min_elevation
            #     df.attrs['MaxElevation'] = site_obj.max_elevation
            # publications = study_obj.publications if hasattr(study_obj, 'publications') else None
            # print(len(publications))
            # for pub in publications:
            #     if hasattr(pub, "doi"):
            #         doi = pub.doi if pub.doi else None
            #         df.attrs['PublicationDOI'].append(doi)                                
            return df

        results = []
        if isinstance(parsed_data, list):
            for df in parsed_data:
                if mapping:
                    df = attach_metadata(df, mapping)
                results.append(df)
        else:
            if mapping:
                parsed_data = attach_metadata(parsed_data, mapping)
            results.append(parsed_data)
        return results


    def get_data(self, dataTableIDs=None, file_urls=None):
        """
        Fetch external data for given dataTableIDs or file URLs, perform validations,
        and attach study and site metadata.

        Parameters
        ----------
        dataTableIDs : list or str, optional
            One or more NOAA data table IDs.
        file_urls : list or str, optional
            One or more file URLs.

        Returns
        -------
        list of pandas.DataFrame
            A list of DataFrames corresponding to the fetched data.

        Raises
        ------
        ValueError
            For missing parent study mapping, missing file URL, or proprietary/unsupported file types.
        Exception
            Propagates any exceptions raised by the parser.
        """
        dfs = []

        # Process based on dataTableIDs.
        if dataTableIDs:
            dataTableIDs = assert_list(dataTableIDs)
            for dt_id in dataTableIDs:

                # print(self.data_table_index, type(self.data_table_index.values()))
                # for id, value in self.data_table_index.items():
                    # print(type(id))
                    # print(value, type(value))
                mapping = self.data_table_index.get(dt_id)
                if not mapping:
                    raise ValueError(f"No parent study mapping found for Data Table ID '{dt_id}'. "
                                     "Please perform a search using this DataTableID or provide a specific file URL.")
                file_url = mapping['paleo_data'].file_url
                if not file_url:
                    raise ValueError(f"File URL for Data Table ID '{dt_id}' is missing. Cannot fetch data.")
                dfs.extend(self._process_file(file_url, mapping))
            return dfs

        # Process based on file_urls provided directly.
        if file_urls:
            file_urls = assert_list(file_urls)
            for url in file_urls:
                mapping = self.file_url_to_datatable.get(url)
                if not mapping:
                    warnings.warn(
                        f"Attached '{url}' is not linked to any parent study; additional metadata will not be attached.",
                        UserWarning
                    )
                    dfs.extend(self._process_file(url))
                else:
                    mapping_details = self.data_table_index.get(mapping)
                    if not mapping_details:
                        warnings.warn(
                            f"Mapping details for file URL '{url}' (Data Table ID '{mapping}') not found; additional metadata will not be attached.",
                            UserWarning
                        )
                        dfs.extend(self._process_file(url))
                    else:
                        dfs.extend(self._process_file(url, mapping_details))
            return dfs

        raise ValueError("No dataTableID or file URL provided. Cannot fetch data.")