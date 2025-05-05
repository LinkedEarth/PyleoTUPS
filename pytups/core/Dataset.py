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
                       latest_year=None, cv_whats=None, recent=False, limit = 100):
        """
        Search for NOAA studies using the specified parameters.

        At least one parameter must be provided to perform a search. This method interfaces with
        the NOAA NCEI Paleo Study Search API. Use it to filter studies based on location,
        investigators, time range, keywords, and more.

        Parameters
        ----------
        xml_id : str, optional
            Specify the internal XML document ID. Must be an exact match (e.g., '1840').

        noaa_id : str, optional
            Provide the unique NOAA Study ID as a number (e.g., '13156').

        search_text : str, optional
            General text search across study content. Supports wildcards (%) and logical operators (AND, OR).
            Examples: 'younger dryas', 'loess AND stratigraphy'

        data_publisher : by default 'NOAA'
            Choose from: 'NOAA', 'NEOTOMA', or 'PANGAEA'.
            Example: 'NOAA'

        data_type_id : str, optional
            Filter by data type. Use one or more type IDs separated by '|'.
            Available IDs:
                1: BOREHOLE, 2: CLIMATE FORCING, 3: CLIMATE RECONSTRUCTIONS, 4: CORALS AND SCLEROSPONGES,
                6: HISTORICAL, 7: ICE CORES, 8: INSECT, 9: LAKE LEVELS, 10: LOESS,
                11: PALEOCLIMATIC MODELING, 12: FIRE HISTORY, 13: PALEOLIMNOLOGY, 14: PALEOCEANOGRAPHY,
                15: PLANT MACROFOSSILS, 16: POLLEN, 17: SPELEOTHEMS, 18: TREE RING,
                19: OTHER COLLECTIONS, 20: INSTRUMENTAL, 59: SOFTWARE, 60: REPOSITORY
            Example: '4|18'

        keywords : str, optional
            Use hierarchical terms separated by '>'. Separate multiple values using '|'.
            Example: 'earth science>paleoclimate>paleocean>biomarkers'

        investigators : str, optional
            Specify one or more investigator names. Use '|' to separate multiple names.
            Example: 'Wahl, E.R.|Vose, R.S.'

        max_lat : float, optional
            Upper bound for latitude. Must be between -90 and 90.
            Example: 90

        min_lat : float, optional
            Lower bound for latitude. Must be between -90 and 90.
            Example: -90

        max_lon : float, optional
            Upper bound for longitude. Must be between -180 and 180.
            Example: 180

        min_lon : float, optional
            Lower bound for longitude. Must be between -180 and 180.
            Example: -180

        location : str, optional
            Use region hierarchy separated by '>'.
            Example: 'Continent>Africa>Eastern Africa>Zambia'

        publication : str, optional
            Match against publication metadata such as title, author, or citation.
            Example: 'Khider'

        earliest_year : int, optional
            Starting year (can be negative for BCE). Used with `timeFormat` and `timeMethod`.
            Example: -500

        latest_year : int, optional
            Ending year. Used with `timeFormat` and `timeMethod`.
            Example: 2020

        cv_whats : str, optional
            Search using controlled vocabulary terms for measured variables.
            Format: Hierarchical string using '>'
            Example: 'chemical composition>compound>inorganic compound>carbon dioxide'

        recent : bool, optional
            Set to True to only return studies from the last two years. Results are sorted by newest.

        limit : int, optional
            Set to 100 by default. Limits the number of studies retrieved. 

        Returns
        -------
        pandas.DataFrame
            Response DataFrame
            Fills the internal `studies` attribute with structured NOAA study data.
        
        Raises
        ------
        ValueError
            If no inputs are passed.

        requests.HTTPError
            If the HTTP request returned an unsuccessful status code.

        Notes
        -----
        - At least one parameter must be specified, otherwise the API call will fail.
        """
        
        # Validate input
        if not any([xml_id, noaa_id, data_type_id, keywords, investigators, max_lat, min_lat, max_lon,
                    min_lon, location, publication, search_text, earliest_year, latest_year, cv_whats, recent]):
            raise ValueError(
                "At least one search parameter must be specified to initiate a query. "
                "To view available parameters and usage examples, run: help(Dataset.search_studies)"
            )
        
        if data_publisher != "NOAA":
            raise NotImplementedError(
                f"PyleoTUPS does not support '{data_publisher}' as the data publisher in the current version."
                "Please retry a search with data_publisher = NOAA "
                "Please check future versions for support of other publishers."
            ) 

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
                'limit': limit
            }
            params = {k: v for k, v in params.items() if v is not None}

        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()
            response_json = response.json()
        except requests.HTTPError as e:
            raise RuntimeError(f"HTTP error from NOAA API: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to fetch or parse response: {e}")
        
        self.studies.clear()
        self.file_url_to_datatable.clear()  
        
        from tqdm import tqdm
        for study_data in tqdm(response_json.get("study", []), desc="Parsing NOAA studies"):
            try:
                study_obj = NOAADataset(study_data)
                self.studies[study_obj.study_id] = study_obj
            except Exception as e:
                print(f"Skipping study due to error: {e}")

        return self.get_summary()

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
        Parse the JSON response and populate studies and reverse mapping indexes.
        """
        self.studies.clear()
        self.data_table_index.clear()
        self.file_url_to_datatable.clear()

        for study_data in data.get('study', []):
            study_obj = NOAADataset(study_data)
            self.studies[study_obj.study_id] = study_obj

            for site in study_obj.sites:
                for paleo in site.paleo_data:
                    self.data_table_index[paleo.datatable_id] = {
                        'study_id': study_obj.study_id,
                        'site_id': site.site_id,
                        'paleo_data': paleo
                    }
                    for file_obj in paleo.files:
                        file_url = file_obj.get('fileUrl')
                        if file_url:
                            self.file_url_to_datatable[file_url] = paleo.datatable_id



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

    def get_publications(self, save=False, path=None, verbose=False):
        """
        Get all publications in both BibTeX and DataFrame formats.

        Parameters
        ----------
        save : bool, default=False
            If True, save the BibTeX to a .bib file.
        path : str or None, optional
            Path to save the .bib file. If None and save=True,
            saves to 'bibtex_<timestamp>.bib'.
        verbose : bool, default=False
            If True, print the BibTeX content to console.

        Returns
        -------
        tuple (pybtex.database.BibliographyData, pandas.DataFrame)
            BibTeX object and DataFrame of publication details.
        """
        from pybtex.database import BibliographyData
        
        publications_data = []
        bib_entries = {}

        # Collect publication metadata and BibTeX entries
        for study in self.studies.values():
            for pub in study.publications:
                pub_dict = pub.to_dict()
                pub_dict['StudyID'] = study.study_id
                pub_dict['StudyName'] = study.metadata.get("studyName") or "Unknown Study"
                publications_data.append(pub_dict)

                try:
                    citation_key = pub.get_citation_key()
                    bib_entries[citation_key] = pub.to_bibtex_entry()
                except Exception as e:
                    raise ValueError(
                        f"Failed to convert a publication in study {study.study_id} to BibTeX. "
                        f"Original error: {e}"
                    )

        df = pd.DataFrame(publications_data)
        bibs = BibliographyData(entries=bib_entries)

        # Save to file if requested
        if save:
            import datetime
            from pybtex.database.output.bibtex import Writer
            from pathlib import Path

            if not path:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
                path = f"bibtex_{timestamp}.bib"
                warnings.warn(f"No path specified. Saving BibTeX to: {path}")

            try:
                writer = Writer()
                with open(Path(path), "w", encoding="utf-8") as f:
                    writer.write_stream(bibs, f)
            except Exception as e:
                raise IOError(f"Failed to write BibTeX file to '{path}': {e}")

        # Print if verbose
        if verbose:
            from pybtex.database.output.bibtex import Writer
            from io import StringIO
            buffer = StringIO()
            Writer().write_stream(bibs, buffer)
            print(buffer.getvalue())

        return bibs, df    
    
    def get_tables(self):
        """
        Get a DataFrame of all sites expanded to paleo data files.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with one row per (Site × PaleoData × File).
        """
        import pandas as pd

        records = []
        for study in self.studies.values():
            study_id = study.study_id
            study_name = study.metadata.get("studyName")

            for site in study.sites:
                paleo_data_records = site.to_dict()  # Already flattened per file
                for paleo_record in paleo_data_records:
                    paleo_record.update({
                        "StudyID": study_id,
                        "StudyName": study_name
                    })
                    records.append(paleo_record)

        return pd.DataFrame(records)

    def get_sites(self):
        """
        Get a DataFrame of all sites expanded to paleo data files.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with one row per (Site × PaleoData × File).
        """
        import pandas as pd

        records = []
        for study in self.studies.values():
            study_id = study.study_id
            study_name = study.metadata.get("studyName")

            for site in study.sites:
                paleo_data_records = site.to_dict()  # Already flattened per file
                for paleo_record in paleo_data_records:
                    paleo_record.update({
                        "StudyID": study_id,
                        "StudyName": study_name
                    })
                    records.append(paleo_record)

        return pd.DataFrame(records)

    import pandas as pd

    def get_geo(self):
        """
        Get a DataFrame of site-level geospatial metadata and associated data types
        from all studies loaded into the Dataset.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with one row per site and columns:
            ['StudyID', 'SiteID', 'SiteName', 'LocationName',
            'Latitude', 'Longitude', 'MinElevation', 'MaxElevation', 'DataType']
        """
        site_records = []

        for study in self.studies.values():
            study_id = study.study_id
            data_type = study.metadata.get("dataType", "Unknown")

            for site in study.sites:
                site_dict = {
                    "StudyID": study_id,
                    "DataType": data_type,
                    **{
                        k: v for k, v in site.to_dict()[0].items()  # site.to_dict() returns list of dicts (1 per file)
                        if k in ["SiteID", "SiteName", "LocationName", "Latitude", "Longitude", "MinElevation", "MaxElevation"]
                    }
                }
                site_records.append(site_dict)

        return pd.DataFrame(site_records)



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
                        f"Attached '{url}' is not linked to any parent study; can not add metadata.",
                        UserWarning
                    )
                    dfs.extend(self._process_file(url))
                else:
                    mapping_details = self.data_table_index.get(mapping)
                    if not mapping_details:
                        warnings.warn(
                            f"Mapping details for file URL '{url}' (Data Table ID '{mapping}') not found; can not add metadata.",
                            UserWarning
                        )
                        dfs.extend(self._process_file(url))
                    else:
                        dfs.extend(self._process_file(url, mapping_details))
            return dfs

        raise ValueError("No dataTableID or file URL provided. Cannot fetch data.")