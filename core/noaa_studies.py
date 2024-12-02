import requests
import pandas as pd
import numpy as np
from pybtex.database import BibliographyData, Entry
import re
# from utils.helper import search_studies, assert_list, get_citation_key, fetch_data
from utils.helper import *

class NOAAStudies:
    def __init__(self):
        """
        Initialize the NOAAStudies class with base URL and dictionaries to hold studies and data table indices.


        The search parameters are designed to be flexible and can vary for different searches using the same object.
        Instead of overwriting the initialized default parameters, each search maintains its own set of parameters, 
        allowing for independent configurations across multiple searches.
        """
        self.BASE_URL = "https://www.ncei.noaa.gov/access/paleo-search/study/search.json"
        self.studies = {}
        self.data_table_index = {}

    def search_studies(self, xml_id=None, noaa_id=None, data_publisher="NOAA", data_type_id = None, keywords = None, investigators=None, 
                       max_lat=None, min_lat=None, max_lon=None, min_lon=None, location = None, publication = None, search_text = None, 
                       earliest_year = None, latest_year = None, cv_whats = None, recent = False):
        """
        Search for NOAA studies using specific search parameters.
        
        Parameters:
            xml_id (str): XML ID of the study. (Primary)
            noaa_id (str): NOAA study ID. (Primary)
            data_publisher (str): Data publisher's name. Default: NOAA 
            data_type_id (str): Data Type specific Studies.
            keywords (str): Keywords to search for.  
            investigators (str): Name(s) of investigators.
            min_lat/max_lat (float): Latitude range for location-based search.
            min_lat/max_lat (float): Longitude range for location-based search.
            location (str): Location description.
            species (str): FOUR letter code species code
            publication (str): Specific publication.
            search_text (str): Publication / studyNotes / String based search.
           
        """
        if noaa_id:
            params = {'NOAAStudyId': noaa_id}
        elif xml_id:
            params = {'xmlId': xml_id}
        else:
            params = {
            'dataPublisher' : data_publisher,
            'dataTypeId' : data_type_id,
            'keywords' : keywords, 
            'investigators' : investigators,
            'minLat' : min_lat,
            'maxLat' : max_lat,
            'minLon' : min_lon,
            'maxLon' : max_lon,
            'locations' : location,
            'searchText' : publication, 
            'searchText' : search_text,
            'cvWhats': cv_whats,
            'earliestYear' : earliest_year,
            'latestYear': latest_year, 
            'recent' : recent,
        }

        self.response_parser(search_studies(params))

    def response_parser(self, data):
        """
        Parse the JSON response from NOAA and populate the studies dictionary.
        
        Parameters:
            data (dict): The JSON data returned from a search query.
        """
        for study in data.get('study', []):
            noaa_study_id = study.get('NOAAStudyId')
            xml_study_id = study.get('xmlId')
            self.studies[noaa_study_id] = {
                'base_meta': self.load_base_meta(study),
                'investigators': self.load_investigators(study),
                'publications': self.load_publications(study),
                'sites': self.load_sites(study, noaa_study_id),
                # 'number of sites': len(sites)
                'pageUrl' : study.get('onlineResourceLink', np.nan)
            }

    def load_base_meta(self, study):
        """
        Load base metadata for a study.
        
        Parameters:
            study (dict): Part of the JSON data pertaining to a single study.
        """
        fields = ['NOAAStudyId', 'studyName', 'dataType', 'earliestYearBP', 'mostRecentYearBP',
                  'earliestYearCE', 'mostRecentYearCE', 'studyNotes', 'scienceKeywords']
        return {field: study.get(field, np.nan) for field in fields}

    def load_investigators(self, study):
        """
        Extract investigator details from the study data.
        
        Parameters:
            study (dict): Part of the JSON data pertaining to a single study.
        """
        investigators = study.get("investigatorDetails", [])
        if investigators:
            return ", ".join([f"{i.get('firstName', 'N/A')} {i.get('lastName', 'N/A')}" for i in investigators])
        return np.nan

    def load_publications(self, study):
        """
        Extract and format publication data from the study as a dictionary.

        Parameters:
            study (dict): Part of the JSON data pertaining to a single study.
            study_id (str): The unique identifier for the NOAA study.

        Returns:
            list: A list of dictionaries representing publication details.
        """
        publications = []
        for pub in study.get('publication', []):
            author_info = pub.get('author', {})
            identifier_info = pub.get('identifier', {})

            # Extract fields for the publication dictionary
            publication_data = {
                'NOAAStudyId': study.get('NOAAStudyId'),
                'author': author_info.get('name', 'Unknown Author'),
                'title': pub.get('title', 'Unknown Title'),
                'journal': pub.get('journal', 'Unknown Journal'),
                'year': str(pub.get('pubYear', 'Unknown Year')),
                'volume': pub.get('volume', np.nan),
                'number': pub.get('issue', np.nan),
                'pages': pub.get('pages', np.nan),
                'type': pub.get('type', np.nan),
                'doi': identifier_info.get('id', np.nan) if identifier_info else np.nan,
                'url': identifier_info.get('url', np.nan) if identifier_info else np.nan
            }

            # Add the publication dictionary to the list
            publications.append(publication_data)
        
        return publications

    def load_sites(self, study, study_id):
        """
        Load and format site data associated with the study.
        
        Parameters:
            study (dict): Part of the JSON data pertaining to a single study.
            study_id (str): The unique identifier of the study for reference.
        """        
        return {
            site.get('NOAASiteId', np.nan): {
                'siteName': site.get('siteName', np.nan),
                'locationName': site.get('locationName', np.nan),
                'lat': site.get('geo', {}).get('geometry', {}).get('coordinates', [np.nan, np.nan])[0],
                'lon': site.get('geo', {}).get('geometry', {}).get('coordinates', [np.nan, np.nan])[1],
                'minElevationMeters': site.get('geo', {}).get('properties', {}).get('minElevationMeters', np.nan),
                'maxElevationMeters': site.get('geo', {}).get('properties', {}).get('maxElevationMeters', np.nan),
                'paleoData': self.load_paleo_data(site.get('paleoData', []), study_id, site.get('NOAASiteId'))
            }
            for site in study.get('site', [])
        }

    def load_paleo_data(self, paleoData, study_id, site_id):
        """
        Extract and format paleo data associated with a site.
        
        Parameters:
            paleoData (list): List of paleo data from the site.
            study_id (str): The unique identifier of the study.
            site_id (str): The unique identifier of the site.
        """
        paleo_dict = {}
        for paleo in paleoData:
            # Safe access to 'dataFile' list
            data_files = paleo.get('dataFile', [])
            file_url = data_files[0].get('fileUrl', np.nan) if data_files else np.nan
            variables = []
            if data_files:  # Check if 'dataFile' is not empty
                variables = [var.get('cvShortName', np.nan) for var in data_files[0].get('variables', [])]
            data_table_id = paleo.get('NOAADataTableId', np.nan)
            paleo_details = {
                'NOAADataTableId': data_table_id,
                'dataTableName': paleo.get('dataTableName', np.nan),
                'timeUnit': paleo.get('timeUnit', np.nan),
                'fileUrl': file_url,
                'variables': variables
            }
            paleo_dict[data_table_id] = paleo_details

            self.data_table_index[data_table_id] = {
                'file_url': file_url,
                'study_id': study_id,
                'site_id': site_id
            }
        return paleo_dict
        
    def get_response(self):
        """
        Compile and return a DataFrame of all loaded studies along with their detailed metadata and linked information.
        
        Returns:
            DataFrame: A DataFrame representing the consolidated data of all studies, including metadata, investigators,
                       publications, and site details.
        """
        data = [{
            **study['base_meta'],
            'Investigators': study['investigators'],
            'publications': study['publications'],
            'sites': study['sites']
        } for study in self.studies.values()]
        return pd.DataFrame(data)
    
    def get_publications(self, study_ids, output_format="dataframe"):
        """
        Return publications for one or more study IDs in the specified format.

        Parameters:
            study_ids (list or str): Single or list of NOAAStudyIds.
            output_format (str): The desired output format. Options:
                - "dataframe": Returns a pandas DataFrame of publication details.
                - "bibtex": Returns a pybtex.database.BibliographyData object.

        Returns:
            pd.DataFrame or pybtex.database.BibliographyData: Publications data in the chosen format.
        """
        study_ids = assert_list(study_ids)  # Ensure study_ids is a list

                
        if output_format == "dataframe":
            dfs = []
            for study_id in study_ids:
                if study_id in self.studies:
                    publications = self.studies[study_id].get('publications', [])
                    df = pd.DataFrame(publications)
                    # df.set_index('NOAAStudyId', inplace=True)  # Set NOAAStudyId as the index
                    dfs.append(df)
                else:
                    print(f"Study ID {study_id} not found.")
            return pd.concat(dfs) if dfs else pd.DataFrame()

    
        elif output_format == "bibtex":
        
            bib_entries = {}
            for study_id in study_ids:
                if study_id in self.studies:
                    publications = self.studies[study_id].get('publications', [])
                    for pub in publications:
                        fields = {k: v for k, v in pub.items() if k not in ['NOAAStudyId'] and v}
                        entry = Entry('article', fields=fields)
                        citation_key = get_citation_key(pub)
                        bib_entries[citation_key] = entry
                else:
                    print(f"Study ID {study_id} not found.")
            
            return BibliographyData(entries=bib_entries)
            
        else:
            print("Invalid output_format. Choose 'dataframe' or 'bibtex'.")

    def get_sites(self, study_ids):
        """
        Return a DataFrame of sites for one or more study IDs.

        Parameters:
            study_ids (list or str): Single or list of NOAAStudyIds.

        Returns:
            pd.DataFrame: Sites DataFrame.
        """
        study_ids = assert_list(study_ids)  # Convert to list if single ID
        
        dfs = []
        for study_id in study_ids:
            if study_id not in self.studies:
                print(f"Study ID {study_id} not found.")
                continue
            
            sites_data = self.studies[study_id].get('sites', {})
            sites_list = []
            for site_id, site_info in sites_data.items():
                site_info['NOAASiteId'] = site_id
                paleo_list = site_info.pop('paleoData', {})
                for paleo_id, paleo_info in paleo_list.items():
                    record = {**site_info, **paleo_info, 'NOAADataTableId': paleo_id, 'NOAAStudyId': study_id}
                    sites_list.append(record)

            if sites_list:
                df = pd.DataFrame(sites_list)
                dfs.append(df)
        
        result = pd.concat(dfs) if dfs else pd.DataFrame()
        result.set_index('NOAAStudyId', inplace=True)  # Set NOAAStudyId as the index
        return result

    def get_data(self, dataTableIDs=None, file_urls=None):
        """
        Fetch and return the data for one or more dataTableIDs or file URLs.

        Parameters:
            dataTableIDs (list or str): Single or list of NOAADataTableIds.
            file_urls (list or str): Single or list of file URLs.

        Returns:
            pd.DataFrame: Combined DataFrame of all fetched data.
        """

        """ 
        @TODO: 
            Add attributes to data frame for file_urls method  
        """
        if dataTableIDs:
            dataTableIDs = assert_list(dataTableIDs)
            dfs = []
            for dataTableID in dataTableIDs:
                file_url = self.data_table_index.get(dataTableID, {}).get('file_url')
                if not file_url:
                    print(f"Data Table ID {dataTableID} not found or no associated file URL.")
                    continue
                df = fetch_data(file_url)

                study_id = self.data_table_index[dataTableID].get('study_id')
                site_id = self.data_table_index[dataTableID].get('site_id')
                study_data = self.studies.get(study_id, {})
                site_data = study_data.get('sites', {}).get(site_id, {})
                
                # Attach attributes to DataFrame
                df.attrs['NOAAStudyId'] = study_id
                # df.attrs['Publication'] = study_data.get('publications', 'N/A')
                df.attrs['StudyName'] = study_data.get('base_meta').get('studyName')
                
                df.attrs['SiteName'] = site_data.get('siteName', np.nan)
                df.attrs['Lat'] = site_data.get('lat', np.nan)
                df.attrs['Lon'] = site_data.get('lon', np.nan)
                df.attrs['minElevationMeters'] = site_data.get('minElevationMeters', np.nan)
                df.attrs['maxElevationMeters'] = site_data.get('maxElevationMeters', np.nan)

                dfs.append(df)
            
            return dfs
        
        if file_urls:
            file_urls = assert_list(file_urls)  # Convert to list if single URL
            dfs = [fetch_data(file_url) for file_url in file_urls]
            return dfs

        print("No dataTableID or file URL provided.")
        return pd.DataFrame()