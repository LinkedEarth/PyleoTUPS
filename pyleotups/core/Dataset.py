__all__ = ['Dataset', 'UnsupportedFileTypeError']

import logging, warnings
import requests
import pandas as pd

from ..utils.NOAADataset import NOAADataset
from ..utils.helpers import assert_list
from ..utils.Parser.StandardParser import DataFetcher, StandardParser
from ..utils.Parser.NonStandardParser import NonStandardParser
from ..utils.api.constants import BASE_URL
from ..utils.api.query_builder import build_payload
from ..utils.api.http import get


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[%(asctime)s][%(levelname)s] - %(message)s')
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
    """
    _PROPRIETARY_TYPES = {'crn', 'rwl', 'fhx', 'lpd'}

    def __init__(self):
        """
        Initialize the Dataset instance.

        Attributes are set to their default empty values.
        """
        self.studies = {}               # NOAAStudyId -> NOAADataset instance
        self.data_table_index = {}      # dataTableID -> dict with study, site, paleo_data
        self.file_url_to_datatable = {} # file_url -> dataTableID
        self.last_timing = {}
        self.logger = logging.getLogger("pyleotups.Dataset")
    
    def search_studies(self, **kwargs):
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
            Example: '4', '4|18'

        investigators : str or list[str], optional
            Investigator(s) in the form ``"LastName, Initials"``. Lists are joined with ``|``.
        
        investigators_and_or : {"and","or"}, default "or"
            Logical combiner when multiple investigators are supplied. Only sent when 2+ items.
        
        locations : str or list[str], optional
            Location(s) as hierarchical strings using ``>`` (e.g., ``"Continent>Africa>Kenya"``). Lists joined with ``|``.
        
        locations_and_or : {"and","or"}, default "or"
            Logical combiner for multiple locations. Only sent when 2+ items.
        
        keywords : str or list[str], optional
            Controlled keyword(s); hierarchies with ``>``. Lists joined with ``|``.
        
        keywords_and_or : {"and","or"}, default "or"
            Logical combiner for multiple keywords. Only sent when 2+ items.
        
        species : str or list[str], optional
            Four-letter tree species codes (uppercase enforced). Lists joined with ``|``.
        
        species_and_or : {"and","or"}, default "or"
            Logical combiner for multiple species. Only sent when 2+ items.
        
        cv_whats : str or list[str], optional
            PaST “What” terms (hierarchies with ``>``). Lists joined with ``|``.
        
        cv_whats_and_or : {"and","or"}, default "or"
            Logical combiner for multiple cv_whats. Only sent when 2+ items.
        
        cv_materials : str or list[str], optional
            PaST “Material” terms (hierarchies with ``>``). Lists joined with ``|``.
        
        cv_materials_and_or : {"and","or"}, default "or"
            Logical combiner for multiple cv_materials. Only sent when 2+ items.
        
        cv_seasonalities : str or list[str], optional
            PaST “Seasonality” terms (e.g., ``"annual"`` or ``"3-month>Aug-Oct"``). Lists joined with ``|``.
        
        cv_seasonalities_and_or : {"and","or"}, default "or"
            Logical combiner for multiple cv_seasonalities. Only sent when 2+ items.
        
        min_lat, max_lat : int, optional
            Latitude bounds in whole degrees (–90..90).
        
        min_lon, max_lon : int, optional
            Longitude bounds in whole degrees (–180..180).
        
        min_elevation, max_elevation : int, optional
            Elevation bounds in meters (integers; negative allowed).
        
        earliest_year, latest_year : int, optional
            Year bounds (integers; negative allowed). If provided without time settings, ``time_format`` defaults to ``'CE'``.
        
        time_format : {"CE","BP"}, optional
            Interpretation of years. If omitted with a time window, defaults to ``'CE'``.
        
        time_method : {"overAny","entireOver","overEntire"}, optional
            How to apply the time window (overlap, envelop, or within).
        
        reconstruction : bool or str, optional
            Accepts True/False or strings (case-insensitive) like ``"true"|"yes"|"y"|"1"`` → ``'Y'`` and
            ``"false"|"no"|"n"|"0"`` → ``'N'``. ``None`` omits the filter.
        
        recent : bool, default False
            If True, restrict to studies from the last ~2 years (newest first).
        
        limit : int, default 100
            Number of studies to return (PyleoTUPS default).
        
        display : bool, default False
            If True, render a small preview after parsing. 

        Returns
        -------
        pandas.DataFrame
            Response DataFrame. Fills the internal `studies` attribute with structured NOAA study data.
        
        Raises
        ------
        ValueError
            If no inputs are passed.

        requests.HTTPError
            If the HTTP request returned an unsuccessful status code.

        Notes
        -----
        Notes (User Guide)
        ------------------
        **Multi-value fields.** For ``investigators``, ``locations``, ``keywords``, ``species``, ``cv_whats``,
        ``cv_materials``, ``cv_seasonalities``:
        - Accept a string (already ``|``-separated) **or** a Python list of strings.
        - Lists are joined with ``|``. The corresponding ``*_and_or`` flag is included only when 2+ items.
        - Species are validated to **four uppercase letters**.

        **Identifiers short-circuit.** If ``xml_id`` or ``noaa_id`` is set, the request includes only that id (plus
        publisher), ignoring other filters.

        **Time window defaults.** If either ``earliest_year`` or ``latest_year`` is provided and neither ``time_format``
        nor ``time_method`` is supplied, ``time_format`` defaults to ``'CE'`` (a note is recorded).

        **Unsupported parameters.** ``headersOnly`` and ``skip`` are not supported by PyleoTUPS and are ignored if passed.

        **Boolean normalization.** Parameters expected as ``'Y'/'N'`` accept: True/False, or strings like
        ``"true"|"yes"|"y"|"1"`` → ``'Y'`` and ``"false"|"no"|"n"|"0"`` → ``'N'``.

        Examples
        --------

        Quick start (identifiers)
        ^^^^^^^^^^^^^^^^^^^^^^^^^
        .. jupyter-execute::

            import pyleotups as pt
            ds = pt.Dataset()
            ds.search_studies(noaa_id=13156, display=True)
            ds.search_studies(xml_id=1840, display=True)

        Full-text search (Oracle syntax)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        .. jupyter-execute::

            # Single phrase
            ds.search_studies(search_text="younger dryas", limit=20, display=True)

            # Logical operator (AND)
            ds.search_studies(search_text="loess AND stratigraphy", limit=20, display=True)

            # Wildcards: '_' (single char), '%' (multi-char)
            ds.search_studies(search_text="f_re", limit=20, display=True)
            ds.search_studies(search_text="pol%", limit=20, display=True)

            # Escaping special characters (use backslashes)
            ds.search_studies(search_text=r"noaa\-tree\-19260", limit=20, display=True)

        Investigators, keywords, locations
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        .. jupyter-execute::

            # Multiple investigators (OR by default)
            ds.search_studies(investigators=["Wahl, E.R.", "Vose, R.S."], display=True)

            # Multiple investigators (OR by default)
            ds.search_studies(investigators=["Wahl, E.R.", "Vose, R.S."], investigatorsAndOr = "and")

            # Keywords: hierarchy with '>' and multiple via '|'
            ds.search_studies(keywords="earth science>paleoclimate>paleocean>biomarkers", display=True)

            # Location hierarchy
            ds.search_studies(locations="Continent>Africa>Eastern Africa>Zambia", display=True)

        Species and types
        ^^^^^^^^^^^^^^^^^
        .. jupyter-execute::

            # Species: four-letter codes (uppercase enforced)
            ds.search_studies(species=["ABAL", "PIPO"], display=True)

            # Data types: one or more IDs separated by '|'
            ds.search_studies(data_type_id="4|18", display=True)
        

        Geography and elevation
        ^^^^^^^^^^^^^^^^^^^^^^^
        .. jupyter-execute::

            ds.search_studies(min_lat=68, max_lat=69, min_lon=30, max_lon=40, display=True)
            ds.search_studies(min_elevation=100, max_elevation=110, display=True)

        Time window (defaults to CE if no time settings)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        .. jupyter-execute::

            # Explicit BP with method
            ds.search_studies(earliest_year=12000, time_format="BP", time_method="overAny", display=True)

            # No time_format/time_method → defaults to CE
            ds.search_studies(earliest_year=1500, latest_year=0, display=True)

        Reconstructions and recency
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^
        .. jupyter-execute::

            ds.search_studies(reconstruction=True, display=True)
            ds.search_studies(recent=True, limit=25, display=True)

        """

        for param in ("headerheaders_only", "skip"):
            if param in kwargs:
                log.warning("%s is not supported and will be ignored.", param)
                kwargs.pop(param, None)

        if not any([
        kwargs.get("xml_id"), kwargs.get("noaa_id"),
        kwargs.get("data_type_id"), kwargs.get("keywords"),
        kwargs.get("investigators"),
        kwargs.get("max_lat"), kwargs.get("min_lat"),
        kwargs.get("max_lon"), kwargs.get("min_lon"),
        kwargs.get("location") or kwargs.get("locations"),
        kwargs.get("publication"), kwargs.get("search_text"),
        kwargs.get("earliest_year"), kwargs.get("latest_year"),
        kwargs.get("cv_whats"), kwargs.get("min_elevation"),
        kwargs.get("max_elevation"), kwargs.get("time_format"),
        kwargs.get("time_method"), kwargs.get("reconstruction"),
        kwargs.get("species"), kwargs.get("recent"),
        ]):
            raise ValueError(
                "At least one search parameter must be specified to initiate a query. "
                "To view available parameters and usage examples, run: help(Dataset.search_studies)"
            )
        
        if kwargs.get("data_publisher") and kwargs["data_publisher"] != "NOAA":
            raise NotImplementedError(
            "PyleoTUPS currently supports data_publisher='NOAA' only. "
            "Please retry with data_publisher='NOAA'."
        )

        # Build payload using our utils (handles ids short-circuit, list→'|', Y/N coercion, time default)
        payload, notes = build_payload(**kwargs)
        for n in notes:
            log.info("search_studies: %s", n)
        self.last_search_notes = notes

        # --- Make the request with explicit 204 handling ---
        resp = get(BASE_URL, payload)
        status = resp.status_code

        # 204 No Content → no studies for given filters
        if status == 204:
            inv = payload.get("investigators")
            if inv:
                warnings.warn(
                    "No studies found for investigator(s): "
                    f"{inv}. NOAA expects 'LastName, Initials'. Try variations like:\n"
                    "  - 'LastName, Initials'\n  - 'LastName'\n  - 'Initials'"
                )
                # Nothing to parse; return display summary (empty) or None
                return self.get_summary() if kwargs.get("display") else None
        # Non-204: ensure success and parse JSON
        try:
            resp.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"HTTP error from NOAA API: {e}")

        try:
            response_json = resp.json()
        except Exception as e:
            raise RuntimeError(f"Failed to parse NOAA response as JSON: {e}")

        # Parse into internal structures (you already have this)
        self._parse_response(response_json, kwargs.get("limit"))

        return self.get_summary() if kwargs.get("display") else log.info(f"Parsed {len(self.studies)} studies.")
        

    def _parse_response(self, data, limit):
        """
        Parse the JSON response and populate studies and reverse mapping indexes.
        """
        from tqdm import tqdm
        
        self.studies.clear()
        self.data_table_index.clear()
        self.file_url_to_datatable.clear()

        for study_data in tqdm(data.get('study', []), desc="Parsing NOAA studies"):
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
            
        if isinstance(limit, int) and len(data.get('study', [])) >= limit:
            warnings.warn(
                f"Retrieved {limit} studies, which is the specified limit. "
                "Consider increasing the limit parameter to fetch more studies."
            )


    def get_summary(self):
        """
        Get a DataFrame summarizing all loaded studies.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with a summary of study metadata and components.
        
        Examples
        --------

        Examples
        --------

        .. jupyter-execute::

            from pyleotups import Dataset
            ds=Dataset()
            df = ds.search_studies(noaa_id=33213)
            df.head()
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
        
        Examples
        --------

        .. jupyter-execute::

            from pyleotups import Dataset
            ds=Dataset()
            dsf = ds.search_studies(noaa_id=33213)
            bib, df = ds.get_publications() 
            df.head()
        
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
        
        Examples
        --------

        .. jupyter-execute::

            from pyleotups import Dataset
            ds=Dataset()
            dsf = ds.search_studies(noaa_id=33213)
            df = ds.get_tables()
            df.head()
        """

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
        
        Examples
        --------

        .. jupyter-execute::

            from pyleotups import Dataset
            ds=Dataset()
            dsf = ds.search_studies(noaa_id=33213)
            df = ds.get_geo()
            df.head()
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


    def get_funding(self):
        """
        Get a DataFrame of all funding records across loaded studies.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with columns ['StudyID', 'StudyName', 'FundingAgency', 'FundingGrant'].
            Returns an empty DataFrame if no funding is available.
        
        Examples
        --------

        .. jupyter-execute::

            from pyleotups import Dataset
            ds=Dataset()
            dsf = ds.search_studies(noaa_id=33213)
            df = ds.get_funding()
            df.head()
        """
        records = []
        for study in self.studies.values():
            study_id = study.study_id
            study_name = study.metadata.get("studyName")

            for fund in study.funding:
                if isinstance(fund, dict):
                    records.append({
                        "StudyID": study_id,
                        "StudyName": study_name,
                        "FundingAgency": fund.get("fundingAgency", None),
                        "FundingGrant": fund.get("fundingGrant", None)
                    })

        return pd.DataFrame(records, columns=["StudyID", "StudyName", "FundingAgency", "FundingGrant"])
    

    def get_variables(self, dataTableIDs):
        """
        Retrieve variable metadata for specified dataTableIDs.

        Parameters
        ----------
        dataTableIDs : list or str
            One or more NOAA dataTableIDs.

        Returns
        -------
        pandas.DataFrame
            A DataFrame indexed by DataTableID with one row per (file × variable).
            Includes full variable metadata such as cvShortName, cvUnit, etc.
        
        Examples
        --------

        .. jupyter-execute::

            from pyleotups import Dataset
            ds=Dataset()
            dsf = ds.search_studies(noaa_id=33213)
            df_var = ds.get_variables(dataTableIDs="45859")
            df_var.head()
        """
        dataTableIDs = assert_list(dataTableIDs)
        records = []

        for dt_id in dataTableIDs:
            mapping = self.data_table_index.get(dt_id)
            if not mapping:
                raise ValueError(f"DataTableID '{dt_id}' not found. Please run `search_studies` first.")

            paleo = mapping["paleo_data"]
            study_id = paleo.study_id
            site_id = paleo.site_id

            for file in paleo.files:
                file_url = file.get("fileUrl")
                if not file_url:
                    continue

                var_map = paleo.file_variable_map.get(file_url, {})
                for var_name, var_meta in var_map.items():
                    records.append({
                        "DataTableID": dt_id,
                        "StudyID": study_id,
                        "SiteID": site_id,
                        "FileURL": file_url,
                        "VariableName": var_name,
                        **var_meta  # includes all cv* fields
                    })

        df = pd.DataFrame(records)
        if df.empty:
            return pd.DataFrame(columns=["StudyID", "SiteID", "FileURL", "VariableName"])  # fallback for no data

        return df.set_index("DataTableID")

    @DeprecationWarning
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
        Process a single file URL: detect parser, parse the file, and attach metadata.
        """
        if not file_url:
            raise ValueError("File URL is missing.")

        file_type = file_url.split('.')[-1].lower()
        if file_type in self._PROPRIETARY_TYPES:
            raise UnsupportedFileTypeError(
                f"pyleotups works with .txt files only. File type '{file_type}' is proprietary."
            )
        if file_type != 'txt':
            raise UnsupportedFileTypeError(
                f"Invalid file type '{file_type}'. Only .txt files are supported."
            )

        # Step 1: Detect parser type by reading initial lines
        import requests
        def detect_parser_type(lines):
            lines = [line.strip() for line in lines if line.strip()]
            if all(line.startswith("#") for line in lines[:5]):
                return "standard"
            for i in range(len(lines) - 4):
                line_normalized = lines[i+1].lower()
                if (
                    (("world data center for paleoclimatology" in line_normalized
                    and "noaa" in lines[i+3].lower()) or 
                    ("noaa" in line_normalized
                    and "world data center for paleoclimatology" in lines[i+3].lower()))
                    and "-" in lines[i] and "-" in lines[i+4]
                ):
                    return "nonstandard"
            return "unparsable"
        
        try:
            response = requests.get(file_url)
            response.raise_for_status()
            lines = response.text.splitlines()
            parser_type = detect_parser_type(lines)
        except Exception as e:
            raise RuntimeError(f"Failed to read file from '{file_url}': {e}")

        # Step 2: Use the appropriate parser
        if parser_type == "standard":
            parser = StandardParser(file_url)
        elif parser_type == "nonstandard":
            parser = NonStandardParser(file_url)
        else:
            raise ValueError(
                f"Unable to determine parser for file: {file_url}. "
            )

        try:
            parsed_data = parser.parse()
        except Exception as e:
            raise RuntimeError(f"Error while parsing file {file_url}: {e}")

        # Step 3: Attach metadata
        def attach_metadata(df, mapping):
            df.attrs['NOAAStudyId'] = mapping.get('study_id')
            study_obj = self.studies.get(mapping.get('study_id'), {})
            df.attrs['StudyName'] = study_obj.metadata.get("studyName") if hasattr(study_obj, 'metadata') else None
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
        
        Examples
        --------

        .. jupyter-execute::

            from pyleotups import Dataset
            ds=Dataset()
            df = ds.search_studies(noaa_id=33213)
            dfs = ds.get_data(dataTableIDs="45859")
            dfs[0].head()
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