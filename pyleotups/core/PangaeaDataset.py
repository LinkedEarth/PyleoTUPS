from .BaseDataset import BaseDataset

from typing import Any, Dict, List, Optional, Tuple, Union
import logging
import io
import os

import pandas as pd
import requests

import warnings
import datetime
from pybtex.database import BibliographyData, Entry, Person
from pybtex.database.output.bibtex import Writer

from pangaeapy.pandataset import PanDataSet, PanEvent

from ..utils.PangaeaStudy import PangaeaStudy

from ..utils.api.query_builder import build_pangaea_query

logging.getLogger("pangaeapy").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

# try to import pangaeapy; raise helpful error if missing
try:
    from pangaeapy import PanQuery, PanDataSet
except Exception as exc:
    PanQuery = None
    PanDataSet = None
    _PANGAEA_IMPORT_ERROR = exc

import re
_DOI_RE = re.compile(r'(10\.\d{4,9}/\S+)', re.IGNORECASE)
_YEAR_RE = re.compile(r'\b(19|20)\d{2}\b')

class PangaeaDataset(BaseDataset):
    """
    PangaeaDataset: lightweight provider that mirrors pyleotups.core.Dataset responses.

    Notes:
    - search_studies(**kwargs) registers studies
    in self.studies (StudyID -> {'panobj': PanDataSet|None, 'summary': normalized_dict})
    - get_summary() returns a pandas.DataFrame exactly matching NOAA Dataset.to_dict() column names.
    - get_publications(), get_geo(), get_funding() return DataFrames with the same column names
    as the original Dataset methods. Missing data produces empty DataFrames / NaNs.
    - get_data(identifier) returns a pandas.DataFrame (parsed table) and sets df.attrs["source"].

    This file intentionally mirrors the structure used in NOAADataset.py and NOAAStudy.py
    """
    def __init__(self, cache_dir: Optional[str] = None, auth_token: Optional[str] = None):
        if PanQuery is None or PanDataSet is None:
            raise ImportError(
                "pangaeapy is required. Install via `pip install pangaeapy`.\n"
                f"Import error: {_PANGAEA_IMPORT_ERROR}"
            )
        self.cache_dir = cache_dir
        self.auth_token = auth_token

        # Registry mirroring pyleotups.core.NOAADataset.studies
        # keys: StudyID (DOI/URI) -> {'panobj': PanDataSet or None, 'summary': normalized_dict}
        self.studies: Dict[str, PangaeaStudy] = {}

    @staticmethod
    def _normalize_id(study_id: str) -> int:
        """
        Extract numeric PANGAEA ID from DOI or URI string.

        Examples
        --------
        'doi.pangaea.de/10.1594/PANGAEA.830587'
        → 830587
        """
        match = re.search(r"PANGAEA\.(\d+)", str(study_id))
        if match:
            return int(match.group(1))

        # Fallback: assume already numeric
        return int(study_id)
    
    def _resolve_and_register_ids(self, study_ids):
        """
        Normalize and register study IDs.

        Parameters
        ----------
        study_ids : int, str, or list
            One or more StudyIDs (numeric or DOI string).

        Returns
        -------
        list
            List of normalized numeric StudyIDs.
        """

        if not isinstance(study_ids, (list, tuple)):
            study_ids = [study_ids]

        normalized_ids = [self._normalize_id(sid) for sid in study_ids]

        for sid in normalized_ids:

            # Already registered
            if sid in self.studies:
                continue

            # Check if sid belongs to any registered collection
            for parent in self.studies.values():
                members = parent._panobj.collection_members
                if members:
                    normalized_members = [
                        self._normalize_id(m) for m in members
                    ]
                    if sid in normalized_members:
                        logger.info(
                            f"Study {sid} found as collection member. "
                            f"Registering child dataset."
                        )
                        self.studies[sid] = PangaeaStudy(
                            study_id=sid,
                            cache_dir=self.cache_dir,
                            auth_token=self.auth_token,
                        )
                        break
            else:
                # Not in registry, not in collection → direct load
                logger.info(
                    f"Registering Study {sid} via direct lookup."
                )
                self.studies[sid] = PangaeaStudy(
                    study_id=sid,
                    cache_dir=self.cache_dir,
                    auth_token=self.auth_token,
                )

        return normalized_ids

    # -------------------------
    # search_studies: q, bbox, keywords -> registers studies and returns same style as Dataset.search_studies (DataFrame)
    # -------------------------
    def search_studies(self,
                #    q: Optional[str] = None,
                #    study_ids: Optional[Union[int, str, List]] = None,
                #    bbox: Optional[Tuple[float, float, float, float]] = None,
                #    limit: int = 10,
                #    offset: int = 0,
                #    display: bool = False
                **kwargs) -> Optional[pd.DataFrame]:
        """
        Search PANGAEA and register results in self.studies.

        Behavior:
        - Populates self.studies (StudyID -> {'panobj': None|'PanDataSet', 'summary': minimal})
        - Does NOT return the DataFrame by default (returns None).
        - If display=True, returns the full normalized summary DataFrame from self.get_summary().

        Search for PANGAEA datasets using unified PyleoTUPS query parameters.

        This method translates user-friendly query parameters into a PANGAEA-compatible
        search query and registers the resulting datasets internally.

        Parameters
        ----------
        study_ids : int, str, or list, optional
            One or more PANGAEA dataset identifiers (numeric ID or DOI string).
            If provided, performs direct lookup and ignores other filters.

        search_text : str, optional
            Free-text search query applied across dataset metadata. Maps to PANGAEA full-text search parameter 'q'.
            Example: 'stable carbon and oxygen isotopes'.

        investigators : str or list[str], optional
            Author names. Mapped internally to PANGAEA query syntax:
            ``author:<name>``

        variable_name : str or list[str], optional
            Name of parameters/variables (columns) present in dataset tables.
            Internally mapped to PANGAEA query term:
            ``parameter:<variable_name>``

        min_lat, max_lat : float, optional
            Latitude bounds (–90..90).

        min_lon, max_lon : float, optional
            Longitude bounds (–180..180)

        limit : int, default 100, maximum 500
            Maximum number of results returned.

        skip : int, default 0
            Number of results to skip (pagination). Maps to PANGAEA 'offset'


        Returns
        -------
        pandas.DataFrame
            DataFrame summarizing matched datasets. Also populates internal registry.

        Raises
        ------
        ValueError
            If no valid search parameters are provided.

        Notes
        -----
        
        PANGAEA search is text-based and less structured than NOAA filters.
        Results may vary depending on metadata completeness.

        **Unified query interface.**
        PyleoTUPS uses consistent parameter names across datasets:
        
        - ``variable_name`` → mapped to ``parameter:`` in PANGAEA
        - ``investigators`` → mapped to ``author:``
        
        **Query construction.**
        If ``q`` is not provided, a query string is constructed by combining:
        - search_text
        - investigators
        - variable_name
        - keywords

        **Geospatial filtering.**
        Bounding box requires all four parameters:
        ``min_lat, max_lat, min_lon, max_lon``.
        Partial inputs are ignored.

        **Identifier priority.**
        If ``study_ids`` is provided, all other filters are ignored.

        **Multi-value parameters.**
        Multiple values for parameters like `variable_name` or `investigators`
        are combined into a space-separated query, interpreted as logical AND
        by the PANGAEA search engine.

        Examples
        --------

        Quick Start - Identifier Based search
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

        .. jupyter-execute::

            
            import pyleotups as pt
            ds = pt.PangaeaDataset()

    
            ### Can use either DOI strings or numeric IDs (extracted from DOIs)
            df = ds.search_studies(
                study_ids=["10.1594/PANGAEA.830587", "10.1594/PANGAEA.830588"]
            )
            df.head()

            df = ds.search_studies(
                study_ids=[830587, 830588]
            )
            df.head()


        Basic search
        ^^^^^^^^^^^^

        .. jupyter-execute::

            df = ds.search_studies(search_text="Stable oxygen and carbon isotopes", limit = 5)
            df.head()

        Variable-based search
        ^^^^^^^^^^^^^^^^^^^^^

        .. jupyter-execute::

            df = ds.search_studies(variable_name=["Pulleniatina obliquiloculata δ13C", "Pulleniatina obliquiloculata δ18O"], limit = 5)
            df.head()

        Investigator/Author-based search
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

        .. jupyter-execute::

            df = ds.search_studies(investigators=["Khider, D"], limit = 5)
            df.head()

        Combined filters
        ^^^^^^^^^^^^^^^^

        .. jupyter-execute::

            df = ds.search_studies(
                search_text="Stable oxygen and carbon isotopes",
                variable_name=["Pulleniatina obliquiloculata δ13C", "Pulleniatina obliquiloculata δ18O"],
                investigators="Khider, D",
                limit = 5
            )
            df.head()

        Geographic filtering
        ^^^^^^^^^^^^^^^^^^^^

        .. jupyter-execute::

            df = ds.search_studies(
                min_lat=-10, max_lat=10,
                min_lon=120, max_lon=160
            )
            df.head()
        """
        study_ids = kwargs.get("study_ids")
        q = kwargs.get("search_text")

        # -------------------------------------------
        # MODE 1: STUDY IDS (HIGHEST PRIORITY)
        # -------------------------------------------
        if study_ids is not None:

            # Prevent mixing modes
            if any([
                kwargs.get("search_text"),
                kwargs.get("investigators"),
                kwargs.get("variable_name"),
                kwargs.get("min_lat"),
                kwargs.get("max_lat"),
                kwargs.get("min_lon"),
                kwargs.get("max_lon"),
                q
            ]):
                logger.warning(
                    "Using identifier-only fetch (Pangaea DOI). Other parameters will be ignored.."
                )

            self._resolve_and_register_ids(kwargs.get("study_ids"))

            logger.info(f"Retrived {len(self.studies)} studies")

            return self.get_summary() 
            # if display else logger.info(f"Retrived {len(self.studies)} studies")   

        if not any([
                kwargs.get("search_text"),
                kwargs.get("investigators"),
                kwargs.get("variable_name"),
                kwargs.get("min_lat"),
                kwargs.get("max_lat"),
                kwargs.get("min_lon"),
                kwargs.get("max_lon"),
                q
            ]):
            raise ValueError(
                "At least one search parameter must be specified to initiate a query. "
                "To view available parameters and usage examples, run: help(PangaeaDataset.search_studies)"
            )

        params = build_pangaea_query(**kwargs)

        try:
            pq = PanQuery(
                query = params["q"], 
                bbox = params["bbox"], 
                limit = params["limit"], 
                offset = params["offset"])
        except Exception as exc:
            logger.exception(f"PanQuery failed due to {exc}")
            raise

        # register results in self.studies but do not accumulate into a dataframe here
        for res in pq.result:
            raw_id = res.get("URI") or res.get("uri") or res.get("id") or res.get("doi") or None
            sid = self._normalize_id(raw_id)
            if raw_id is None:
                sid = res.get("title") or f"pangaea_unidentified_{len(self.studies) + 1}"
            if sid not in self.studies:
                self.studies[sid] = PangaeaStudy(
                    study_id=sid,
                    cache_dir=self.cache_dir,
                    auth_token=self.auth_token,
                )

        logger.info(f"Retrived {len(self.studies)} studies")

        return self.get_summary() 
        # if display else logger.info(f"Retrived {len(self.studies)} studies")
        

    # -------------------------
    # get_summary(): returns DataFrame of ALL registered studies (same shape as Dataset.get_summary())
    # -------------------------
    def get_summary(self) -> pd.DataFrame:
        """
        Retrieve summary metadata for all registered studies.

        Returns
        -------
        pandas.DataFrame
            Return a DataFrame summarizing all loaded/registered PANGAEA datasets.
            ["StudyID","StudyName","EarliestYearBP","MostRecentYearBP",
            "EarliestYearCE","MostRecentYearCE","StudyNotes","ScienceKeywords","Investigators",
            "Publications","Sites","Funding"]
        """
        rows = []
        collection_found = []

        for study in self.studies.values():
            if study._panobj.isCollection:
                collection_found.append(study.study_id)
            rows.append(study.to_summary_dict())

        if collection_found:
            logger.warning(
                f"The search contains dataset(s) [{', '.join(map(str, collection_found))}] marked as collection. "
                "Refer to the 'CollectionMembers' column to"
                "identify respective child datasets."
            )

        return pd.DataFrame(rows)

    # -------------------------
    # get_geo(): per-site DataFrame like Dataset.get_geo()
    # -------------------------
    def get_geo(self) -> pd.DataFrame:
        """
        Retrieve geographic information for all studies.

        Returns
        -------
        pandas.DataFrame
            Combined geographic metadata.
            ['StudyID','SiteID','SiteName','LocationName','Latitude','Longitude','MinElevation','MaxElevation','DataType']
            If PANGAEA lacks site-level metadata, returns an empty DataFrame.
        """
        frames = [study.get_geo() for study in self.studies.values()]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # -------------------------
    # get_publications(): aggregated DataFrame per study
    # -------------------------
    def get_publications(
        self,
        save: bool = False,
        path: Optional[str] = None,
        verbose: bool = False,
    ):
        """
        Retrieve publication information in BibTeX and DataFrame format.

        Parameters
        ----------
        save : bool, default=False
            If True, save BibTeX file.
        path : str or None, optional
            Output path.
        verbose : bool, default=False
            Print BibTeX content.

        Returns
        -------
        tuple
            (BibliographyData, pandas.DataFrame)
        """
        all_rows = []
        all_entries = {}
        idx = 0

        for study in self.studies.values():
            rows, entries = study._extract_publications()
            all_rows.extend(rows)

            for k, v in entries.items():
                idx += 1
                all_entries[f"{k}_{idx}"] = v

        df = pd.DataFrame(all_rows)

        bibs = BibliographyData(entries=all_entries)

        if save:
            if not path:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
                path = f"bibtex_{timestamp}.bib"

            if os.path.isdir(path):
                path = os.path.join(
                    path,
                    f"pangaea_publications_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.bib",
                )

            writer = Writer()
            with open(path, "w", encoding="utf-8") as fh:
                writer.write_stream(bibs, fh)

        if verbose:
            writer = Writer()
            s = io.StringIO()
            writer.write_stream(bibs, s)
            print(s.getvalue())

        return bibs, df


    # -------------------------
    # get_funding(): aggregated funding across studies
    # -------------------------
    def get_funding(self) -> pd.DataFrame:
        """
        Retrieve funding information for all registered studies.

        Returns
        -------
        pandas.DataFrame
            Combined funding DataFrame across studies.
            If no funding is available, returns empty DataFrame
            with standardized columns.
        """
        frames = [study.get_funding() for study in self.studies.values()]

        frames = [f for f in frames if not f.empty]

        if not frames:
            return pd.DataFrame(
                columns=["StudyID", "StudyName", "FundingAgency", "FundingGrant"]
            )

        return pd.concat(frames, ignore_index=True)
    

    # -------------------------
    # get_data(identifier): DOI or file URL -> pandas.DataFrame parsed table and set df.attrs["source"]
    # -------------------------
    def get_data(self, study_id: int) -> pd.DataFrame:
        """
        Retrieve dataset for a specific study.

        If the study is a collection, a warning is logged
        suggesting access to its collection members.

        If the study is not registered but exists as a
        collection member of a registered study, it will
        be automatically loaded and registered.

        Parameters
        ----------
        study_id : int, str, or list
            One or more StudyIDs.

        Returns
        -------
        pandas.DataFrame or dict
            If single ID → DataFrame.
            If multiple IDs → dict of {StudyID: DataFrame}.
        """

        normalized_ids = self._resolve_and_register_ids(study_id)

        results = []

        for sid in normalized_ids:
            study = self.studies[sid]

            if study._panobj.isCollection:
                logger.warning(
                    f"Study {sid} is a collection dataset. Skipping."
                )
                continue

            results.append(study.get_data())

        return results

    # -------------------------
    # translator stub
    # -------------------------
    def convert_tups_to_pangaea(self, tups_query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Stub for TUPS -> PANGAEA query translation.
        """
        return {}

    def get_variables(self, study_ids=None) -> pd.DataFrame:
        """
        Retrieve variable metadata for specified studies.

        Parameters
        ----------
        study_ids : int, str, list, or None
            One or more StudyIDs. Can be numeric or DOI string.
            If None, variables for all registered studies are returned.

        Returns
        -------
        pandas.DataFrame
            One row per (study × variable).

        Raises
        ------
        KeyError
            If a requested StudyID is not registered and not found
            among collection members.
        """

        if study_ids is None:
            selected = list(self.studies.values())
        else:
            if not isinstance(study_ids, (list, tuple)):
                study_ids = [study_ids]

            selected = []

            for sid in study_ids:
                normalized_id = self._normalize_id(sid)

                # Directly registered
                if normalized_id in self.studies:
                    selected.append(self.studies[normalized_id])
                    continue

                # Check collection members
                found = False
                for parent in self.studies.values():
                    members = parent._panobj.collection_members
                    if members:
                        normalized_members = [
                            PangaeaStudy._normalize_id(m) for m in members
                        ]
                        if normalized_id in normalized_members:
                            # Auto-load and register
                            self.studies[normalized_id] = PangaeaStudy(
                                study_id=normalized_id,
                                cache_dir=self.cache_dir,
                                auth_token=self.auth_token,
                            )
                            selected.append(self.studies[normalized_id])
                            found = True
                            break

                if not found:
                    raise KeyError(
                        f"Study '{sid}' not found. "
                        f"Run search_studies() first."
                    )

        frames = [study.get_variables() for study in selected]

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)


if __name__ == "__main__":

    pg = PangaeaDataset()

    # Search (returns DataFrame)
    pg.search_studies(q="coral aragonite", bbox=(-180,-90,180,90), keywords=["Sr/Ca"], limit=10)

    hits_df = pg.search_studies(q="coral aragonite", bbox=(-180,-90,180,90), keywords=["Sr/Ca"], limit=10)
    
    print(hits_df.shape)
    print(hits_df.columns)
    # register is automatically populated
    print("Registry size:", len(pg.studies))

    # Pick an id from hits and fetch summary (DataFrame single-row)
    ident = hits_df.iloc[0]["id"]
    summary_df = pg.get_summary()
    print(summary_df.T)   # pretty inspect

    # get_geo (DataFrame or empty DataFrame)
    geo_df = pg.get_geo()
    print(geo_df)

    # get_publications -> DataFrame
    pubs_df = pg.get_publications()
    print(pubs_df)
    # get_data -> DataFrame (parsed table)
    data_df = pg.get_data()  # uses PanDataSet and returns pandas.DataFrame
    print(data_df)  # provenance
