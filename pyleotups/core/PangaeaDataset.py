from .BaseDataset import BaseDataset

from typing import Any, Dict, List, Optional, Tuple
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
    - search_studies(q=..., bbox=..., keywords=..., limit=..., offset=...) registers studies
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

    # -------------------------
    # search_studies: q, bbox, keywords -> registers studies and returns same style as Dataset.search_studies (DataFrame)
    # -------------------------
    def search_studies(self,
                   q: Optional[str] = None,
                   bbox: Optional[Tuple[float, float, float, float]] = None,
                   limit: int = 10,
                   offset: int = 0,
                   display: bool = False) -> Optional[pd.DataFrame]:
        """
        Search PANGAEA and register results in self.studies.

        Behavior:
        - Populates self.studies (StudyID -> {'panobj': None|'PanDataSet', 'summary': minimal})
        - Does NOT return the DataFrame by default (returns None).
        - If display=True, returns the full normalized summary DataFrame from self.get_summary().

        Args:
            q: free-text query
            bbox: geographical bounding box (minlon,minlat,maxlon,maxlat)
            limit, offset: paging
            display: if True, return get_summary() after populating registry

        Returns:
            None by default, or pandas.DataFrame (same shape as Dataset.get_summary()) if display=True.
        """
        # build query string
        q_parts = []
        if q:
            q_parts.append(q)
        query_str = " ".join(q_parts).strip() or ""

        try:
            pq = PanQuery(query=query_str, bbox=bbox, limit=limit, offset=offset)
        except Exception as exc:
            logger.exception("PanQuery failed")
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

    
        # Only return if user explicitly asked for display
        if display:
            return self.get_summary()
        

    # -------------------------
    # get_summary(): returns DataFrame of ALL registered studies (same shape as Dataset.get_summary())
    # -------------------------
    def get_summary(self) -> pd.DataFrame:
        """
        Return a DataFrame summarizing all loaded/registered PANGAEA datasets.
        Columns mirror NOAADataset.to_dict() keys:
          ["StudyID","StudyName","EarliestYearBP","MostRecentYearBP",
           "EarliestYearCE","MostRecentYearCE","StudyNotes","ScienceKeywords","Investigators",
           "Publications","Sites","Funding"]
        """
        rows = [study.to_summary_dict() for study in self.studies.values()]
        return pd.DataFrame(rows)

    # -------------------------
    # get_geo(): per-site DataFrame like Dataset.get_geo()
    # -------------------------
    def get_geo(self) -> pd.DataFrame:
        """
        Return a DataFrame with site-level rows and columns:
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
    def get_data(self, study_id) -> pd.DataFrame:
        """
        Fetch the data table for the given identifier (DOI/URI or direct file URL).
        Always returns a pandas.DataFrame (parsed). On failure raises ValueError / requests exceptions.

        Sets df.attrs["source"] = {'publisher':'PANGAEA','id':identifier, ...}
        Also registers PanDataSet instance in self.studies[identifier]['panobj'] for reuse.

        Parameters
        ----------
        study_id : str
            Identifier of the study.

        Returns
        -------
        pandas.DataFrame
            Dataset table.
        """
        
        if study_id not in self.studies:
            raise KeyError(f"Study '{study_id}' not found.")
        return self.studies[study_id].get_data()


    # -------------------------
    # translator stub
    # -------------------------
    def convert_tups_to_pangaea(self, tups_query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Stub for TUPS -> PANGAEA query translation.
        """
        return {}



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
