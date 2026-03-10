import re
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
from pangaeapy import PanDataSet
from pybtex.database import Entry, Person

import logging
logger = logging.getLogger(__name__)

_DOI_RE = re.compile(r"(10\.\d{4,9}/\S+)", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


def _extract_dois(s: Optional[str]) -> List[str]:
    if not s:
        return []
    return [m.rstrip(".,);") for m in _DOI_RE.findall(s)]


def _extract_year(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    m = _YEAR_RE.search(s)
    return m.group(0) if m else None


def _make_citation_key(base: str, idx: int) -> str:
    safe = base.replace("/", "_").replace(".", "_").replace(":", "_").replace(" ", "_")
    return f"{safe}_{idx}"


def _split_authors(author_str: Optional[str]) -> List[Person]:
    if not author_str:
        return []
    parts = [p.strip() for p in re.split(r";| and |, and |,", author_str) if p.strip()]
    return [Person(p) for p in parts]


class PangaeaStudy:
    """
    Utility class representing a single PANGAEA study.

    This class wraps a persistent `pangaeapy.PanDataSet` instance and provides:
    - Lazy data loading
    - NOAA-style summary normalization
    - Geographic extraction
    - Deep publication parsing (including supplement handling)

    Parameters
    ----------
    study_id : str
        DOI, URI, or identifier of the PANGAEA dataset.
    cache_dir : str or None, optional
        Directory for pangaeapy cache.
    auth_token : str or None, optional
        PANGAEA authentication token for restricted datasets.
    """

    def __init__(
        self,
        study_id: str,
        cache_dir: Optional[str] = None,
        auth_token: Optional[str] = None,
    ):
        self.study_id = study_id
        self.cache_dir = cache_dir
        self.auth_token = auth_token

        self._panobj = PanDataSet(
            id=study_id,
            cachedir=cache_dir,
            auth_token=auth_token,
        )

    # ------------------------------------------------------------------
    # Data Handling
    # ------------------------------------------------------------------

    def get_data(self) -> pd.DataFrame:
        """
        Retrieve the dataset as a pandas DataFrame.

        Returns
        -------
        pandas.DataFrame
            Copy of the dataset table with metadata stored in ``df.attrs``.
        """
        df = self._panobj.data.copy()

        df.attrs["source"] = "PANGAEA"
        df.attrs["StudyID"] = self.study_id
        df.attrs["DOI"] = self._panobj.doi
        df.attrs["Citation"] = self._panobj.citation

        return df

    # ------------------------------------------------------------------
    # Summary Metadata
    # ------------------------------------------------------------------

    def _extract_temporal_extent(
        self,
    ) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
        """
        Extract temporal coverage from Age columns (CE and/or BP).

        1.Collect all CE-type columns
        2.Collect all BP-type columns
        3.Compute CE from CE columns
        4.Compute BP from BP columns    
        5.If one side missing → derive from the other
        6.If still missing → fallback to Date/Time column (if present)
    
            Returns
        -------
        tuple
            (EarliestYearBP, MostRecentYearBP,
            EarliestYearCE, MostRecentYearCE)
        """

        earliest_bp = latest_bp = None
        earliest_ce = latest_ce = None

        try:
            df = self._panobj.data
            params = self._panobj.params

            ce_values = []
            bp_values = []

            # --------------------------------------------------
            # Identify all age-related columns
            # --------------------------------------------------

            AGE_PATTERN = re.compile(r"\bage\b", re.IGNORECASE)
            EXCLUDE_PATTERN = re.compile(
                r"(error|std|deviation|uncertainty|comment|\be\b)",
                re.IGNORECASE
            )

            for col_name, param in params.items():

                name = param.name or ""
                short = param.shortName or ""
                unit = (param.unit or "").lower()
                # -----------------------------------------
                # Detect Age columns (word boundary safe)
                # -----------------------------------------
                if not (AGE_PATTERN.search(name) or AGE_PATTERN.search(short)):
                    continue

                # -----------------------------------------
                # Exclude uncertainty / error columns
                # -----------------------------------------
                if EXCLUDE_PATTERN.search(name) or EXCLUDE_PATTERN.search(short):
                    continue

                # if col_name not in df.columns:
                #     continue
                
                series = pd.to_numeric(df[col_name], errors="coerce").dropna()
                if series.empty:    
                    continue

                # -----------------------------------------
                # CE units
                # -----------------------------------------
                if re.search(r"\b(ad|ce)\b", unit):
                    ce_values.extend(series.tolist())

                # -----------------------------------------
                # BP units
                # -----------------------------------------
                if re.search(r"\bbp\b", unit):
                    if "ka" in unit:
                        bp_values.extend((series * 1000).tolist())
                    else:
                        bp_values.extend(series.tolist())
                
                
            # --------------------------------------------------
            # Compute CE if present
            # --------------------------------------------------
            if ce_values:
                ce_min = min(ce_values)
                ce_max = max(ce_values)

                earliest_ce = int(ce_min)
                latest_ce = int(ce_max)

            # --------------------------------------------------
            # Compute BP if present
            # --------------------------------------------------
            if bp_values:
                bp_min = min(bp_values)
                bp_max = max(bp_values)

                # In BP: larger = older
                earliest_bp = int(bp_max)
                latest_bp = int(bp_min)

            # --------------------------------------------------
            # Derive missing side if necessary
            # --------------------------------------------------
            if earliest_ce is not None and earliest_bp is None:
                earliest_bp = 1950 - latest_ce
                latest_bp = 1950 - earliest_ce

            if earliest_bp is not None and earliest_ce is None:
                earliest_ce = 1950 - earliest_bp
                latest_ce = 1950 - latest_bp

            # --------------------------------------------------
            # Fallback to Date/Time if nothing found
            # --------------------------------------------------
            if (
                earliest_ce is None
                and earliest_bp is None
                and "Date/Time" in df.columns
            ):
                years = pd.to_datetime(
                    df["Date/Time"], errors="coerce"
                ).dt.year.dropna()

                if not years.empty:
                    earliest_ce = int(years.min())
                    latest_ce = int(years.max())
                    earliest_bp = 1950 - latest_ce
                    latest_bp = 1950 - earliest_ce

        except Exception:
            pass

        return earliest_bp, latest_bp, earliest_ce, latest_ce

    def to_summary_dict(self) -> Dict[str, Any]:
        """
        Convert study metadata to NOAA-style summary dictionary.

        Returns
        -------
        dict
            Dictionary with standardized summary fields.
        """
        ds = self._panobj
        self.earliest_bp, self.latest_bp, self.earliest_ce, self.latest_ce = (
            self._extract_temporal_extent()
        )

        return {
            "StudyID": self.study_id,
            "StudyName": ds.title,
            "EarliestYearBP": self.earliest_bp,
            "MostRecentYearBP": self.latest_bp,
            "EarliestYearCE": self.earliest_ce,
            "MostRecentYearCE": self.latest_ce,
            "StudyNotes": ds.abstract,
            "ScienceKeywords": getattr(ds, "keywords", None),
            "Investigators": ", ".join(a.fullname for a in ds.authors),
            "Publications": ds.citation,
            "Sites": [e.label for e in ds.events],
            "Funding": [
                {"name": p.name, "url": p.URL, "award": p.awardURI}
                for p in ds.projects
            ],
            "CollectionMembers": (
                self._panobj.collection_members
                if self._panobj.isCollection
                else None
            ),
        }

    # ------------------------------------------------------------------
    # Geographic Information
    # ------------------------------------------------------------------

    def get_geo(self) -> pd.DataFrame:
        """
        Retrieve geographic metadata for study events.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing event-level geographic information.
        """
        rows = []
        for ev in self._panobj.events:
            rows.append(
                {
                    "StudyID": self.study_id,
                    "SiteID": ev.id,
                    "SiteName": ev.label,
                    "LocationName": ev.location,
                    "Latitude": ev.latitude,
                    "Longitude": ev.longitude,
                    "Elevation": ev.elevation,
                }
            )
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Publications
    # ------------------------------------------------------------------

    def _extract_publications(self):
        """
        Extract structured publication information.

        Returns
        -------
        tuple
            (list_of_rows, dict_of_bibtex_entries)
        """
        ds = self._panobj
        rows = []
        bib_entries = {}
        idx = 0

        study_title = ds.title
        citation = ds.citation or getattr(ds, "citationString", None)

        if not citation:
            dataset_doi = ds.doi or ds.uri
            rows.append(
                {
                    "StudyID": self.study_id,
                    "StudyName": study_title,
                    "Author": None,
                    "Title": study_title,
                    "Journal": "PANGAEA",
                    "Year": ds.year,
                    "Volume": None,
                    "Number": None,
                    "Pages": None,
                    "Type": "dataset",
                    "DOI": dataset_doi,
                    "URL": ds.uri,
                    "CitationKey": None,
                }
            )
            return rows, bib_entries

        dataset_dois = _extract_dois(citation)
        dataset_doi = dataset_dois[0] if dataset_dois else (ds.doi or ds.uri)
        dataset_year = _extract_year(citation) or ds.year

        author_part = citation.split("(")[0].strip().rstrip(":")

        rows.append(
            {
                "StudyID": self.study_id,
                "StudyName": study_title,
                "Author": author_part,
                "Title": study_title,
                "Journal": "PANGAEA",
                "Year": dataset_year,
                "Volume": None,
                "Number": None,
                "Pages": None,
                "Type": "dataset",
                "DOI": dataset_doi,
                "URL": ds.uri,
                "CitationKey": citation,
            }
        )

        idx += 1
        key = _make_citation_key(dataset_doi or study_title, idx)

        bib_entries[key] = Entry(
            "misc",
            persons={"author": _split_authors(author_part)},
            fields={
                "title": study_title or "",
                "year": str(dataset_year) if dataset_year else "",
                "doi": dataset_doi or "",
                "url": ds.uri or "",
            },
        )

        if "Supplement to:" in citation:
            article_part = citation.split("Supplement to:")[-1].strip()
            article_dois = _extract_dois(article_part)
            article_doi = article_dois[-1] if article_dois else None
            article_year = _extract_year(article_part)

            rows.append(
                {
                    "StudyID": self.study_id,
                    "StudyName": study_title,
                    "Author": None,
                    "Title": article_part,
                    "Journal": None,
                    "Year": article_year,
                    "Volume": None,
                    "Number": None,
                    "Pages": None,
                    "Type": "article",
                    "DOI": article_doi,
                    "URL": None,
                    "CitationKey": article_part,
                }
            )

        return rows, bib_entries
    
    def get_funding(self) -> pd.DataFrame:
        """
        Retrieve funding information for this study.

        Returns
        -------
        pandas.DataFrame
            DataFrame with columns:
            ['StudyID', 'StudyName', 'FundingAgency', 'FundingGrant'].

            If no funding metadata is available,
            returns an empty DataFrame with columns preserved.
        """
        ds = self._panobj
        rows = []

        projects = getattr(ds, "projects", None)

        if projects:
            for p in projects if isinstance(projects, (list, tuple)) else [projects]:
                grant = ""
                if getattr(p, "label", None):
                    grant += p.label
                if getattr(p, "id", None):
                    grant += f" / {p.id}"

                rows.append(
                    {
                        "StudyID": self.study_id,
                        "StudyName": ds.title,
                        "FundingAgency": getattr(p, "URL", None)
                                        or getattr(p, "url", None),
                        "FundingGrant": grant if grant else None,
                    }
                )

        if not rows:
            return pd.DataFrame(
                columns=["StudyID", "StudyName", "FundingAgency", "FundingGrant"]
            )

        return pd.DataFrame(
        rows,
        columns=["StudyID", "StudyName", "FundingAgency", "FundingGrant"],
    )

    def get_variables(self) -> pd.DataFrame:
        """
        Retrieve variable (parameter) metadata for this study.

        Returns
        -------
        pandas.DataFrame
            One row per parameter with the following columns:

            - StudyID
            - VariableName
            - ShortName
            - Unit
            - OntologyTerms

        Notes
        -----
        For collection datasets, this returns an empty DataFrame.
        """

        ds = self._panobj

        # Collections do not contain parameters
        if ds.isCollection:
            return pd.DataFrame(
                columns=[
                    "StudyID",
                    "VariableName",
                    "ShortName",
                    "Unit",
                    "OntologyTerms",
                ]
            )

        rows = []

        for col_name, param in ds.params.items():

            rows.append(
                {
                    "StudyID": self.study_id,
                    "VariableName": param.name,
                    "ShortName": param.shortName,
                    "Unit": param.unit,
                    "OntologyTerms": param.terms,
                }
            )

        return pd.DataFrame(rows)