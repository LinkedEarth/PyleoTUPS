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

    This file intentionally mirrors the structure used in Dataset.py and NOAADataset.py. See:
    - Dataset: :contentReference[oaicite:2]{index=2}
    - NOAADataset: :contentReference[oaicite:3]{index=3}
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
        self.studies: Dict[str, Dict[str, Any]] = {}

        # keep last PanQuery totalcount if available
        self._last_totalcount: Optional[int] = None

    # -------------------------
    # Internal normalizer (PanDataSet -> NOAA-like dict)
    # -------------------------
    
    def _extract_doi(self, identifier_or_uri: Optional[str]) -> Optional[str]:
        if not identifier_or_uri or not isinstance(identifier_or_uri, str):
            return None
        s = identifier_or_uri.strip()
        if s.startswith("<") and s.endswith(">"):
            s = s[1:-1].strip()
        if s.lower().startswith("doi:"):
            candidate = s[4:].strip()
            m = _DOI_RE.search(candidate)
            return m.group(1).rstrip('.,);') if m else candidate
        m = _DOI_RE.search(s)
        if m:
            return m.group(1).rstrip('.,);')
        if s.startswith("10.") and "/" in s:
            return s.split()[0]
        return None

    def _normalize_pan_dataset(self, ds: PanDataSet, identifier: str) -> Dict[str, Any]: # type: ignore
        """
        Return a dict that matches NOAADataset.to_dict() keys:
          "StudyID","XMLID","StudyName","DataType",
          "EarliestYearBP","MostRecentYearBP","EarliestYearCE","MostRecentYearCE",
          "StudyNotes","ScienceKeywords","Investigators","Publications","Sites","Funding"
        """
        # Basic identifiers
        study_id = getattr(ds, "uri", None) or getattr(ds, "doi", None) or identifier
        xml_id = None  # no XML concept in PANGAEA

        # Title
        study_name = getattr(ds, "title", None)

        # DataType / StudyNotes not usually present on PANGAEA
        data_type = None
        study_notes = getattr(ds, "abstract", None) or None

        # Temporal mapping: attempt to read mintimeextent / maxtimeextent
        earliest_bp = None
        latest_bp = None
        earliest_ce = None
        latest_ce = None
        try:
            min_t = getattr(ds, "mintimeextent", None)
            max_t = getattr(ds, "maxtimeextent", None)
            # Pangaea might provide ISO dates — leave mapping to user; here we keep raw values
            earliest_ce = min_t
            latest_ce = max_t
        except Exception:
            pass

        # Keywords: pangaeapy may provide subjects/keywords in ds.params or ds.keywords
        science_keywords = None
        try:
            kw = getattr(ds, "keywords", None)
            if kw:
                science_keywords = kw
            else:
                params = getattr(ds, "params", None)
                if params and isinstance(params, dict):
                    # heuristically pick common keyword fields
                    science_keywords = params.get("keywords") or params.get("subject")
        except Exception:
            science_keywords = None

        # Investigators: build comma-separated string similar to NOAADataset.investigators
        investigators = None
        try:
            authors = getattr(ds, "authors", None) or []
            if isinstance(authors, (list, tuple)) and authors:
                names = []
                for a in authors:
                    # attempt various attributes
                    name = getattr(a, "name", None) or getattr(a, "fullname", None) or str(a)
                    names.append(name)
                investigators = ", ".join(names)
        except Exception:
            investigators = None

        # Sites: PANGAEA may have geometric info or events; return list of site-like dicts (may be empty)
        sites = []
        try:
            # else attempt to use ds.events if present
            events = getattr(ds, "events", None)
            if events:
                # events may be an iterable of dicts with lat/lon
                for ev in events:
                    sites.append({
                        "SiteID": ev.id,
                        "LocationName": ev.location,
                        "Latitude": ev.latitude,
                        "Longitude": ev.longitude,
                        "Elevation": ev.elevation,
                    })
        except Exception:
            sites = sites or []

        # Funding: PANGAEA rarely stores funding metadata; try ds.params or ds.projects
                # Funding: prefer structured ds.projects if available
        funding = []
        try:
            projects = getattr(ds, "projects", None)
            if projects:
                # projects may be a list of project-like objects (dataclass/obj) or dicts
                for p in projects if isinstance(projects, (list, tuple)) else [projects]:
                    # p is likely an object with attributes
                    label = getattr(p, "label", None)
                    name = getattr(p, "name", None)
                    url = getattr(p, "URL", None) or getattr(p, "url", None)
                    award = getattr(p, "awardURI", None) or getattr(p, "awardUri", None) or getattr(p, "award", None)
                    pid = getattr(p, "id", None)

                    # Determine a readable fundingAgency and fundingGrant
                    # Use 'label' or 'name' as agency, and prefer award or URL or id as grant/identifier
                    
                    funding.append({
                        "url": url,
                        "fundingGrant": name,
                        "raw_project": {"label": label, "name": name, "URL": url, "awardURI": award, "id": pid}
                    })

        except Exception:
            # if anything unexpected happens, leave funding empty but don't raise
            funding = []

        raw_id = getattr(ds, "uri", None) or getattr(ds, "doi", None) or identifier
        study_id = self._extract_doi(raw_id) or raw_id

        normalized = {
            "StudyID": study_id,
            "StudyName": study_name,
            "EarliestYearBP": earliest_bp,
            "MostRecentYearBP": latest_bp,
            "EarliestYearCE": earliest_ce,
            "MostRecentYearCE": latest_ce,
            "StudyNotes": study_notes,
            "ScienceKeywords": science_keywords,
            "Investigators": investigators,
            "Publications": ds.citation,
            "Sites": sites,
            "Funding": funding, 
            "native": {
                "raw_uri": raw_id,
                "raw_object": ds,
            }
        }
        return normalized

    # -------------------------
    # search_studies: q, bbox, keywords -> registers studies and returns same style as Dataset.search_studies (DataFrame)
    # -------------------------
    def search_studies(self,
                   q: Optional[str] = None,
                   bbox: Optional[Tuple[float, float, float, float]] = None,
                   keywords: Optional[List[str]] = None,
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
            keywords: list of provider-specific keywords
            limit, offset: paging
            display: if True, return get_summary() after populating registry

        Returns:
            None by default, or pandas.DataFrame (same shape as Dataset.get_summary()) if display=True.
        """
        # build query string
        q_parts = []
        if q:
            q_parts.append(q)
        if keywords:
            q_parts.append(" ".join(keywords))
        query_str = " ".join(q_parts).strip() or ""

        try:
            pq = PanQuery(query=query_str, bbox=bbox, limit=limit, offset=offset)
        except Exception as exc:
            logger.exception("PanQuery failed")
            raise

        # register results in self.studies but do not accumulate into a dataframe here
        for res in pq.result:
            ident = res.get("URI") or res.get("uri") or res.get("id") or res.get("doi") or None
            if ident is None:
                ident = res.get("title") or f"pangaea_unidentified_{len(self.studies) + 1}"

            title = res.get("title")
            # minimal placeholder summary (will be expanded when get_summary() or get_data() is called)
            placeholder = {
                "StudyID": ident,
                "StudyName": title
            }

            # register placeholder if not present
            if ident not in self.studies:
                self.studies[ident] = {"panobj": None, "summary": placeholder}

        # record totalcount if available
        try:
            total = getattr(pq, "totalcount", None)
            self._last_totalcount = total
        except Exception:
            self._last_totalcount = None

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
          ["StudyID","XMLID","StudyName","DataType","EarliestYearBP","MostRecentYearBP",
           "EarliestYearCE","MostRecentYearCE","StudyNotes","ScienceKeywords","Investigators",
           "Publications","Sites","Funding"]
        """
        rows = []
        for sid, entry in self.studies.items():
            panobj = entry.get("panobj")
            summary = entry.get("summary")

            # if we already normalized, use it
            if summary and set(summary.keys()) >= {"StudyID", "StudyName"} and len(summary.keys()) >= 12:
                rows.append(summary)
                continue

            # else attempt to load PanDataSet if not present
            if panobj is None:
                try:
                    ds = PanDataSet(id=sid, include_data=False, auth_token=self.auth_token, cachedir=self.cache_dir)
                    panobj = ds
                    # register panobj
                    self.studies[sid]["panobj"] = ds
                except Exception:
                    # if load fails, create minimal summary
                    rows.append({
                        "StudyID": sid,
                        "XMLID": None,
                        "StudyName": summary.get("StudyName") if summary else None,
                        "DataType": None,
                        "EarliestYearBP": None,
                        "MostRecentYearBP": None,
                        "EarliestYearCE": None,
                        "MostRecentYearCE": None,
                        "StudyNotes": None,
                        "ScienceKeywords": None,
                        "Investigators": None,
                        "Publications": [],
                        "Sites": [],
                        "Funding": []
                    })
                    continue

            # now normalize PanDataSet to NOAA-like dict
            normalized = self._normalize_pan_dataset(panobj, sid)
            # store normalized summary
            self.studies[sid]["summary"] = normalized
            rows.append(normalized)

        df = pd.DataFrame(rows)
        return df

    # -------------------------
    # get_geo(): per-site DataFrame like Dataset.get_geo()
    # -------------------------
    def get_geo(self) -> pd.DataFrame:
        """
        Return a DataFrame with site-level rows and columns:
        ['StudyID','SiteID','SiteName','LocationName','Latitude','Longitude','MinElevation','MaxElevation','DataType']
        If PANGAEA lacks site-level metadata, returns an empty DataFrame.
        """
        rows = []
        for sid, entry in self.studies.items():
            panobj = entry.get("panobj")
            # ensure panobj available
            if panobj is None:
                try:
                    panobj = PanDataSet(id=sid, include_data=False, auth_token=self.auth_token, cachedir=self.cache_dir)
                    self.studies[sid]["panobj"] = panobj
                except Exception:
                    continue

            # attempt to extract geometry/events
            sites = []
            try:
                events = getattr(panobj, "events", None)
                if events:
                    for ev in events:
                        sites.append({
                            "SiteID": ev.id,
                            "SiteName": ev.label,
                            "LocationName": ev.location,
                            "Latitude": ev.latitude,
                            "Longitude": ev.longitude,
                            "MinElevation": None,
                            "MaxElevation": None
                        })
            except Exception as e:
                print(f"Warning: Failed to extract geometry/events for StudyID {sid}: {e}")
                sites = sites or []

            for s in sites:
                row = {
                    "StudyID": sid,
                    "SiteID": s.get("SiteID"),
                    "SiteName": s.get("SiteName"),
                    "LocationName": s.get("LocationName"),
                    "Latitude": s.get("Latitude"),
                    "Longitude": s.get("Longitude"),
                    "MinElevation": s.get("MinElevation"),
                    "MaxElevation": s.get("MaxElevation"),
                }
                rows.append(row)

        if not rows:
            # return empty frame with same columns as NOAA version
            return pd.DataFrame(columns=["StudyID", "SiteID", "SiteName", "LocationName",
                                         "Latitude", "Longitude", "MinElevation", "MaxElevation", "DataType"])
        return pd.DataFrame(rows)

    # -------------------------
    # get_publications(): aggregated DataFrame per study
    # -------------------------
    def get_publications(self, save: bool = False, path: Optional[str] = None, verbose: bool = False):
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

        # small helpers
        _DOI_RE = re.compile(r'(10\.\d{4,9}/\S+)', re.IGNORECASE)
        _YEAR_RE = re.compile(r'\b(19|20)\d{2}\b')

        def _extract_dois(s):
            if not s:
                return []
            return [m.rstrip('.,);') for m in _DOI_RE.findall(s)]

        def _extract_year(s):
            if not s:
                return None
            m = _YEAR_RE.search(s)
            return m.group(0) if m else None

        def _make_citation_key(base: str, idx: int):
            # sanitize base a little
            safe = base.replace("/", "_").replace(".", "_").replace(":", "_").replace(" ", "_")
            return f"{safe}_{idx}"

        def _split_authors(author_str: str):
            """
            Try to split author string into Person objects.
            Common separators: ';' (pangaea), ' and ', ',' sometimes — be conservative.
            """
            if not author_str:
                return []
            parts = [p.strip() for p in re.split(r';| and |, and |,', author_str) if p.strip()]
            persons = []
            for p in parts:
                try:
                    persons.append(Person(p))
                except Exception:
                    # fallback: raw string Person
                    persons.append(Person(str(p)))
            return persons

        rows = []
        bib_entries = {}
        bib_idx = 0

        # iterate over registered studies
        for sid, entry in self.studies.items():
            panobj = entry.get("panobj")

            # lazy load if needed
            if panobj is None:
                try:
                    panobj = PanDataSet(id=sid, include_data=False, auth_token=self.auth_token, cachedir=self.cache_dir)
                    self.studies[sid]["panobj"] = panobj
                except Exception:
                    # skip studies that fail to load
                    continue

            study_title = getattr(panobj, "title", None)
            citation = getattr(panobj, "citation", None) or getattr(panobj, "citationString", None)

            # If no citation string present, still add a dataset-row (fallback)
            if not citation:
                # create simple dataset publication row (fallback)
                dataset_doi = getattr(panobj, "doi", None) or getattr(panobj, "uri", None)
                rows.append({
                    "StudyID": sid,
                    "StudyName": study_title,
                    "Author": None,
                    "Title": study_title,
                    "Journal": "PANGAEA",
                    "Year": getattr(panobj, "year", None),
                    "Volume": None,
                    "Number": None,
                    "Pages": None,
                    "Type": "dataset",
                    "DOI": dataset_doi,
                    "URL": getattr(panobj, "uri", None),
                    "CitationKey": None
                })
                # also create a minimal bib entry for dataset DOI if present
                if dataset_doi:
                    bib_idx += 1
                    key = _make_citation_key(dataset_doi, bib_idx)
                    fields = {"title": study_title or "", "year": str(getattr(panobj, "year", ""))}
                    if dataset_doi:
                        fields["doi"] = dataset_doi
                    if getattr(panobj, "uri", None):
                        fields["url"] = getattr(panobj, "uri")
                    entry_obj = Entry("misc", fields=fields)
                    bib_entries[key] = entry_obj
                continue

            # ---------------------
            # Dataset row (from citation string)
            # ---------------------
            dataset_dois = _extract_dois(citation)
            dataset_doi = dataset_dois[0] if dataset_dois else (getattr(panobj, "doi", None) or getattr(panobj, "uri", None))
            dataset_year = _extract_year(citation) or getattr(panobj, "year", None)
            # author portion is typically before the first '(' year ')', take that heuristic
            author_part = None
            try:
                author_part = citation.split("(")[0].strip().rstrip(":")
            except Exception:
                author_part = None

            rows.append({
                "StudyID": sid,
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
                "URL": getattr(panobj, "uri", None),
                "CitationKey": citation
            })

            # Build BibTeX entry for dataset row
            bib_idx += 1
            base_for_key = dataset_doi or (study_title or "pangaea_dataset")
            key = _make_citation_key(base_for_key, bib_idx)
            persons = _split_authors(author_part)
            fields = {"title": study_title or "", "year": str(dataset_year) if dataset_year else ""}
            if dataset_doi:
                fields["doi"] = dataset_doi
            if getattr(panobj, "uri", None):
                fields["url"] = getattr(panobj, "uri")
            entry_obj = Entry("misc", persons={"author": persons} if persons else {}, fields=fields)
            bib_entries[key] = entry_obj

            # ---------------------
            # Article/Supplement row: detect "Supplement to:" and parse
            # ---------------------
            if "Supplement to:" in citation:
                article_part = citation.split("Supplement to:")[-1].strip()
                # extract article DOI(s) and year and a journal string heuristically
                article_dois = _extract_dois(article_part)
                article_doi = article_dois[-1] if article_dois else None
                article_year = _extract_year(article_part)
                # attempt to heuristically extract journal and pages and volume/issue
                journal = None
                volume = None
                number = None
                pages = None
                try:
                    # split by commas, often: "Title. Journal, 38(6), 613-621, DOI"
                    parts = [p.strip() for p in article_part.split(",")]
                    if len(parts) >= 2:
                        # choose the part that looks like a journal name (non-year)
                        journal = parts[1]
                    # look for vol(issue)
                    m = re.search(r'(\d+)\((\d+)\)', article_part)
                    if m:
                        volume = m.group(1)
                        number = m.group(2)
                    # pages
                    pm = re.search(r'(\d+\s*-\s*\d+)', article_part)
                    if pm:
                        pages = pm.group(1)
                except Exception:
                    pass

                rows.append({
                    "StudyID": sid,
                    "StudyName": study_title,
                    "Author": None,
                    "Title": article_part,
                    "Journal": journal,
                    "Year": article_year,
                    "Volume": volume,
                    "Number": number,
                    "Pages": pages,
                    "Type": "article",
                    "DOI": article_doi,
                    "URL": None,
                    "CitationKey": article_part
                })

                # create bib entry for article
                bib_idx += 1
                key_article = _make_citation_key(article_doi or (journal or "article"), bib_idx)
                article_fields = {"title": article_part or "", "year": str(article_year) if article_year else ""}
                if article_doi:
                    article_fields["doi"] = article_doi
                if journal:
                    article_fields["journal"] = journal
                if volume:
                    article_fields["volume"] = str(volume)
                if pages:
                    article_fields["pages"] = pages
                entry_article = Entry("article", fields=article_fields)
                bib_entries[key_article] = entry_article

        # Build DataFrame with expected columns
        cols = ["StudyID","StudyName","Author","Title","Journal","Year","Volume","Number","Pages","Type","DOI","URL","CitationKey"]
        df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

        # Build BibliographyData
        bibs = BibliographyData(entries=bib_entries)

        # Save if requested
        if save:
            if not path:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
                path = f"bibtex_{timestamp}.bib"
                warnings.warn(f"No path specified. Saving BibTeX to: {path}")
            # if given a directory, construct filename
            if os.path.isdir(path):
                path = os.path.join(path, f"pangaea_publications_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.bib")
            try:
                writer = Writer()
                with open(path, "w", encoding="utf-8") as fh:
                    writer.write_stream(bibs, fh)
            except Exception as e:
                raise IOError(f"Failed to write BibTeX file to '{path}': {e}")

        # Verbose: print BibTeX content
        if verbose:
            try:
                writer = Writer()
                s = io.StringIO()
                writer.write_stream(bibs, s)
                print(s.getvalue())
            except Exception:
                # fallback: print representation
                print(bibs)

        return bibs, df


    # -------------------------
    # get_funding(): aggregated funding across studies
    # -------------------------
    def get_funding(self) -> pd.DataFrame:
        """
        Return a DataFrame of funding records: ['StudyID','StudyName','FundingAgency','FundingGrant'].
        If PANGAEA lacks funding metadata, returns an empty DataFrame (columns preserved).
        """
        rows = []
        for sid, entry in self.studies.items():
            panobj = entry.get("panobj")
            if panobj is None:
                try:
                    panobj = PanDataSet(id=sid, include_data=False, auth_token=self.auth_token, cachedir=self.cache_dir)
                    self.studies[sid]["panobj"] = panobj
                except Exception:
                    continue

            try:
                projects = getattr(panobj, "projects", None)
                if projects:
                    for p in projects if isinstance(projects, (list, tuple)) else [projects]:
                        grant = ""
                        if p.label:    
                            grant += p.label
                        if p.id:
                            grant += f" / {p.id}" 
                        rows.append({
                            "StudyID": sid,
                            "StudyName": getattr(panobj, "title", None),
                            "FundingAgency": getattr(p, "URL", None) or getattr(p, "url", None),
                            "FundingGrant": grant if grant else None,
                        })
            except Exception as e:
                print(f"Error occurred while processing funding for study {sid}: {e}")
                continue

        if not rows:
            return pd.DataFrame(columns=["StudyID","StudyName","FundingAgency","FundingGrant"])
        return pd.DataFrame(rows, columns=["StudyID","StudyName","FundingAgency","FundingGrant"])

    # -------------------------
    # get_data(identifier): DOI or file URL -> pandas.DataFrame parsed table and set df.attrs["source"]
    # -------------------------
    def get_data(self, identifier: str, parse: bool = True, include_data: bool = True) -> pd.DataFrame:
        """
        Fetch the data table for the given identifier (DOI/URI or direct file URL).
        Always returns a pandas.DataFrame (parsed). On failure raises ValueError / requests exceptions.

        Sets df.attrs["source"] = {'publisher':'PANGAEA','id':identifier, ...}
        Also registers PanDataSet instance in self.studies[identifier]['panobj'] for reuse.
        """
        is_doi = ("10." in identifier) or (identifier.isdigit() if isinstance(identifier, str) else False)

        if is_doi:
            # reuse or fetch PanDataSet
            reg = self.studies.get(identifier)
            panobj = (reg and reg.get("panobj")) or None
            if panobj is None:
                try:
                    panobj = PanDataSet(id=identifier, include_data=include_data, auth_token=self.auth_token, cachedir=self.cache_dir)
                    # register
                    self.studies[identifier] = {"panobj": panobj, "summary": None}
                except Exception as exc:
                    logger.exception("Failed to load PanDataSet for %s", identifier)
                    raise

            # if PanDataSet.data exists, return it
            print("PanDataSet loaded for identifier:", identifier)
            if hasattr(panobj, "data") and panobj.data is not None:
                df = panobj.data.copy()
                print(df)
                df.attrs["source"] = {"publisher": "PANGAEA", "id": identifier, "fetched_with": "PanDataSet"}
                return df

            # otherwise attempt ds.download() and parse first CSV-like file
            try:
                files = panobj.download()
            except Exception:
                raise ValueError(f"No data available for identifier {identifier}")

            for fpath in files:
                if fpath.endswith((".csv", ".txt", ".tsv")):
                    try:
                        df = pd.read_csv(fpath)
                        df.attrs["source"] = {"publisher": "PANGAEA", "id": identifier, "saved_file": fpath}
                        return df
                    except Exception:
                        continue
            # if nothing parsed, raise
            raise ValueError(f"No parseable data files found for identifier {identifier}")

        # otherwise treat as direct URL
        try:
            resp = requests.get(identifier, timeout=(5, 30))
            resp.raise_for_status()
            content = resp.content
        except Exception as exc:
            logger.exception("Failed to download url: %s", identifier)
            raise

        if not parse:
            df = pd.DataFrame({"raw_bytes": [content]})
            df.attrs["source"] = {"publisher": "PANGAEA", "id": identifier}
            return df

        # heuristics: try CSV, TSV, Excel
        try:
            text = content.decode("utf-8")
            try:
                df = pd.read_csv(io.StringIO(text))
                df.attrs["source"] = {"publisher": "PANGAEA", "id": identifier}
                return df
            except Exception:
                try:
                    df = pd.read_csv(io.StringIO(text), sep="\t")
                    df.attrs["source"] = {"publisher": "PANGAEA", "id": identifier}
                    return df
                except Exception:
                    pass
        except Exception:
            pass

        # try excel
        try:
            df = pd.read_excel(io.BytesIO(content))
            df.attrs["source"] = {"publisher": "PANGAEA", "id": identifier}
            return df
        except Exception:
            logger.exception("Failed to parse content from %s", identifier)
            raise ValueError("Failed to parse content at %s" % identifier)

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
