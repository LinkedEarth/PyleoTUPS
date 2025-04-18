__all__ = ['NOAADataset']

from ..utils.Publication import Publication
from ..utils.Site import Site
from ..utils.PaleoData import PaleoData
import numpy as np

class NOAADataset:
    """
    This class encapsulates study metadata and its related components (e.g. publications,
    sites) retrieved from the NOAA API.

    Attributes
    ----------
    study_id : str
        The unique NOAA study identifier.
    xml_id : str
        The XML identifier of the study.
    metadata : dict
        A dictionary containing basic metadata such as studyName, dataType, earliestYearBP, etc.
    investigators : str
        A comma-separated string of investigator names.
    publications : list of Publication
        A list of Publication objects associated with the study.
    sites : list of Site
        A list of Site objects associated with the study.

    Methods
    -------
    __init__(study_data)
        Initializes the NOAADataset instance using a dictionary of study data.
    _load_metadata(study_data)
        Extracts metadata from the study data.
    _load_investigators(study_data)
        Extracts investigator details from the study data.
    to_dict()
        Returns a dictionary summary of the study and its components.
    """
    def __init__(self, study_data):
        """
        Initialize a NOAADataset instance.

        Parameters
        ----------
        study_data : dict
            JSON object for a NOAA study.
        """
        self.study_id = study_data.get('NOAADatasetId')
        self.xml_id = study_data.get('xmlId')
        self.metadata = self._load_metadata(study_data)
        self.investigators = self._load_investigators(study_data)
        self.publications = []
        for pub in study_data.get('publication', []):
            publication_obj = Publication(pub)
            publication_obj.study_id = self.study_id
            self.publications.append(publication_obj)
        self.sites = []
        for site in study_data.get('site', []):
            site_obj = Site(site, self.study_id)
            self.sites.append(site_obj)

    def _load_metadata(self, study_data):
        """
        Extract metadata from the study data.

        Parameters
        ----------
        study_data : dict
            The dictionary containing study information.

        Returns
        -------
        dict
            A dictionary with base metadata fields and their values.
        """
        fields = ['studyName', 'dataType', 'earliestYearBP', 'mostRecentYearBP',
                  'earliestYearCE', 'mostRecentYearCE', 'studyNotes', 'scienceKeywords']
        return {field: study_data.get(field, None) for field in fields}

    def _load_investigators(self, study_data):
        """
        Extract investigator details from the study data.

        Parameters
        ----------
        study_data : dict
            The dictionary containing study information.

        Returns
        -------
        str
            A comma-separated string of investigator names or None if not available.
        """
        investigators = study_data.get("investigatorDetails", [])
        if investigators:
            return ", ".join([f"{i.get('firstName', 'N/A')} {i.get('lastName', 'N/A')}" for i in investigators])
        return None

    def to_dict(self):
        """
        Convert the study data and its components to a dictionary.

        Returns
        -------
        dict
            A dictionary representing the study including metadata, investigators,
            publications, and sites.
        """
        return {
            "StudyID": self.study_id,
            "XMLID": self.xml_id,
            "StudyName": self.metadata.get("studyName"),
            "DataType": self.metadata.get("dataType"),
            "EarliestYearBP": self.metadata.get("earliestYearBP"),
            "MostRecentYearBP": self.metadata.get("mostRecentYearBP"),
            "EarliestYearCE": self.metadata.get("earliestYearCE"),
            "MostRecentYearCE": self.metadata.get("mostRecentYearCE"),
            "StudyNotes": self.metadata.get("studyNotes"),
            "ScienceKeywords": self.metadata.get("scienceKeywords"),
            "Investigators": self.investigators,
            "Publications": [pub.to_dict() for pub in self.publications],
            "Sites": [site.to_dict() for site in self.sites]
        }
