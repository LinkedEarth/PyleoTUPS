__all__ = ['Publication']

import re
import numpy as np

class Publication:
    """
    Represents a publication within a study.

    Attributes
    ----------
    author : str
        The name of the author(s) of the publication.
    title : str
        The title of the publication.
    journal : str
        The journal where the publication appeared.
    year : str
        The publication year.
    volume : str or None
        The volume number (if applicable).
    number : str or None
        The issue number (if applicable).
    pages : str or None
        The page numbers (if applicable).
    pub_type : str or None
        The type of publication.
    doi : str or None
        The Digital Object Identifier.
    url : str or None
        URL for the publication.
    study_id : str or None
        The NOAA study ID to which this publication belongs.

    Methods
    -------
    get_citation_key()
        Generate and return a unique citation key.
    to_dict()
        Return a dictionary representation of the publication.
    """
    def __init__(self, pub_data):
        """
        Initialize a Publication instance.

        Parameters
        ----------
        pub_data : dict
            Dictionary containing publication data.
        """
        self.author = pub_data.get('author', {}).get('name', 'Unknown Author')
        self.title = pub_data.get('title', 'Unknown Title')
        self.journal = pub_data.get('journal', 'Unknown Journal')
        self.year = str(pub_data.get('pubYear', 'Unknown Year'))
        self.volume = pub_data.get('volume', np.nan)
        self.number = pub_data.get('issue', np.nan)
        self.pages = pub_data.get('pages', np.nan)
        self.pub_type = pub_data.get('type', np.nan)
        identifier_info = pub_data.get('identifier', {})
        self.doi = identifier_info.get('id', np.nan) if identifier_info else np.nan
        self.url = identifier_info.get('url', np.nan) if identifier_info else np.nan
        self.study_id = None

    def get_citation_key(self):
        """
        Generate a unique citation key for the publication.

        Returns
        -------
        str
            A citation key in the format: "<LastName>_<FirstSignificantWord>_<Year>_<StudyID>".
        """
        
        last_name = self.author.split()[-1]
        words = re.findall(r'\w+', self.title)
        first_significant_word = next((word.capitalize() for word in words if len(word) > 2 and word.lower() != 'the'), "Unknown")
        return f"{last_name}_{first_significant_word}_{self.year}_{self.study_id}".replace(" ", "")

    def to_dict(self):
        """
        Convert the publication data into a dictionary.

        Returns
        -------
        dict
            A dictionary representation of the publication.
        """
        return {
            "Author": self.author,
            "Title": self.title,
            "Journal": self.journal,
            "Year": self.year,
            "Volume": self.volume,
            "Number": self.number,
            "Pages": self.pages,
            "Type": self.pub_type,
            "DOI": self.doi,
            "URL": self.url,
            "CitationKey": self.get_citation_key() if self.study_id else None
        }