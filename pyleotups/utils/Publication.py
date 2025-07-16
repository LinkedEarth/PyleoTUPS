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
    """
    def __init__(self, pub_data):
        """
        Initialize a Publication instance.

        Parameters
        ----------
        pub_data : dict
            Dictionary containing publication data.
        """
        author_data = pub_data.get('author')
        self.author = (
            author_data.get('name') if isinstance(author_data, dict)
            else 'Unknown Author'
        )
        self.title = pub_data.get('title') or 'Unknown Title'
        self.journal = pub_data.get('journal') or 'Unknown Journal'
        self.year = pub_data.get('pubYear') or 'Unknown Year'
        self.citation = pub_data.get('citation') or ''
        self.volume = pub_data.get('volume') or np.nan
        self.number = pub_data.get('issue') or np.nan
        self.pages = pub_data.get('pages') or np.nan
        self.pub_type = pub_data.get('type') or np.nan
        identifier_info = pub_data.get('identifier') or {}
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
        if isinstance(self.author, str) and self.author.strip():
            last_name = self.author.strip().split()[-1]
        else:
            last_name = "UnknownAuthor"

        # Ensure `title` is a string before regex
        title = self.title if isinstance(self.title, str) else "UnknownTitle"
        words = re.findall(r'\w+', title)
        first_significant_word = next(
            (word.capitalize() for word in words if len(word) > 2 and word.lower() != "the"),
            "Unknown"
        )

        # Handle year and study_id
        year = str(self.year) if self.year else "UnknownYear"
        study_id = str(self.study_id) if self.study_id else "UnknownID"

        # Assemble key
        return f"{last_name}_{first_significant_word}_{year}_{study_id}".replace(" ", "")

        

    def to_bibtex_entry(self):
        from pybtex.database import Entry
        
        fields = {
            "author": self.author,
            "title": self.title,
            "journal": self.journal,
            "year": str(self.year) if self.year else "Unknown",
            "doi": self.doi,
            "url": self.url,
        }
        fields = {k: v for k, v in fields.items() if v}  # Drop empty values
        return Entry("article", fields=fields)


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