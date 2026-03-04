from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional


class BaseDataset(ABC):
    """
    Abstract interface for dataset providers (e.g., NOAA, PANGAEA).

    All dataset backends must implement this researcher-facing API.
    """

    @abstractmethod
    def search_studies(self, *args, **kwargs) -> Optional[pd.DataFrame]:
        """
        Search backend and register studies internally.
        May optionally return a summary DataFrame.
        """
        pass

    @abstractmethod
    def get_summary(self) -> pd.DataFrame:
        """Return summary DataFrame of registered studies."""
        pass

    @abstractmethod
    def get_geo(self) -> pd.DataFrame:
        """Return site-level geospatial metadata."""
        pass

    @abstractmethod
    def get_publications(self, *args, **kwargs):
        """Return publication metadata."""
        pass

    @abstractmethod
    def get_funding(self) -> pd.DataFrame:
        """Return funding metadata."""
        pass

    @abstractmethod
    def get_data(self, *args, **kwargs):
        """Return parsed data tables."""
        pass