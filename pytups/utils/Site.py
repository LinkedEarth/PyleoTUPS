__all__ = ['Site']

import numpy as np
from .PaleoData import PaleoData

class Site:
    """
    Represents a site within a study.

    Attributes
    ----------
    site_id : str
        The unique identifier for the site.
    site_name : str
        The name of the site.
    location_name : str
        A descriptive location name.
    lat : float or str
        The latitude coordinate.
    lon : float or str
        The longitude coordinate.
    min_elevation : float or None
        The minimum elevation in meters.
    max_elevation : float or None
        The maximum elevation in meters.
    paleo_data : list of PaleoData
        A list of PaleoData objects associated with this site.

    Methods
    -------
    to_dict()
        Return a dictionary representation of the site.
    """
    def __init__(self, site_data, study_id):
        """
        Initialize a Site instance.

        Parameters
        ----------
        site_data : dict
            Dictionary containing site data.
        study_id : str
            The NOAA study ID that this site belongs to.
        """
        self.site_id = site_data.get('NOAASiteId', np.nan)
        self.site_name = site_data.get('siteName', np.nan)
        self.location_name = site_data.get('locationName', np.nan)
        geo = site_data.get('geo', {})
        geometry = geo.get('geometry', {})
        coordinates = geometry.get('coordinates', [np.nan, np.nan])
        self.lat = coordinates[0]
        self.lon = coordinates[1]
        properties = geo.get('properties', {})
        self.min_elevation = properties.get('minElevationMeters', np.nan)
        self.max_elevation = properties.get('maxElevationMeters', np.nan)
        paleo_data_list = site_data.get('paleoData', [])
        self.paleo_data = [PaleoData(paleo, study_id, self.site_id) for paleo in paleo_data_list]

    def to_dict(self):
        """
        Convert the site data into a dictionary.

        Returns
        -------
        dict
            A dictionary representation of the site, including its paleo data.
        """
        return {
            "SiteID": self.site_id,
            "SiteName": self.site_name,
            "LocationName": self.location_name,
            "Latitude": self.lat,
            "Longitude": self.lon,
            "MinElevation": self.min_elevation,
            "MaxElevation": self.max_elevation,
            "PaleoData": [p.to_dict() for p in self.paleo_data]
        }