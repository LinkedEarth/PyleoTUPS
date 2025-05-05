__all__ = ['Site']

import numpy as np
from .PaleoData import PaleoData

class Site:
    """
    Represents a site within a study.
    """

    def __init__(self, site_data, study_id):
        """
        Initialize a Site instance.
        """
        self.site_id = site_data.get('NOAASiteId', np.nan)
        self.site_name = site_data.get('siteName', np.nan)
        self.location_name = site_data.get('locationName', np.nan)

        # ✅ Safely extract geo information
        geo = site_data.get('geo')
        if isinstance(geo, dict):
            geometry = geo.get('geometry', {})
            coordinates = geometry.get('coordinates', [np.nan, np.nan])
            self.lat = coordinates[0] if len(coordinates) > 0 else np.nan
            self.lon = coordinates[1] if len(coordinates) > 1 else np.nan

            properties = geo.get('properties', {})
            self.min_elevation = properties.get('minElevationMeters', np.nan)
            self.max_elevation = properties.get('maxElevationMeters', np.nan)
        else:
            self.lat = np.nan
            self.lon = np.nan
            self.min_elevation = np.nan
            self.max_elevation = np.nan

        # ✅ Validate paleoData entries
        paleo_data_list = site_data.get('paleoData', [])
        self.paleo_data = [
            PaleoData(paleo, study_id, self.site_id)
            for paleo in paleo_data_list
            if isinstance(paleo, dict)
        ]


    def to_dict(self):
        """
        Convert the site into a list of dictionaries, one per PaleoData file.
        """

        site_info = {
            "SiteID": self.site_id,
            "SiteName": self.site_name,
            "LocationName": self.location_name,
            "Latitude": self.lat,
            "Longitude": self.lon,
            "MinElevation": self.min_elevation,
            "MaxElevation": self.max_elevation,
        }

        paleo_data_records = []
        for paleo in self.paleo_data:
            for file_obj in paleo.files:
                paleo_entry = paleo.to_dict(file_obj)
                # Merge site info into each paleo record
                paleo_entry.update(site_info)
                paleo_data_records.append(paleo_entry)

        return paleo_data_records
