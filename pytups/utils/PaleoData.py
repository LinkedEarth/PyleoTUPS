__all__ = ['PaleoData']

import numpy as np

class PaleoData:
    """
    Represents paleo data associated with a site.

    Attributes
    ----------
    datatable_id : str
        The NOAA data table identifier.
    dataTableName : str
        The name of the data table.
    timeUnit : str
        The unit of time for the data.
    file_url : str
        The URL from which the data file can be fetched.
    variables : list
        A list of variable names or identifiers.
    study_id : str
        The NOAA study ID this data belongs to.
    site_id : str
        The site identifier this data belongs to.

    Methods
    -------
    to_dict()
        Return a dictionary representation of the paleo data.
    """
    def __init__(self, paleo_data, study_id, site_id):
        """
        Initialize a PaleoData instance.

        Parameters
        ----------
        paleo_data : dict
            Dictionary containing paleo data.
        study_id : str
            The NOAA study ID.
        site_id : str
            The site ID.
        """
        self.datatable_id = paleo_data.get('NOAADataTableId', np.nan)
        self.dataTableName = paleo_data.get('dataTableName', np.nan)
        self.timeUnit = paleo_data.get('timeUnit', np.nan)
        data_files = paleo_data.get('dataFile', [])
        self.file_url = data_files[0].get('fileUrl', np.nan) if data_files else np.nan
        self.variables = []
        if data_files:
            self.variables = [var.get('cvShortName', np.nan) for var in data_files[0].get('variables', [])]
        self.study_id = study_id
        self.site_id = site_id

    def to_dict(self):
        """
        Convert the paleo data into a dictionary.

        Returns
        -------
        dict
            A dictionary representation of the paleo data.
        """
        return {
            "DataTableID": self.datatable_id,
            "DataTableName": self.dataTableName,
            "TimeUnit": self.timeUnit,
            "FileURL": self.file_url,
            "Variables": self.variables
        }
