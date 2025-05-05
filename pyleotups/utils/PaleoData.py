__all__ = ['PaleoData']

import numpy as np

class PaleoData:
    """
    Represents paleo data associated with a site, capable of holding multiple associated files.
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
        self.study_id = study_id
        self.site_id = site_id

        # Store all associated files
        self.files = []
        for file_info in paleo_data.get('dataFile', []):
            if (
            isinstance(file_info, dict) and
            isinstance(file_info.get('fileUrl'), str) and
            file_info['fileUrl'].strip() != ""
            ):                
                self.files.append(file_info)

        # For backward access (optional fields)
        if self.files:
            selected_file = self.files[0]
            self.file_url = selected_file.get('fileUrl', np.nan)
            self.variables = [v.get('cvShortName', np.nan) for v in selected_file.get('variables', []) if v.get('cvShortName')]
        else:
            self.file_url = np.nan
            self.variables = []

    def to_dict(self, file_obj=None):
        """
        Convert PaleoData into a dictionary.

        If file_obj provided, uses that file's metadata.
        Otherwise defaults to first file.
        """
        selected_file = file_obj if file_obj else (self.files[0] if self.files else {})

        return {
            "DataTableID": self.datatable_id,
            "DataTableName": self.dataTableName,
            "TimeUnit": self.timeUnit,
            "FileURL": selected_file.get("fileUrl", np.nan),
            "Variables": [v.get("cvShortName", np.nan) for v in selected_file.get('variables', []) if v.get('cvShortName')],
            "FileDescription": selected_file.get("urlDescription", np.nan),
            "TotalFilesAvailable": len(self.files)
        }
