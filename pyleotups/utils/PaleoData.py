__all__ = ['PaleoData']

import numpy as np

class PaleoData:
    """
    Represents paleo data associated with a site, including multiple data files and
    full variable metadata per file.

    Attributes
    ----------
    datatable_id : str
        Unique NOAA data table identifier.
    dataTableName : str
        Name of the data table.
    timeUnit : str
        Time unit used in the data table.
    files : list of dict
        List of raw file info dicts.
    file_variable_map : dict
        Maps fileUrl to a dict of variables and their full metadata.
    file_url : str or np.nan
        Shortcut to first file URL (for backward compatibility).
    variables : list of str
        Shortcut to variable names in first file (for backward compatibility).
    """

    def __init__(self, paleo_data, study_id, site_id):
        self.datatable_id = paleo_data.get('NOAADataTableId', np.nan)
        self.dataTableName = paleo_data.get('dataTableName', np.nan)
        self.timeUnit = paleo_data.get('timeUnit', np.nan)
        self.study_id = study_id
        self.site_id = site_id

        self.files = []
        self.file_variable_map = {}

        for file_info in paleo_data.get('dataFile', []):
            if not (isinstance(file_info, dict) and isinstance(file_info.get('fileUrl'), str)):
                continue

            file_url = file_info.get('fileUrl').strip()
            if not file_url:
                continue

            self.files.append(file_info)

            # Safely extract variable metadata
            variables_meta = {}
            used_names = {}

            for i, var in enumerate(file_info.get('variables', []), start=1):
                if not isinstance(var, dict):
                    continue

                # Resolve variable name (fallback logic)
                if isinstance(var.get("cvShortName"), str) and var["cvShortName"].strip():
                    base_name = var["cvShortName"].strip()
                elif isinstance(var.get("cvWhat"), str) and ">" in var["cvWhat"]:
                    base_name = var["cvWhat"].split(">")[-1].strip()
                else:
                    base_name = f"Var{i}"

                # Ensure unique variable name
                count = used_names.get(base_name, 0)
                var_name = base_name if count == 0 else f"{base_name}_{count}"
                used_names[base_name] = count + 1

                # Store full variable metadata dictionary
                variables_meta[var_name] = {
                    "cvDataType": var.get("cvDataType"),
                    "cvWhat": var.get("cvWhat"),
                    "cvMaterial": var.get("cvMaterial"),
                    "cvError": var.get("cvError"),
                    "cvUnit": var.get("cvUnit"),
                    "cvSeasonality": var.get("cvSeasonality"),
                    "cvDetail": var.get("cvDetail"),
                    "cvMethod": var.get("cvMethod"),
                    "cvAdditionalInfo": var.get("cvAdditionalInfo"),
                    "cvFormat": var.get("cvFormat"),
                    "cvShortName": var.get("cvShortName")
                }

            self.file_variable_map[file_url] = variables_meta

        # Compatibility for older uses
        if self.files:
            selected_file = self.files[0]
            self.file_url = selected_file.get('fileUrl', np.nan)
            # Use resolved names only
            self.variables = list(self.file_variable_map.get(self.file_url, {}).keys())
        else:
            self.file_url = np.nan
            self.variables = []

    def to_dict(self, file_obj=None):
        """
        Convert PaleoData into a dictionary, optionally for a specific file.

        Parameters
        ----------
        file_obj : dict, optional
            Specific file object (default is first file).

        Returns
        -------
        dict
            Dictionary of core metadata for one file.
        """
        selected_file = file_obj if file_obj else (self.files[0] if self.files else {})
        file_url = selected_file.get("fileUrl", np.nan)
        return {
            "DataTableID": self.datatable_id,
            "DataTableName": self.dataTableName,
            "TimeUnit": self.timeUnit,
            "FileURL": file_url,
            "Variables": list(self.file_variable_map.get(file_url, {}).keys()),
            "FileDescription": selected_file.get("urlDescription", np.nan),
            "TotalFilesAvailable": len(self.files)
        }
