__all__ = ['DataFetcher', 'StandardParser', 'ParsingError']

import requests
import pandas as pd
import re

@DeprecationWarning
class DataFetcher:
    """
    Standard parser for fetching and parsing external data files.
    """
    @staticmethod
    def _detect_line_terminator(content):
        read_size = 1024
        if len(content) < read_size:
            last_part = content
        else:
            last_part = content[-read_size:]
        if b'\r\n' in last_part:
            return r'\r\n'
        elif b'\n' in last_part:
            return r'\n'
        elif b'\r' in last_part:
            return r'\r'
        return r'\n'

    @staticmethod
    def fetch_data(file_url):
        if file_url.endswith(".xls") or file_url.endswith(".xlsx"):
            try:
                excel_data = pd.read_excel(file_url, sheet_name=None, comment='#', header=0)
                df_list = list(excel_data.values())
                print(f"Extracted {len(df_list)} DataFrame(s) from {file_url.rsplit('/', 1)[-1]}")
                return df_list
            except Exception as e:
                print(f"Error reading {file_url}: {e}")
                return pd.DataFrame()
        elif file_url.endswith(".txt"):
            response = requests.get(file_url)
            if response.status_code == 200:
                try:
                    terminator = DataFetcher._detect_line_terminator(response.content)
                    lines = re.split(terminator, response.text)
                    data_lines = [line for line in lines if not line.startswith('#') and line.strip() and len(line.split('\t')) > 1]
                    if data_lines:
                        headers = data_lines[0].split('\t')
                        data = [line.split('\t') for line in data_lines[1:]]
                        return pd.DataFrame(data, columns=headers)
                except Exception as e:
                    print(f"Error parsing text file {file_url}: {e}")
                    return pd.DataFrame()
            print(f"Failed to fetch data from {file_url}.")
        else:
            print(f"Unsupported file format: {file_url}")
        return pd.DataFrame()


# Custom exception for parsing errors.
class ParsingError(Exception):
    """Exception raised when the StandardParser encounters a parsing error."""
    pass


class StandardParser:
    """
    StandardParser parses NOAA .txt data files with standard format:
    Standard format refers to NOAA Templated file with 
    metadata -> (# lines), variables -> (## lines), data (tab-deliimited).

    Attributes
    ----------
    url : str
        URL of the file to parse.
    lines : list of str
        Fetched lines from file.
    meta_start : int
        Index where metadata block starts.
    meta_end : int
        Index where metadata block ends.
    variables : list of str
        Extracted variable names.
    skip_lines : int
        Lines to skip after metadata to reach data.
    data : list of list of str
        Parsed data rows.
    df : pandas.DataFrame
        Final constructed dataframe.
    """

    def __init__(self, url=None):
        self.url = url
        self.lines = None
        self.meta_start = None
        self.meta_end = None
        self.variables = None
        self.skip_lines = 0
        self.data = None
        self.df = None

    def parse(self, url=None):
        """
        Public method to parse the NOAA file.

        Parameters
        ----------
        url : str, optional
            URL to override the existing one.

        Returns
        -------
        pandas.DataFrame
        """
        if url:
            self.url = url
        if not self.url:
            raise ParsingError("No URL provided to parse.")

        try:
            self._fetch_file()
            self.meta_start, self.meta_end = self._identify_metadata()
            if self.meta_start is None or self.meta_end is None:
                raise ParsingError("No metadata block detected — not a standard file.")
            self.variables, _, self.skip_lines = self._extract_variables()
            self.data, _ = self._parse_data()
            self.df = self._construct_dataframe()
        except Exception as e:
            raise ParsingError(f"Error parsing file: {e}")

        if self.df is None:
            raise ParsingError("DataFrame construction failed.")

        return self.df

    # ===== Private Helper Methods =====

    def _fetch_file(self):
        """
        Download a file from the given URL and split its content into lines.

        Parameters
        ----------
        url : str
            The URL of the file to fetch.

        Returns
        -------
        list of str
            The file content split into individual lines.

        Raises
        ------
        requests.HTTPError
            If the HTTP request returned an unsuccessful status code.
        """
        response = requests.get(self.url)
        response.raise_for_status()
        self.lines = response.text.splitlines()

    def _identify_metadata(self):
        """
        Identify the metadata block in the file by finding lines that start with '#'.

        Parameters
        ----------
        lines : list of str
            All lines from the file.

        Returns
        -------
        tuple of (int, int) or (None, None)
            A tuple containing the first and last indices of metadata lines.
            Returns (None, None) if no metadata lines are found.
        """
        metadata_indices = [i for i, line in enumerate(self.lines) if line.lstrip().startswith('#')]
        if metadata_indices:
            return metadata_indices[0], metadata_indices[-1]
        return None, None

    def _extract_variables(self):
        """
        Extract variable names (column headers) from a NOAA text file using multiple methods.

        The function first attempts to extract variables from a metadata block containing an explicit 
        "Variables" marker. If that fails, it attempts extraction from the first data header line. If that 
        fails too, it uses a fallback method on the first non-empty data line.

        Parameters
        ----------
        lines : list of str
            All lines from the file.
        meta_start : int
            The index of the first metadata line.
        meta_end : int
            The index of the last metadata line.

        Returns
        -------
        tuple of (list of str, str, int)
            A tuple (variables, source, header_skip_count) where:
            - variables is the list of extracted variable names,
            - source is "metadata" if variables were extracted from the metadata block, 
                or "data" if extracted from the data header,
            - header_skip_count indicates how many header lines should be skipped.
        """
        variables, header_skip = self._parse_metadata_variables()
        if variables:
            return variables, "metadata", header_skip

        variables, header_skip = self._parse_data_header_variables()
        if variables:
            return variables, "data", header_skip

        variables, header_skip = self._fallback_variable_extraction()
        if variables:
            return variables, "fallback", header_skip

        return [], None, 0

    def _parse_metadata_variables(self):
        """
        Extract variable names from a metadata block when an explicit "Variables" block exists.

        This function attempts to extract variables by looking for a metadata line that starts with 
        "# variables" (case-insensitive). If found, it first searches for lines starting with '##' 
        following the marker. If no such lines exist, it falls back to splitting other non-comment lines.

        Parameters
        ----------
        lines : list of str
            All lines from the file.
        meta_start : int
            Index of the first metadata line.
        meta_end : int
            Index of the last metadata line.

        Returns
        -------
        tuple of (list of str, int)
            A tuple where the first element is a list of extracted variable names and the second element is 
            the header skip count (usually 1 if variables are successfully extracted).
        """
        variables = []
        header_skip_count = 0

        for i in range(self.meta_start, self.meta_end + 1):
            if re.match(r'^#\s*variables', self.lines[i], re.IGNORECASE):
                for j in range(i + 1, self.meta_end + 1):
                    if self.lines[j].lstrip().startswith('##'):
                        token = self._extract_first_non_digit_token(self.lines[j].lstrip('#'))
                        if token:
                            variables.append(token)
                if variables:
                    header_skip_count = 1
                break
        return variables, header_skip_count

    def _parse_data_header_variables(self):
        """
        Extract variable names from the data header when no explicit metadata "Variables" block exists.

        It searches from the line immediately after the metadata block until a non-comment line is found 
        that, when split by either tab or comma, yields at least 9 tokens.

        Parameters
        ----------
        lines : list of str
            All lines from the file.
        meta_end : int
            The index of the last metadata line.

        Returns
        -------
        tuple of (list of str, int)
            A tuple containing the extracted variable names and a header skip count (typically 1).
        """
        variables = []
        header_skip_count = 1
        for i in range(self.meta_end + 1, len(self.lines)):
            line = self.lines[i].strip()
            if line and not line.startswith('#'):
                tokens_tab = line.split('\t')
                tokens_comma = line.split(',')
                tokens = tokens_tab if len(tokens_tab) >= len(tokens_comma) else tokens_comma
                if len(tokens) >= 9:
                    variables = tokens
                    break
        return variables, header_skip_count

    def _fallback_variable_extraction(self):
        """
        Fallback extraction: use the first non-empty line in the data block, split by tabs.

        Parameters
        ----------
        lines : list of str
            All lines from the file.
        meta_end : int
            The index of the last metadata line.

        Returns
        -------
        tuple of (list of str, int)
            A tuple containing variable names (or autogenerated names for empty tokens) and a header skip count.
        """
        variables = []
        header_skip_count = 1
        for i in range(self.meta_end + 1, len(self.lines)):
            line = self.lines[i].strip()
            if line:
                tokens = line.split('\t')
                if len(tokens) > 1:
                    variables = [token if token else f"Unnamed_{idx}" for idx, token in enumerate(tokens)]
                    break
        return variables, header_skip_count

    def _parse_data(self):
        """
        Parse the data block of the file, skipping empty lines and header lines.

        This function detects the delimiter used in the data block and ensures that all rows are padded 
        to have a uniform number of columns.

        Parameters
        ----------
        lines : list of str
            All lines from the file.
        meta_end : int
            The index of the last metadata line.
        skip_lines : int, optional
            Number of header lines to skip in the data block, by default 0.

        Returns
        -------
        tuple of (list, int) or (None, None)
            A tuple (data, row_len) where data is a list of rows (each row is a list of tokens) and row_len 
            is the uniform number of columns. Returns (None, None) if parsing fails.
        """
        index = self.meta_end + 1
        index = self._skip_empty_lines(index)
        index += self.skip_lines
        remaining_lines = self.lines[index:]

        delimiter = self._detect_delimiter(remaining_lines)
        data = []
        for line in remaining_lines:
            if not line.strip():
                continue
            if delimiter == '\t':
                row = line.split('\t')
            else:
                row = re.split(r'\s{2,}', line.strip())
            data.append(row)

        if not data or len(data[0]) < 2:
            return None, None

        max_len = max(len(row) for row in data)
        for row in data:
            if len(row) < max_len:
                row.extend([''] * (max_len - len(row)))

        return data, max_len

    def _construct_dataframe(self):
        """
        Construct a pandas DataFrame from parsed data rows and variable names.

        Handles three cases:
        - Exact match: The number of variables equals the number of columns.
        - Extra columns: More columns than variables (trims extra columns).
        - Missing columns: Fewer columns than variables (pads rows with empty strings).

        Parameters
        ----------
        data : list of list of str
            Parsed data rows.
        variables : list of str
            Column headers.

        Returns
        -------
        pandas.DataFrame or None
            The constructed DataFrame with an attribute 'variables' set, or None if data or variables are missing.
        """        
        if not self.data or not self.variables:
            return None

        row_len = len(self.data[0])
        var_len = len(self.variables)

        if var_len == row_len:
            df = pd.DataFrame(self.data, columns=self.variables)
        elif var_len < row_len:
            trimmed = [row[:var_len] for row in self.data]
            df = pd.DataFrame(trimmed, columns=self.variables)
        else:  # var_len > row_len
            padded = [row + [''] * (var_len - len(row)) for row in self.data]
            df = pd.DataFrame(padded, columns=self.variables)

        df.attrs['variables'] = self.variables
        return df

    def _skip_empty_lines(self, index):
        """
        Advance the index until a non-empty line is encountered.

        Parameters
        ----------
        lines : list of str
            The file lines.
        index : int
            The starting index.

        Returns
        -------
        int
            The index of the first non-empty line.
        """        
        while index < len(self.lines) and not self.lines[index].strip():
            index += 1
        return index

    def _detect_delimiter(self, lines):
        r"""
        Detect the delimiter used in a set of data lines.

        It first tries tab-delimitation; if token counts are inconsistent, it falls back to splitting 
        on two or more spaces.

        Parameters
        ----------
        data_lines : list of str
            A list of non-empty data lines.

        Returns
        -------
        str
            The detected delimiter, either the tab character ('\t') or a regex pattern (r'\s{2,}').
        """
        non_empty = [line.strip() for line in lines if line.strip()]
        if not non_empty:
            return '\t'
        tab_counts = [len(line.split('\t')) for line in non_empty]
        if len(set(tab_counts)) == 1 and tab_counts[0] > 1:
            return '\t'
        space_counts = [len(re.split(r'\s{2,}', line)) for line in non_empty]
        if len(set(space_counts)) == 1 and space_counts[0] > 1:
            return r'\s{2,}'
        return '\t'

    def _extract_first_non_digit_token(self, line):
        """
        Remove any leading comment markers from a line and return the first token that is not purely numeric.

        Parameters
        ----------
        line : str
            A line of text (typically from metadata).

        Returns
        -------
        str or None
            The first non-digit token, or None if no valid token is found.
        """
        pattern = r'^\s*(.*?)(?:\t|\s{2,})(?:[^,\n]*,){0,9}[^,\n]*$'
        match = re.match(pattern, line)
        if match:
            return match.group(1).strip()
        tokens = re.split(r'[\s,]+', line.strip())
        for token in tokens:
            if token and not token.isdigit():
                return token
        return None

# def fetch_file(url):
#     """
#     Download a file from the given URL and split its content into lines.

#     Parameters
#     ----------
#     url : str
#         The URL of the file to fetch.

#     Returns
#     -------
#     list of str
#         The file content split into individual lines.

#     Raises
#     ------
#     requests.HTTPError
#         If the HTTP request returned an unsuccessful status code.
#     """
#     response = requests.get(url)
#     response.raise_for_status()
#     return response.text.splitlines()


# def identify_metadata(lines):
#     """
#     Identify the metadata block in the file by finding lines that start with '#'.

#     Parameters
#     ----------
#     lines : list of str
#         All lines from the file.

#     Returns
#     -------
#     tuple of (int, int) or (None, None)
#         A tuple containing the first and last indices of metadata lines.
#         Returns (None, None) if no metadata lines are found.
#     """
#     metadata_indices = [i for i, line in enumerate(lines) if line.lstrip().startswith('#')]
#     if metadata_indices:
#         return metadata_indices[0], metadata_indices[-1]
#     return None, None


# def extract_first_non_digit_token(line):
#     """
#     Remove any leading comment markers from a line and return the first token that is not purely numeric.

#     Parameters
#     ----------
#     line : str
#         A line of text (typically from metadata).

#     Returns
#     -------
#     str or None
#         The first non-digit token, or None if no valid token is found.
#     """
#     pattern = r'^\s*(.*?)(?:\t|\s{2,})(?:[^,\n]*,){0,9}[^,\n]*$'
#     match = re.match(pattern, line)
#     if match:
#         return match.group(1).strip()
#     tokens = re.split(r'[\s,]+', line.strip())
#     for token in tokens:
#         if token and not token.isdigit():
#             return token
#     return None


# def parse_metadata_variables(lines, meta_start, meta_end):
#     """
#     Extract variable names from a metadata block when an explicit "Variables" block exists.

#     This function attempts to extract variables by looking for a metadata line that starts with 
#     "# variables" (case-insensitive). If found, it first searches for lines starting with '##' 
#     following the marker. If no such lines exist, it falls back to splitting other non-comment lines.

#     Parameters
#     ----------
#     lines : list of str
#         All lines from the file.
#     meta_start : int
#         Index of the first metadata line.
#     meta_end : int
#         Index of the last metadata line.

#     Returns
#     -------
#     tuple of (list of str, int)
#         A tuple where the first element is a list of extracted variable names and the second element is 
#         the header skip count (usually 1 if variables are successfully extracted).
#     """
#     variables = []
#     header_skip_count = 0
#     variable_block_index = None

#     for i in range(meta_start, meta_end + 1):
#         if re.match(r'^#\s*variables', lines[i], re.IGNORECASE):
#             variable_block_index = i
#             break

#     if variable_block_index is not None:
#         # CASE 1A: Look for lines starting with '##'
#         for i in range(variable_block_index + 1, meta_end + 1):
#             if lines[i].lstrip().startswith('##'):
#                 token = extract_first_non_digit_token(lines[i].lstrip('#'))
#                 if token:
#                     variables.append(token)
#         # CASE 1B: Fallback if no '##' lines found.
#         if not variables:
#             for i in range(variable_block_index + 1, meta_end + 1):
#                 if lines[i].strip() and not lines[i].startswith("#"):
#                     if len(re.split(r',', lines[i].strip())) >= 9:
#                         token = extract_first_non_digit_token(lines[i])
#                         if token:
#                             variables.append(token)
#         if variables:
#             header_skip_count = 1
#     return variables, header_skip_count


# def parse_data_header_variables(lines, meta_end):
#     """
#     Extract variable names from the data header when no explicit metadata "Variables" block exists.

#     It searches from the line immediately after the metadata block until a non-comment line is found 
#     that, when split by either tab or comma, yields at least 9 tokens.

#     Parameters
#     ----------
#     lines : list of str
#         All lines from the file.
#     meta_end : int
#         The index of the last metadata line.

#     Returns
#     -------
#     tuple of (list of str, int)
#         A tuple containing the extracted variable names and a header skip count (typically 1).
#     """
#     variables = []
#     header_skip_count = 1
#     for i in range(meta_end + 1, len(lines)):
#         line = lines[i].strip()
#         if line and not line.lstrip().startswith('#'):
#             tokens_tab = re.split(r'\t', line)
#             tokens_comma = re.split(r',', line)
#             if len(tokens_tab) >= 9 or len(tokens_comma) >= 9:
#                 variables = tokens_tab if len(tokens_tab) >= len(tokens_comma) else tokens_comma
#                 break
#     return variables, header_skip_count


# def fallback_variable_extraction(lines, meta_end):
#     """
#     Fallback extraction: use the first non-empty line in the data block, split by tabs.

#     Parameters
#     ----------
#     lines : list of str
#         All lines from the file.
#     meta_end : int
#         The index of the last metadata line.

#     Returns
#     -------
#     tuple of (list of str, int)
#         A tuple containing variable names (or autogenerated names for empty tokens) and a header skip count.
#     """
#     variables = []
#     header_skip_count = 1
#     for i in range(meta_end + 1, len(lines)):
#         if lines[i].strip():
#             tokens = re.split(r'\t', lines[i].strip())
#             if len(tokens) > 1:
#                 variables = [f"Unnamed_{idx}" if not token else token for idx, token in enumerate(tokens)]
#                 break
#     return variables, header_skip_count


# def variable_parser(lines, meta_start, meta_end):
#     """
#     Extract variable names (column headers) from a NOAA text file using multiple methods.

#     The function first attempts to extract variables from a metadata block containing an explicit 
#     "Variables" marker. If that fails, it attempts extraction from the first data header line. If that 
#     fails too, it uses a fallback method on the first non-empty data line.

#     Parameters
#     ----------
#     lines : list of str
#         All lines from the file.
#     meta_start : int
#         The index of the first metadata line.
#     meta_end : int
#         The index of the last metadata line.

#     Returns
#     -------
#     tuple of (list of str, str, int)
#         A tuple (variables, source, header_skip_count) where:
#           - variables is the list of extracted variable names,
#           - source is "metadata" if variables were extracted from the metadata block, 
#             or "data" if extracted from the data header,
#           - header_skip_count indicates how many header lines should be skipped.
#     """
#     variables, header_skip_count = parse_metadata_variables(lines, meta_start, meta_end)
#     if variables:
#         return variables, "metadata", header_skip_count

#     variables, header_skip_count = parse_data_header_variables(lines, meta_end)
#     if variables:
#         return variables, "data", header_skip_count

#     variables, header_skip_count = fallback_variable_extraction(lines, meta_end)
#     if variables:
#         return variables, "data", header_skip_count

#     return [], None, 0


# def skip_empty_lines(lines, index):
#     """
#     Advance the index until a non-empty line is encountered.

#     Parameters
#     ----------
#     lines : list of str
#         The file lines.
#     index : int
#         The starting index.

#     Returns
#     -------
#     int
#         The index of the first non-empty line.
#     """
#     while index < len(lines) and not lines[index].strip():
#         index += 1
#     return index


# def detect_delimiter(data_lines):
#     r"""
#     Detect the delimiter used in a set of data lines.

#     It first tries tab-delimitation; if token counts are inconsistent, it falls back to splitting 
#     on two or more spaces.

#     Parameters
#     ----------
#     data_lines : list of str
#         A list of non-empty data lines.

#     Returns
#     -------
#     str
#         The detected delimiter, either the tab character ('\t') or a regex pattern (r'\s{2,}').
#     """
#     non_empty = [line.strip() for line in data_lines if line.strip()]
#     if not non_empty:
#         return '\t'
#     tab_counts = [len(line.split('\t')) for line in non_empty]
#     if len(set(tab_counts)) == 1 and tab_counts[0] > 1:
#         return '\t'
#     space_counts = [len(re.split(r'\s{2,}', line)) for line in non_empty]
#     if len(set(space_counts)) == 1 and space_counts[0] > 1:
#         return r'\s{2,}'
#     return '\t'


# def data_parser(lines, meta_end, skip_lines=0):
#     """
#     Parse the data block of the file, skipping empty lines and header lines.

#     This function detects the delimiter used in the data block and ensures that all rows are padded 
#     to have a uniform number of columns.

#     Parameters
#     ----------
#     lines : list of str
#         All lines from the file.
#     meta_end : int
#         The index of the last metadata line.
#     skip_lines : int, optional
#         Number of header lines to skip in the data block, by default 0.

#     Returns
#     -------
#     tuple of (list, int) or (None, None)
#         A tuple (data, row_len) where data is a list of rows (each row is a list of tokens) and row_len 
#         is the uniform number of columns. Returns (None, None) if parsing fails.
#     """
#     data = []
#     index = meta_end + 1
#     index = skip_empty_lines(lines, index)
#     index += skip_lines
#     remaining_lines = lines[index:]
#     delimiter = detect_delimiter(remaining_lines)
#     for line in remaining_lines:
#         if not line.strip():
#             continue
#         if delimiter == '\t':
#             row = line.split('\t')
#         else:
#             row = re.split(delimiter, line.strip())
#         data.append(row)
#     if not data or (data and len(data[0]) < 2):
#         return None, None
#     max_len = max(len(row) for row in data)
#     for i in range(len(data)):
#         if len(data[i]) < max_len:
#             data[i] = data[i] + [''] * (max_len - len(data[i]))
#     return data, max_len


# def dataframe_constructor(data, variables):
#     """
#     Construct a pandas DataFrame from parsed data rows and variable names.

#     Handles three cases:
#       - Exact match: The number of variables equals the number of columns.
#       - Extra columns: More columns than variables (trims extra columns).
#       - Missing columns: Fewer columns than variables (pads rows with empty strings).

#     Parameters
#     ----------
#     data : list of list of str
#         Parsed data rows.
#     variables : list of str
#         Column headers.

#     Returns
#     -------
#     pandas.DataFrame or None
#         The constructed DataFrame with an attribute 'variables' set, or None if data or variables are missing.
#     """
#     if not data or not variables:
#         return None

#     row_len = len(data[0])
#     var_len = len(variables)

#     if var_len == row_len:
#         df = pd.DataFrame(data, columns=variables)
#     elif var_len < row_len:
#         data_trimmed = [row[:var_len] for row in data]
#         df = pd.DataFrame(data_trimmed, columns=variables)
#     elif var_len > row_len:
#         data_padded = [row + [''] * (var_len - len(row)) for row in data]
#         df = pd.DataFrame(data_padded, columns=variables)

#     df.attrs['variables'] = variables
#     return df

# # ---------------------------------------------------------------------------
# # StandardParser Class
# # ---------------------------------------------------------------------------
# class StandardParser:
#     """
#     StandardParser encapsulates the complete workflow for downloading and parsing a NOAA text file.
    
#     The class maintains attributes such as the URL, file lines, metadata boundaries, extracted variable names,
#     header skip count, parsed data, and the final DataFrame.

#     Attributes
#     ----------
#     url : str
#         The URL of the file to parse.
#     lines : list of str
#         The content of the file split into lines.
#     meta_start : int
#         The index of the first metadata line.
#     meta_end : int
#         The index of the last metadata line.
#     variables : list of str
#         The extracted variable names.
#     skip_lines : int
#         The number of header lines to skip in the data block.
#     data : list of list of str
#         The parsed data rows.
#     df : pandas.DataFrame
#         The constructed DataFrame.

#     Methods
#     -------
#     parse(url=None)
#         Execute the full parsing workflow and return the constructed DataFrame.
#     _fetch_file()
#         Fetch the file and set the 'lines' attribute.
#     _identify_metadata()
#         Identify metadata boundaries and set 'meta_start' and 'meta_end'.
#     _extract_variables()
#         Extract variable names and header skip count, setting 'variables' and 'skip_lines'.
#     _parse_data()
#         Parse the data block from the file and set the 'data' attribute.
#     _construct_dataframe()
#         Construct the final DataFrame from parsed data and variables.
#     """
#     def __init__(self, url=None):
#         self.url = url
#         self.lines = None
#         self.meta_start = None
#         self.meta_end = None
#         self.variables = None
#         self.skip_lines = 0
#         self.data = None
#         self.df = None

#     def parse(self, url=None):
#         """
#         Orchestrate the full parsing process.

#         Parameters
#         ----------
#         url : str, optional
#             The URL to parse. If provided, it overrides the existing URL attribute.

#         Returns
#         -------
#         pandas.DataFrame
#             The constructed DataFrame.

#         Raises
#         ------
#         ParsingError
#             If any step of the parsing process fails.
#         """
#         if url is not None:
#             self.url = url
#         if not self.url:
#             raise ParsingError("No URL provided.")
#         try:
#             self._fetch_file()
#         except Exception as e:
#             raise ParsingError(f"Error fetching file: {e}")
#         self.meta_start, self.meta_end = self._identify_metadata()
#         if self.meta_start is None:
#             raise ParsingError("Invalid file format."
#             "Wrapper can only parse stndard NOAA template formatted files")
#         self.variables, _, self.skip_lines = self._extract_variables()
#         if not self.variables:
#             raise ParsingError("Failed to extract variable names from file.")
#         self.data, _ = self._parse_data()
#         if self.data is None:
#             raise ParsingError("No valid data block found.")
#         self.df = self._construct_dataframe()
#         if self.df is None:
#             raise ParsingError("DataFrame construction failed.")
#         return self.df

#     def _fetch_file(self):
#         self.lines = fetch_file(self.url)

#     def _identify_metadata(self):
#         return identify_metadata(self.lines)

#     def _extract_variables(self):
#         return variable_parser(self.lines, self.meta_start, self.meta_end)

#     def _parse_data(self):
#         return data_parser(self.lines, self.meta_end, self.skip_lines)

#     def _construct_dataframe(self):
#         return dataframe_constructor(self.data, self.variables)