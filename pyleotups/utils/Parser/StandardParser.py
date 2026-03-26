__all__ = ['DataFetcher', 'StandardParser', 'ParsingError']

import requests
import pandas as pd
import re

from .NonStandardParserUtils import auto_cast_df

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
        df = auto_cast_df(df)
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

if __name__ == "__main__":
    parser = StandardParser("https://www.ncei.noaa.gov/pub/data/paleo/contributions_by_author/khider2014/khider2014-benth.txt")
    dfs = parser.parse()
    print(dfs)
    print(dfs["depth_cm"].dtype)