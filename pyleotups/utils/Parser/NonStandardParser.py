import pandas as pd
import re
import requests
class ParsingError(Exception):
    """
    Exception raised when parsing a non-standard file fails.
    """
    pass

class NonStandardParser:
    """
    Parser for NOAA files that do not follow standard metadata formatting.

    Attributes
    ----------
    file_path : str
        Path to the file to be parsed.
    lines : list of str
        Lines read from the file.
    blocks : list of dict
        Segregated blocks of lines with associated metadata.
    """

    def __init__(self, file_path):
        """
        Parameters
        ----------
        file_path : str
            Path to the NOAA `.txt` file.
        """
        self.file_path = file_path
        self.lines = []
        self.blocks = []

    def parse(self):
        """
        Parses the file and extracts tabular data.

        Returns
        -------
        list of pandas.DataFrame
            List of extracted tables.

        Raises
        ------
        ParsingError
            If no usable tables are found.
        """
        self._read_file()
        self._segregate_blocks()
        dfs = []
        for idx, block in enumerate(self.blocks):
            try:
                self._process_block(block)
                if block.get("df") is not None:
                    dfs.append(block["df"])
            except Exception:
                continue
        if not dfs:
            raise ParsingError(f"No tabular data could be extracted from: {self.file_path}")
        return dfs

    def _read_file(self):
        """
        Reads the file content line-by-line.

        Raises
        ------
        ParsingError
            If the file cannot be read.
        """
        try:
            response = requests.get(self.file_path)
            response.raise_for_status()
            self.lines = response.text.splitlines()
        except Exception as e:
            raise ParsingError(f"Failed to read file {self.file_path}: {e}")

    def _split_lines(self):
        """
        Cleans the lines by stripping whitespace.

        Returns
        -------
        list of str
            List of cleaned lines.
        """
        return [line.strip() for line in self.lines]

    def _segregate_blocks(self):
        """
        Segments the lines into logical blocks.
        """
        self.blocks = []
        current_block = []
        for line in self.lines:
            if line.strip():
                current_block.append(line)
            else:
                if current_block:
                    self.blocks.append({"lines": current_block})
                    current_block = []
        if current_block:
            self.blocks.append({"lines": current_block})

    def _compute_statistics(self, block):
        """
        Computes statistics needed for block classification.

        Parameters
        ----------
        block : dict
            Block of lines.
        """
        lines = block['lines']
        numeric_ratios = []
        token_counts_tab = []
        token_counts_space = []
        token_counts_multispace = []

        for line in lines:
            tokens_tab = line.split('\t')
            tokens_space = re.split(r'\s+', line.strip())
            tokens_multispace = re.split(r'\s{2,}', line.strip())

            numeric_tokens = [token for token in tokens_space if self._is_numeric(token)]
            numeric_ratio = len(numeric_tokens) / max(len(tokens_space), 1)
            numeric_ratios.append(numeric_ratio)

            token_counts_tab.append(len(tokens_tab))
            token_counts_space.append(len(tokens_space))
            token_counts_multispace.append(len(tokens_multispace))

        block['mean_numeric_ratio'] = sum(numeric_ratios) / len(numeric_ratios)
        block['mode_tab_tokens'] = max(set(token_counts_tab), key=token_counts_tab.count)
        block['mode_space_tokens'] = max(set(token_counts_space), key=token_counts_space.count)
        block['mode_multispace_tokens'] = max(set(token_counts_multispace), key=token_counts_multispace.count)
        block['cv_tab_tokens'] = self._coefficient_of_variation(token_counts_tab)
        block['cv_space_tokens'] = self._coefficient_of_variation(token_counts_space)
        block['cv_multispace_tokens'] = self._coefficient_of_variation(token_counts_multispace)

    def _process_block(self, block):
        """
        Processes and classifies a block.

        Parameters
        ----------
        block : dict
            Block of lines.
        """
        self._compute_statistics(block)
        lines = block['lines']
        delimiter = self._detect_delimiter(lines)

        if block['mean_numeric_ratio'] < 0.1:
            if block['mode_multispace_tokens'] > 1:
                block['block_type'] = 'header-only'
            else:
                block['block_type'] = 'narrative'
            return

        if block['cv_tab_tokens'] == 0 or block['cv_multispace_tokens'] == 0:
            headers = self._extract_headers(block, delimiter)
            # print(headers)
            data_lines = lines[len(headers):]
            # print(data_lines)
            if headers and data_lines:
                block['df'] = self._generate_df(headers, data_lines, delimiter)
                block['block_type'] = 'complete-tabular'
                return

        headers = self._extract_headers(block, delimiter)
        data_lines = lines[len(headers):]
        if headers and data_lines:
            try:
                block['df'] = self._assign_tokens_by_overlap(headers, data_lines, delimiter)
                block['block_type'] = 'complete-tabular'
            except Exception:
                block['block_type'] = 'narrative'
        else:
            block['block_type'] = 'narrative'

    def _detect_delimiter(self, lines):
        """
        Detects the delimiter used in the lines.

        Parameters
        ----------
        lines : list of str
            List of lines.

        Returns
        -------
        str
            Detected delimiter.
        """
        non_empty_lines = [line for line in lines if line.strip()]
        tab_counts = [len(line.split('\t')) for line in non_empty_lines]
        multispace_counts = [len(re.split(r'\s{2,}', line.strip())) for line in non_empty_lines]

        if len(set(tab_counts)) == 1 and tab_counts[0] > 1:
            return '\t'
        if len(set(multispace_counts)) == 1 and multispace_counts[0] > 1:
            return r'\s{2,}'
        return '\t'

    def _extract_headers(self, block, delimiter):
        """
        Extracts headers from a block.

        Parameters
        ----------
        block : dict
            Block of lines.
        delimiter : str
            Delimiter used to split lines.

        Returns
        -------
        list of list of str
            Tokenized headers.
        """
        lines = block['lines']
        headers = []
        header_extent, title_line = self.detect_header_extent(block, delimiter)
        # print(self.detect_header_extent(block, delimiter))
        for line in lines[:header_extent]:
            if delimiter == '\t':
                tokens = line.split('\t')
            else:
                tokens = re.split(delimiter, line.strip())
            headers.append(tokens)

        return headers

    def _generate_df(self, headers, data_lines, delimiter):
        """
        Generates a DataFrame from headers and data lines.

        Parameters
        ----------
        headers : list of list of str
            List of headers.
        data_lines : list of str
            Data lines.
        delimiter : str
            Delimiter used.

        Returns
        -------
        pandas.DataFrame
            Constructed DataFrame.
        """
        if delimiter == '\t':
            split_func = lambda line: line.split('\t')
        else:
            split_func = lambda line: re.split(delimiter, line.strip())

        header = headers[0] if headers else []
        data = []
        for line in data_lines:
            tokens = split_func(line)
            if len(tokens) < len(header):
                tokens.extend([''] * (len(header) - len(tokens)))
            if len(tokens) > len(header):
                tokens = tokens[:len(header)]
            data.append(tokens)

        return pd.DataFrame(data, columns=header)

    def _assign_tokens_by_overlap(self, headers, lines, delimiter):
        """
        Assigns tokens to headers based on overlaps.

        Parameters
        ----------
        headers : list of list of str
            Tokenized headers.
        lines : list of str
            Data lines.
        delimiter : str
            Delimiter used.

        Returns
        -------
        pandas.DataFrame
            Constructed DataFrame.
        """
        return self._generate_df(headers, lines, delimiter)

    def _merge_headers_by_overlap(self, token_maps):
        """
        Merges multiple header lines into one.

        Parameters
        ----------
        token_maps : list of list of str
            Tokenized headers.

        Returns
        -------
        list of str
            Merged headers.
        """
        merged = []
        for tokens in zip(*token_maps):
            merged.append(' '.join(filter(None, tokens)))
        return merged
    
    def generate_row_pattern(self, tokens):
        return ''.join(['N' if t.replace('.', '', 1).isdigit() else 'S' for t in tokens])


    def detect_header_extent(self, block, delimiter):
        """
        Detects how many initial lines qualify as header rows.

        Parameters
        ----------
        block : dict
            Block of lines.
        delimiter : str
            Delimiter used to split lines.

        Returns
        -------
        tuple of (int, Optional[int])
            Number of header lines, and index of title line if found.
        """
        patterns, title_line = [], None
        lines = block["lines"]

        for i, line in enumerate(lines):
            tokens = [t for t in re.split(delimiter, line.strip()) if t]
            pattern = self.generate_row_pattern(tokens)
            patterns.append(pattern)
            if i == 0 and pattern == "S":
                title_line = i

        start_i = title_line + 1 if title_line is not None else 0
        extent = 0
        for pattern in patterns[start_i:]:
            if all(c == "S" for c in pattern):
                extent += 1
            else:
                break

        return extent, title_line

    def _is_numeric(self, token):
        """
        Checks if a token is numeric.

        Parameters
        ----------
        token : str
            Token to check.

        Returns
        -------
        bool
            True if numeric, False otherwise.
        """
        try:
            float(token)
            return True
        except ValueError:
            return False

    def _coefficient_of_variation(self, counts):
        """
        Calculates the coefficient of variation.

        Parameters
        ----------
        counts : list of int
            List of token counts.

        Returns
        -------
        float
            Coefficient of variation.
        """
        if not counts:
            return float('inf')
        mean = sum(counts) / len(counts)
        variance = sum((x - mean) ** 2 for x in counts) / len(counts)
        stddev = variance ** 0.5
        return stddev / mean if mean else float('inf')
