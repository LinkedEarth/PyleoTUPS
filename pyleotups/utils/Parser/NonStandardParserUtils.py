# NonStandardParserUtils.py
import re
import statistics
import pandas as pd
import requests
from enum import Enum
from collections import Counter

# ============================
#        DATA MODELS
# ============================

class BlockType(str, Enum):
    """
    Enumeration for the different types a Block can be classified as.
    """
    NARRATIVE = "narrative"
    HEADER_ONLY = "header-only"
    DATA = "data"
    TABULAR = "tabular"
    COMPLETE_TABULAR = "complete-tabular"
    ERROR = "error"


class LineInfo:
    """
    Holds the text and pre-computed statistics for a single line.

    Attributes
    ----------
    idx : int
        The original line number (index) from the source file.
    text : str
        The raw text of the line.
    line_len : int
        The character length of the line.
    count_single_tokens : int
        Token count using a single-space delimiter (r"\s+").
    count_multispace_tokens : int
        Token count using a multi-space delimiter (r"(\s{2,})").
    count_tab_tokens : int
        Token count using a tab delimiter (r"\t").
    numeric_single_ratio : float
        Ratio of numeric tokens (0.0 to 1.0) using r"\s+".
    numeric_multispace_ratio : float
        Ratio of numeric tokens (0.0 to 1.0) using r"(\s{2,})".
    numeric_tab_ratio : float
        Ratio of numeric tokens (0.0 to 1.0) using r"\t".
    """
    def __init__(self, idx, text):
        self.idx = idx
        self.text = text
        self.line_len = 0
        self.count_single_tokens = 0
        self.count_multispace_tokens = 0
        self.count_tab_tokens = 0
        self.numeric_single_ratio = 0.0
        self.numeric_multispace_ratio = 0.0
        self.numeric_tab_ratio = 0.0


class Block:
    """
    Represents a contiguous block of non-empty lines from the source file.

    This is the main data structure used by the parser, holding the lines,
    their classified type, and the resulting parsed DataFrame.

    Attributes
    ----------
    idx : int
        The sequential index (0, 1, 2...) of the block in the file.
    start : int
        The starting line number (index) of this block in the source file.
    end : int
        The ending line number (index) of this block in the source file.
    lines : list[LineInfo]
        A list of LineInfo objects contained within this block.
    block_type : BlockType
        The classified type of the block (e.g., NARRATIVE, TABULAR).
    headers : list[dict]
        A list of header dictionaries, where each dict contains:
        - "name" (str): The parsed header name.
        - "interval" (tuple[int, int]): The (start, end) char position.
    title : str or None
        A potential title line detected above the headers.
    stats : dict
        Aggregated statistics computed for the entire block.
    header_extent : int
        The number of lines detected as being part of the header.
    delimiter : str or None
        The regex string of the delimiter chosen for this block.
    df : pd.DataFrame or None
        The resulting pandas DataFrame if parsing was successful.
    used_as_header_for : list[int]
        A list of block indices that successfully borrowed this block's headers.
    """
    def __init__(self, idx, start, end):
        self.idx = idx
        self.start = start
        self.end = end
        self.lines = []
        self.block_type = BlockType.NARRATIVE
        self.headers = []
        self.title = None
        self.stats = {}
        self.header_extent = 0
        self.delimiter = None
        self.df = None
        self.used_as_header_for = []


# ============================
#    NUMERIC/STRING UTILS
# ============================

# Constants used for robust numeric parsing
_DASHES = "-\u2010\u2011\u2012\u2013\u2014\u2212"
_TRAILING_MARKS_RE = re.compile(r"[†‡*°%‰§#^~+]+$")
_NUM_RE = re.compile(r"""^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$""", re.ASCII)


def count_tokens(line, delimiter):
    """Counts non-empty tokens in a line given a regex delimiter."""
    tokens = re.split(delimiter, line.strip())
    return len([t for t in tokens if t])


def _strip_wrapping_brackets(s):
    """Recursively strips wrapping brackets like '([])' from a string."""
    s = s.strip()
    while s and s[0] in "([{" and s[-1] in ")]}":
        s = s[1:-1].strip()
    return s


def _normalize_piece(s):
    """Normalizes a string for numeric parsing."""
    s = s.replace(",", "")
    s = _TRAILING_MARKS_RE.sub("", s.strip())
    for d in _DASHES[1:]:
        s = s.replace(d, _DASHES[0])
    return s.strip()


def _is_plain_number(s):
    """Checks if a string is a plain ASCII number."""
    return bool(_NUM_RE.match(s))


def is_numeric(token):
    """
    Robustly checks if a token is numeric.

    Handles plain numbers, ranges (e.g., '10-20'), values with
    uncertainty (e.g., '1.5 ± 0.1' or '1.50.1'), and wrapped
    values (e.g., '(10)' or '6.80 (8.98)').

    Parameters
    ----------
    token : str
        The string token to check.

    Returns
    -------
    bool
        True if the token is considered numeric, False otherwise.
    """
    if token is None:
        return False
    t = _strip_wrapping_brackets(str(token).strip())
    if not t:
        return False

    t_norm = _normalize_piece(t)
    if _is_plain_number(t_norm):
        return True

    # Case: value followed by parenthetical, e.g., "6.80 (8.98)"
    m = re.match(r"^(.*?\S)\s*\(([^()]*)\)\s*$", t)
    if m:
        left, inside = m.group(1), m.group(2)
        left = _normalize_piece(_strip_wrapping_brackets(left))
        inside = _normalize_piece(_strip_wrapping_brackets(inside))
        return (_is_plain_number(left) or is_numeric(left)) and \
               (_is_plain_number(inside) or is_numeric(inside))

    # Case: special separator "" (legacy in some files)
    if "�" in t:
        parts = [p.strip() for p in t.split("�") if p.strip()]
        return len(parts) > 0 and all(is_numeric(p) for p in parts)

    # Case: standard uncertainty "±"
    if "±" in t:
        parts = [p.strip() for p in t.split("±") if p.strip()]
        return len(parts) == 2 and all(is_numeric(p) for p in parts)

    # Case: numeric ranges with hyphen/en-dash/em-dash/minus
    if any(d in t_norm for d in _DASHES):
        if t_norm.count("-") >= 1:
            pieces = [p.strip() for p in t_norm.split("-") if p.strip()]
            if len(pieces) == 2 and all(is_numeric(p) or 
                                       _is_plain_number(_normalize_piece(p)) 
                                       for p in pieces):
                return True

    # Case: whitespace-separated cluster that are all numeric
    ws_parts = [p for p in re.split(r"\s+", t) if p]
    if len(ws_parts) > 1 and all(is_numeric(p) for p in ws_parts):
        return True

    t_final = _normalize_piece(_strip_wrapping_brackets(t_norm))
    return _is_plain_number(t_final)


def numeric_ratio(line, delimiter):
    """Calculates the ratio of numeric tokens in a line."""
    tokens = [t for t in re.split(delimiter, line.strip()) if t]
    if not tokens:
        return 0.0
    return sum(is_numeric(t) for t in tokens) / len(tokens)


def generate_row_pattern(tokens):
    """Generates a string pattern ('N' for numeric, 'S' for string) for a list of tokens."""
    return ''.join('N' if is_numeric(tok) else 'S' for tok in tokens)


# ============================
#      STATISTICS UTILS
# ============================

def _safe_mean(x):
    """Calculates mean, returning 0 for empty list."""
    return statistics.mean(x) if len(x) > 0 else 0


def _safe_var(x):
    """Calculates variance, returning 0 for list with < 2 elements."""
    return statistics.variance(x) if len(x) > 1 else 0


def _safe_cv(x, mean):
    """Calculates coefficient of variation, handling 0 mean/empty list."""
    if len(x) == 0 or mean == 0:
        return 0
    return _safe_var(x) ** 0.5 / mean


def _most_common(lst):
    """Finds the most common element in a list, breaking ties with the max value."""
    if not lst:
        return None
    counts = Counter(lst)
    return max(counts, key=lambda x: (counts[x], x))


# ============================
#    HEADER & TOKEN UTILS
# ============================

def get_token_intervals_multi(line, delimiter):
    """
    Splits a line by a regex delimiter and returns token intervals.

    Parameters
    ----------
    line : str
        The line to parse.
    delimiter : str
        The regex delimiter string (e.g., r"(\s{2,})").

    Returns
    -------
    list[dict]
        A list of token dictionaries, each with:
        - "key" (str): A unique key for the token.
        - "display" (str): The stripped token text.
        - "interval" (tuple[int, int]): The (start, end) char position.
    """
    tokens = []
    token_counts = {}
    parts = re.split(delimiter, line)
    pos = 0
    for part in parts:
        if re.fullmatch(delimiter, part):
            pos += len(part)
        elif part:
            start = pos + 1
            end = pos + len(part) + 1
            base = part.strip()
            token_counts[base] = token_counts.get(base, 0) + 1
            tokens.append({
                "key": f"{base} {token_counts[base]}" if token_counts[base] > 1 else base,
                "display": base,
                "interval": (start, end)
            })
            pos += len(part)
    return tokens


def merge_headers_by_overlap(token_maps):
    """
    Merges multiple lines of header tokens into a single header list.

    Used for multi-line headers, where tokens from subsequent lines are
    merged into the first line's headers based on character overlap.

    Parameters
    ----------
    token_maps : list[list[dict]]
        A list where each item is the output of `get_token_intervals_multi`
        for one header line.

    Returns
    -------
    list[dict]
        A single list of merged header dictionaries.
    """
    if not token_maps:
        return []
    base_row = token_maps[0]
    merged_headers = [{"name": tok["display"], "interval": tok["interval"]}
                      for tok in base_row]
    for row in token_maps[1:]:
        for tok in row:
            matched = False
            for hdr in merged_headers:
                if intervals_overlap(hdr["interval"], tok["interval"]):
                    hdr["name"] += " " + tok["display"]
                    hdr["interval"] = (min(hdr["interval"][0], tok["interval"][0]),
                                       max(hdr["interval"][1], tok["interval"][1]))
                    matched = True
                    break
            if not matched:
                merged_headers.append(
                    {"name": tok["display"], "interval": tok["interval"]})
    return sorted(merged_headers, key=lambda x: x["interval"][0])


def compute_interval_overlap(interval1, interval2):
    """Calculates the number of overlapping characters between two intervals."""
    start1, end1 = interval1
    start2, end2 = interval2
    return max(0, min(end1, end2) - max(start1, start2))


def intervals_overlap(interval1, interval2):
    """Checks if two intervals overlap at all."""
    return max(interval1[0], interval2[0]) < min(interval1[1], interval2[1])


def _calculate_interval_distance(tok_interval, head_interval):
    """
    Calculates the minimum character distance between two intervals.
    Returns 0 if they overlap.
    """
    t_start, t_end = tok_interval
    h_start, h_end = head_interval
    if t_end <= h_start:
        return h_start - t_end  # Token is to the left
    elif t_start >= h_end:
        return t_start - h_end  # Token is to the right
    else:
        return 0  # Intervals overlap


# ============================
#      DATAFRAME UTILS
# ============================

def generate_df(lines_info, delimiter, headers, header_extent=0):
    """
    Generates a DataFrame using a simple split, assuming columns align.

    Parameters
    ----------
    lines_info : list[LineInfo]
        The list of LineInfo objects to parse.
    delimiter : str
        The regex delimiter to split lines.
    headers : list[dict]
        The list of header objects (must contain "name").
    header_extent : int, optional
        The number of lines to skip from the start of `lines_info`.
        Defaults to 0.

    Returns
    -------
    pd.DataFrame
        The parsed DataFrame.

    Raises
    ------
    ValueError
        If `delimiter` or `headers` are missing.
    ValueError
        If the number of tokens in a data row does not match
        the number of headers (and data rows exist).
    """
    if not delimiter:
        raise ValueError("generate_df requires a valid delimiter.")
    if not headers:
        raise ValueError("generate_df requires valid headers.")

    data_lines_text = [line.text for line in lines_info[header_extent:]]
    col_names = [h["name"] for h in headers]

    rows = []
    for line in data_lines_text:
        tokens = [t.strip() for t in re.split(delimiter, line.strip()) if t.strip()]
        rows.append(tokens)

    if rows and len(col_names) != len(rows[0]):
        raise ValueError(f"Column count ({len(rows[0])}) "
                         f"does not match header count ({len(col_names)})")

    return pd.DataFrame(rows, columns=col_names)


def assign_tokens_by_overlap(lines_info, delimiter, headers, header_extent=0):
    """
    Generates a DataFrame by assigning tokens based on character-level overlap.

    This is a fallback for misaligned data. It checks two stages:
    1. Assigns a token to the header with the *maximum overlap*.
    2. If no overlap, assigns to the header with the *minimum distance*
       (closest neighbor).

    Parameters
    ----------
    lines_info : list[LineInfo]
        The list of LineInfo objects to parse.
    delimiter : str
        The regex delimiter to split lines.
    headers : list[dict]
        The list of header objects (must contain "name" and "interval").
    header_extent : int, optional
        The number of lines to skip from the start of `lines_info`.
        Defaults to 0.

    Returns
    -------
    pd.DataFrame
        The parsed DataFrame.

    Raises
    ------
    ValueError
        If `delimiter` or `headers` are missing or malformed.
    """
    if not delimiter:
        raise ValueError("assign_tokens_by_overlap requires a valid delimiter.")
    if not headers or not all("interval" in h for h in headers):
        raise ValueError("assign_tokens_by_overlap requires headers with 'interval' data.")

    data_lines_info = lines_info[header_extent:]
    n_cols, n_rows = len(headers), len(data_lines_info)
    matrix = [[None for _ in range(n_cols)] for _ in range(n_rows)]
    col_names = [h["name"] for h in headers]

    for i, line_info in enumerate(data_lines_info):
        tokens = get_token_intervals_multi(line_info.text, delimiter)

        for tok in tokens:
            # Stage 1: Find best overlap
            overlaps = [
                (j, compute_interval_overlap(tok["interval"], headers[j]["interval"]))
                for j in range(n_cols)
            ]
            best_match_col, max_overlap = max(overlaps, key=lambda item: item[1],
                                              default=(-1, 0))

            # Stage 2: If no overlap, find nearest neighbor
            if max_overlap == 0:
                distances = [
                    (j, _calculate_interval_distance(tok["interval"], headers[j]["interval"]))
                    for j in range(n_cols)
                ]
                best_match_col, _ = min(distances, key=lambda item: item[1],
                                        default=(-1, float('inf')))

            # Assign token if a match was found
            if best_match_col != -1:
                current_val = matrix[i][best_match_col]
                tok_display = tok["display"]

                if current_val is None:
                    matrix[i][best_match_col] = tok_display
                else:
                    matrix[i][best_match_col] = f"{current_val} {tok_display}"

    return pd.DataFrame(matrix, columns=col_names)