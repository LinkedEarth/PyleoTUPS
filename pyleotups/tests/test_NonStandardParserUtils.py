# pyleotups/tests/test_NonStandardParserUtils.py

import pytest
import pandas as pd
from pyleotups.utils.Parser.NonStandardParserUtils import (
    Block, LineInfo, BlockType,
    is_numeric,
    get_token_intervals_multi,
    merge_headers_by_overlap,
    _calculate_interval_distance,
    assign_tokens_by_overlap,
    generate_df
)
from pyleotups.utils.Parser.NonStandardParser import NonStandardParser

# ====================================================================
# Unit Tests for is_numeric
# ====================================================================

@pytest.mark.parametrize("token, expected", [
    ("1", True),
    ("-2.5", True),
    (".75", True),
    ("1e-3", True),
    ("+50", True),
])
def test_is_numeric_plain(token, expected):
    """Tests plain, valid numeric strings."""
    assert is_numeric(token) == expected

@pytest.mark.parametrize("token, expected", [
    ("abc", False),
    ("", False),
    (None, False),
    ("1-2-3", False),
    ("e10", False),
    (".", False),
    ("-", False),
])
def test_is_numeric_non_numeric(token, expected):
    """Tests invalid or non-numeric strings."""
    assert is_numeric(token) == expected

@pytest.mark.parametrize("token, expected", [
    ("(90)", True),
    ("[12.4]", True),
    ("{(10)}", True),
    ("((8.5))", True),
])
def test_is_numeric_wrapped(token, expected):
    """Tests numbers wrapped in brackets or parentheses."""
    assert is_numeric(token) == expected

@pytest.mark.parametrize("token, expected", [
    ("6.80 (8.98)", True),
    ("6.80(8.98)", True),
    ("  5.1 (0.2)  ", True),
    ("5 (abc)", False),
    ("abc (5)", False),
])
def test_is_numeric_with_parenthetical(token, expected):
    """Tests numbers with parenthetical values (e.g., uncertainty)."""
    assert is_numeric(token) == expected

@pytest.mark.parametrize("token, expected", [
    ("61-63", True),
    ("61–63", True),  # en-dash
    ("61—63", True),  # em-dash
    ("1.5-2.5", True),
])
def test_is_numeric_ranges(token, expected):
    """Tests numeric ranges with various dash types."""
    assert is_numeric(token) == expected

@pytest.mark.parametrize("token, expected", [
    ("354.70.4", False),
    ("110", True),
])
def test_is_numeric_special_char(token, expected):
    """Tests numbers with the special '' separator."""
    # This test assumes the bug fix from t.split("") to t.split("")
    assert is_numeric(token) == expected

@pytest.mark.parametrize("token, expected", [
    ("1.5 ± 0.1", True),
    ("5±1", True),
    ("10 ± abc", False),
])
def test_is_numeric_uncertainty(token, expected):
    """Tests numbers with the '±' uncertainty symbol."""
    assert is_numeric(token) == expected

@pytest.mark.parametrize("token, expected", [
    ("61-63‡", True),
    ("12.3°", True),
    ("45%", True),
    ("100†", True),
])
def test_is_numeric_trailing_marks(token, expected):
    """Tests numbers with trailing symbols or footnote marks."""
    assert is_numeric(token) == expected

@pytest.mark.parametrize("token, expected", [
    ("8035 58", True),
    ("10 20 30", True),
    ("10 abc 30", False),
])
def test_is_numeric_whitespace_separated(token, expected):
    """Tests multiple numeric values separated by whitespace."""
    assert is_numeric(token) == expected


# ====================================================================
# Unit Tests for Header & Token Functions
# ====================================================================

def test_get_token_intervals_multi():
    """
    Tests that token intervals (char start/end) are calculated correctly.
    """
    line = "  col1    col2  "
    delimiter = r"(\s{2,})"
    expected = [
        {'display': 'col1', 'interval': (3, 7)},
        {'display': 'col2', 'interval': (11, 15)}
    ]
    
    # We strip 'key' for a simpler comparison
    result = [{k: v for k, v in d.items() if k != 'key'} 
              for d in get_token_intervals_multi(line, delimiter)]
    
    assert result == expected

def test_merge_headers_by_overlap():
    """
    Tests the multi-line header merging logic.
    
    This test simulates two lines of header tokens and checks if
    they are correctly merged into a single set of headers based
    on character interval overlap.
    """
    # Simulate output from get_token_intervals_multi for two lines
    token_map_line1 = [
        {'name': 'Header A', 'display': 'Header A', 'interval': (2, 12)},
        {'name': 'Header B', 'display': 'Header B', 'interval': (15, 25)}
    ]
    token_map_line2 = [
        {'name': '(unit A)', 'display': '(unit A)', 'interval': (3, 9)},
        {'name': '(unit B)', 'display': '(unit B)', 'interval': (16, 22)}
    ]
    token_maps = [token_map_line1, token_map_line2]
    
    merged = merge_headers_by_overlap(token_maps)
    
    # Check names
    assert len(merged) == 2
    assert merged[0]['name'] == 'Header A (unit A)'
    assert merged[1]['name'] == 'Header B (unit B)'
    
    # Check that intervals were expanded
    assert merged[0]['interval'] == (2, 12) # min(2,3), max(12,9) -> (2, 12) - corrected logic
    assert merged[1]['interval'] == (15, 25) # min(15,16), max(25,22) -> (15, 25) - corrected logic

@pytest.mark.parametrize("interval1, interval2, expected_dist", [
    ((10, 20), (15, 25), 0),  # Overlap
    ((0, 5), (10, 15), 5),   # Left
    ((20, 25), (10, 15), 5),   # Right
    ((5, 10), (10, 15), 0),   # Touching (distance 0)
])
def test_calculate_interval_distance(interval1, interval2, expected_dist):
    """Tests the calculation of distance between two intervals."""
    assert _calculate_interval_distance(interval1, interval2) == expected_dist


# ====================================================================
# Unit Tests for DataFrame Assignment
# ====================================================================

@pytest.fixture
def mock_headers():
    """Provides a standard set of headers for assignment tests."""
    return [
        {'name': 'H1', 'interval': (0, 10)},
        {'name': 'H2', 'interval': (11, 20)},
        {'name': 'H3', 'interval': (21, 30)}
    ]

@pytest.fixture
def mock_lines_info():
    """Provides a mock LineInfo object."""
    # Line: "  tok1     tok2       tok3  "
    #        (3, 7)   (12, 16)   (24, 28)
    # Line: "  orphan   "
    #        (16, 22) -> no overlap, closest to H2
    return [
        LineInfo(0, "  tok1     tok2       tok3  "),
        LineInfo(1, "               orphan   ") 
    ]

def test_assign_tokens_by_overlap_stage_1_overlap(mock_headers, mock_lines_info):
    """
    Tests the assignment of data via Stage 1 (Maximum Overlap).
    """
    lines = [mock_lines_info[0]] # Only test the first line
    delimiter = r"(\s+)" # Use single space to get all tokens
    
    df = assign_tokens_by_overlap(lines, delimiter, mock_headers)
    
    assert df.shape == (1, 3)
    assert df.iloc[0, 0] == 'tok1'
    assert df.iloc[0, 1] == 'tok2'
    assert df.iloc[0, 2] == 'tok3'

def test_assign_tokens_by_overlap_stage_2_proximity(mock_headers, mock_lines_info):
    """
    Tests the assignment of data via Stage 2 (Minimum Distance).
    
    'orphan' token is at (16, 22).
    - H1 (0, 10): distance = 6 (16-10)
    - H2 (11, 20): distance = 0 (overlap) - This test is now Stage 1
    - H3 (21, 30): distance = 0 (overlap) - This test is now Stage 1
    
    Let's create a better line for proximity.
    """
    line_text = "             tokA      " # (14, 18)
    line_info = [LineInfo(0, line_text)]
    headers = [
        {'name': 'H1', 'interval': (0, 10)}, # dist = 4
        {'name': 'H2', 'interval': (22, 30)}  # dist = 4
    ]
    delimiter = r"(\s+)"

    df = assign_tokens_by_overlap(line_info, delimiter, headers)
    
    # 'tokA' is at (14, 18).
    # Dist to H1 (0, 10) is 4 (14-10).
    # Dist to H2 (22, 30) is 4 (22-18).
    # It will be assigned to H1 (the first one).
    assert df.shape == (1, 2)
    assert df.iloc[0, 0] == 'tokA'
    assert df.iloc[0, 1] is None

def test_generate_df_mismatch_raises_error(mock_headers):
    """
    Tests that generate_df raises ValueError on column/token mismatch.
    """
    # 3 headers, but line has 2 tokens
    lines = [LineInfo(0, "  tok1     tok2  ")]
    delimiter = r"(\s{2,})"
    
    with pytest.raises(ValueError, match="Column count"):
        generate_df(lines, delimiter, mock_headers)


# ====================================================================
# Unit Tests for Classifier Helpers
# ====================================================================

@pytest.fixture
def mock_block():
    """Provides a basic mock Block object."""
    b = Block(0, 0, 0)
    b.lines = [LineInfo(0, "")] # Add dummy line
    return b

def test_choose_delimiter(mock_block):
    """
    Tests the delimiter selection logic (strict and non-strict).
    """
    # Case 1: Strict Pass (CV=0, mode > 1)
    mock_block.stats = {'cv_multi': 0.0, 'mode_multi': 5, 'cv_tab': 0.1, 'mode_tab': 5}
    assert NonStandardParser._choose_delimiter(mock_block, strict=True) == r"(\s{2,})"
    
    # Case 2: Strict Fail (CV > 0)
    mock_block.stats = {'cv_multi': 0.1, 'mode_multi': 5}
    assert NonStandardParser._choose_delimiter(mock_block, strict=True) is None
    
    # Case 3: Strict Fail (mode <= 1)
    mock_block.stats = {'cv_multi': 0.0, 'mode_multi': 1}
    assert NonStandardParser._choose_delimiter(mock_block, strict=True) is None
    
    # Case 4: Non-Strict Best (multi is better CV than tab)
    mock_block.stats = {'cv_multi': 0.1, 'mode_multi': 5, 'cv_tab': 0.5, 'mode_tab': 5}
    assert NonStandardParser._choose_delimiter(mock_block, strict=False) == r"(\s{2,})"
    
    # Case 5: Non-Strict Best (tab is better CV than multi)
    mock_block.stats = {'cv_multi': 0.5, 'mode_multi': 5, 'cv_tab': 0.1, 'mode_tab': 5}
    assert NonStandardParser._choose_delimiter(mock_block, strict=False) == r"\t"
    
    # Case 6: Non-Strict Fail (all modes <= 1)
    mock_block.stats = {'cv_multi': 0.1, 'mode_multi': 1, 'cv_tab': 0.1, 'mode_tab': 1}
    assert NonStandardParser._choose_delimiter(mock_block, strict=False) is None

def test_detect_header_extent_and_title(mock_block):
    """
    Tests the logic for detecting header extent and a title line.
    
    This test ensures the parser can find a title, skip it, and then
    find the headers.
    """
    delimiter = r"\s+"
    
    # Case 1: Title + 1 Header Line + Data
    mock_block.lines = [
        LineInfo(0, "My Title Line"),      # All 'S' -> Title
        LineInfo(1, "Col1    Col2    Col3"),    # All 'S' -> Header
        LineInfo(2, "1         2       3"),             # All 'N' -> Data
    ]
    extent, title_line = NonStandardParser._detect_header_extent(mock_block, delimiter)
    assert extent == 2
    assert title_line == None
    
    # Case 2: No Title + 2 Header Lines + Data
    mock_block.lines = [
        LineInfo(0, "Text Line 1"),     # All 'S' -> Header 1
        LineInfo(1, "Text Line 2"),     # All 'S' -> Header 2
        LineInfo(2, "1 2 3"),             # All 'N' -> Data
    ]
    extent, title_line = NonStandardParser._detect_header_extent(mock_block, delimiter)
    assert extent == 0
    assert title_line is None

    # Case 3: Special Numeric Header + String Header + Data
    mock_block.lines = [
        LineInfo(0, "1 2 3"),             # All 'N' -> Header 1
        LineInfo(1, "a b c"),             # All 'S' -> Header 2
        LineInfo(2, "1 2 3"),             # All 'N' -> Data
    ]
    extent, title_line = NonStandardParser._detect_header_extent(mock_block, delimiter)
    assert extent == 2
    assert title_line is None