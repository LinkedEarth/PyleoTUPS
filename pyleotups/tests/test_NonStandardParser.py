# pyleotups/tests/test_NonStandardParser.py

import pytest
import requests
import pandas as pd
from pathlib import Path
from pyleotups.utils.Parser.NonStandardParser import NonStandardParser
from pyleotups.utils.Parser.NonStandardParserUtils import BlockType
from unittest import mock
# --- Fixture Setup ---

# Get the directory of the current test file
TEST_DIR = Path(__file__).parent
# Define the path to the test data directory
DATA_DIR = TEST_DIR / "data"


@pytest.fixture(scope="session")
def data_path():
    """
    Pytest fixture to provide the path to the test data directory.
    
    Returns
    -------
    pathlib.Path
        The path object pointing to the 'data' directory.
    """
    return DATA_DIR


@pytest.fixture(scope="session")
def example_file_path(data_path):
    """
    Pytest fixture to provide the full path to 'nonstandard_file_example.txt'.
    
    Parameters
    ----------
    data_path : pathlib.Path
        The fixture providing the path to the 'data' directory.
        
    Returns
    -------
    pathlib.Path
        The full path to the 'nonstandard_file_example.txt' file.
    """
    path = data_path / "nonstandard_file_example.txt"
    if not path.exists():
        pytest.skip(f"Test data file not found: {path}")
    return path


@pytest.fixture(scope="session")
def parsed_blocks(example_file_path):
    """
    Pytest fixture that runs the NonStandardParser on the example file once.
    
    This provides the resulting list of 'Block' objects to all tests
    that need it, avoiding re-parsing the file for every test.
    
    Parameters
    ----------
    example_file_path : pathlib.Path
        The fixture providing the path to the example file.
        
    Returns
    -------
    list[Block]
        The list of parsed Block objects.
    """
    parser = NonStandardParser(file_path=str(example_file_path), use_skip=True)
    blocks = parser.parse()
    return blocks


# --- Test Class ---

class TestNonStandardParser:
    """
    A test class to group all integration tests for the NonStandardParser.
    
    This class tests the parser's functionality end-to-end, using the
    `nonstandard_file_example.txt` file as its primary data source.
    """

    def test_init(self, example_file_path):
        """
        Tests that the NonStandardParser class initializes correctly.
        
        Parameters
        ----------
        example_file_path : pathlib.Path
            The fixture providing the path to the example file.
        """
        parser = NonStandardParser(file_path=str(example_file_path), use_skip=True)
        assert parser.file_path == str(example_file_path)
        assert parser.use_skip is True
        assert parser.lines == []
        assert parser.blocks == []

    def test_fetch_local_file(self, example_file_path):
        """
        Tests the parser's ability to read a local file.
        
        Parameters
        ----------
        example_file_path : pathlib.Path
            The fixture providing the path to the example file.
        """
        parser = NonStandardParser(file_path=str(example_file_path))
        parser._fetch_lines()
        assert len(parser.lines) > 0
        assert "Study Title" in parser.lines[0]

    def test_fetch_url(self):
        """
        Tests the parser's ability to fetch a file from a URL.

        This test uses the built-in 'unittest.mock' to "mock"
        the `requests.get` call, avoiding an external dependency.
        """
        # 1. Create a mock response object
        class MockResponse:
            status_code = 200
            text = "line1\nline2\nline3"
            def raise_for_status(self):
                pass

        # 2. Use mock.patch.object as a context manager
        with mock.patch.object(requests, "get", return_value=MockResponse()) as mock_get:
            parser = NonStandardParser(file_path="http://example.com/test.txt")
            parser._fetch_lines()

            # 3. Assert that requests.get was called correctly
            mock_get.assert_called_once_with("http://example.com/test.txt")
            assert parser.lines == ["line1", "line2", "line3"]

    def test_parse_no_data_marker_raises_error(self, tmp_path):
        """
        Tests that parsing with use_skip=True on a file without a
        "DATA:" marker correctly raises a ValueError.
        
        Parameters
        ----------
        tmp_path : pathlib.Path
            A pytest fixture providing a temporary directory.
        """
        # Create a dummy file in the temporary directory
        p = tmp_path / "no_data.txt"
        p.write_text("This file has no data marker.")
        
        parser = NonStandardParser(file_path=str(p), use_skip=True)
        
        with pytest.raises(ValueError, match="No Data Descriptor"):
            parser.parse()

    def test_parse_block_count(self, parsed_blocks):
        """
        Tests that parsing the example file results in the
        correct total number of blocks (as commented in the file).
        
        Parameters
        ----------
        parsed_blocks : list[Block]
            The fixture providing the parsed blocks.
        """
        # Based on the 8 commented cases in 'nonstandard_file_example.txt'
        assert len(parsed_blocks) == 19

    def test_case_1_complete_tabular(self, parsed_blocks):
        """
        Tests CASE 1: A "perfect" Complete Tabular block (Block 0).
        
        Checks for:
        - Correct block type (COMPLETE_TABULAR)
        - DataFrame creation and correct shape (10 rows, 9 cols)
        - Correct header names
        
        Parameters
        ----------
        parsed_blocks : list[Block]
            The fixture providing the parsed blocks.
        """
        block = parsed_blocks[10]
        
        assert block.block_type == BlockType.COMPLETE_TABULAR
        assert block.df is not None
        assert isinstance(block.df, pd.DataFrame)
        assert block.df.shape == (34, 3)
        assert "Depth(cm)" in block.df.columns
        assert "YearAD" in block.df.columns
        assert block.title is None # Should have no title

    def test_case_2_complete_tabular_with_title(self, parsed_blocks):
        """
        Tests CASE 2: A Complete Tabular block with a title (Block 1).
        
        Checks for:
        - Correct block type (COMPLETE_TABULAR)
        - Correct title detection
        - DataFrame creation and correct shape (35 rows, 3 cols)
        
        Parameters
        ----------
        parsed_blocks : list[Block]
            The fixture providing the parsed blocks.
        """
        block = parsed_blocks[-1]
        
        assert block.block_type == BlockType.TABULAR
        assert block.df is not None
        assert block.df.shape == (6, 10)
        assert "d13C-TOC" in block.df.columns
        assert 1 not in parsed_blocks[0].used_as_header_for # Should not borrow

    def test_case_3_data_block_borrowing(self, parsed_blocks):
        """
        Tests CASE 3: A DATA block that borrows from Block 0 (Block 2).
        
        Checks for:
        - Correct block type (DATA)
        - Confirms header borrowing occurred from Block 0
        - Correct headers and DataFrame shape
        
        Parameters
        ----------
        parsed_blocks : list[Block]
            The fixture providing the parsed blocks.
        """
        block = parsed_blocks[4]
        header_block = parsed_blocks[2]
        
        assert block.block_type == BlockType.DATA
        assert block.df is not None
        assert 4 in header_block.used_as_header_for
        assert block.headers == header_block.headers
        assert block.df.shape == (10, 9)
        assert "9 230Th Age (AD) (corrected)" in block.df.columns

    def test_case_4_tabular_overlap_assignment(self, parsed_blocks):
        """
        Tests CASE 4: A TABULAR (imperfect) block (Block 3).
        
        Checks for:
        - Correct block type (TABULAR)
        - Multi-line header merging
        - DataFrame creation (implying overlap/proximity fallback worked)
        - Correct shape (12 rows, 11 cols)
        - Correct assignment of a difficult, line-wrapped token
        
        Parameters
        ----------
        parsed_blocks : list[Block]
            The fixture providing the parsed blocks.
        """
        block = parsed_blocks[12]
        
        assert block.block_type == BlockType.TABULAR
        assert block.df is not None
        assert block.df.shape == (12, 11)
        
        # Check that multi-line headers merged correctly
        assert "Depth to top (mm)" in block.df.columns
        assert "Age (yr BP)* corrected" in block.df.columns
        
        # Check that the token '8035 58' from a new line was
        # correctly assigned to the first row, last column.
        assert "8035 �58" in block.df.iloc[0, -1]
        assert "62 �5" in block.df.iloc[0, 3]

    def test_case_5_tabular_borrowing(self, parsed_blocks):
        """
        Tests CASE 5: A TABULAR block that borrows from a *previous*
        TABULAR block (Block 4 borrows from Block 3).
        
        Checks for:
        - Correct block type (TABULAR)
        - Confirms borrowing occurred from Block 3
        - Correct headers and DataFrame shape
        
        Parameters
        ----------
        parsed_blocks : list[Block]
            The fixture providing the parsed blocks.
        """
        block = parsed_blocks[14]
        header_block = parsed_blocks[12]
        
        assert block.block_type == BlockType.TABULAR
        assert block.df is not None
        assert 14 in header_block.used_as_header_for
        assert block.headers == header_block.headers
        assert block.df.shape == (13, 11)
        assert "d234U initial corrected" in block.df.columns

    def test_case_6_header_only(self, parsed_blocks):
        """
        Tests CASE 6: A HEADER_ONLY block (Block 5).
        
        Checks for:
        - Correct block type (HEADER_ONLY)
        - No DataFrame
        - Correct headers were extracted
        
        Parameters
        ----------
        parsed_blocks : list[Block]
            The fixture providing the parsed blocks.
        """
        block = parsed_blocks[2]
        
        assert block.block_type == BlockType.HEADER_ONLY
        assert block.df is None
        assert len(block.headers) == 9
        assert "1 Sample Number" in [h['name'] for h in block.headers]
        assert "2 238U (ppb)" in [h['name'] for h in block.headers]

    def test_case_7_data_borrow_from_header(self, parsed_blocks):
        """
        Tests CASE 7: A DATA block borrowing from a HEADER_ONLY block
        (Block 6 borrows from Block 5).
        
        Checks for:
        - Correct block type (DATA)
        - Confirms borrowing from Block 5
        - Correct DataFrame shape and columns
        
        Parameters
        ----------
        parsed_blocks : list[Block]
            The fixture providing the parsed blocks.
        """
        block = parsed_blocks[4]
        header_block = parsed_blocks[2]
        
        assert block.block_type == BlockType.DATA
        assert block.df is not None
        assert 4 in header_block.used_as_header_for
        assert block.headers == header_block.headers
        assert block.df.shape == (10, 9)
