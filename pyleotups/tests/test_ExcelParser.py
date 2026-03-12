import pytest
from pathlib import Path
from pyleotups.utils.Parser.ExcelParser import ExcelParser, BlockType

@pytest.fixture(scope="session")
def local_datapath():
    return Path(__file__).parent / "data"/"noaa"

def get_file_path(datapath, filename):
    path = datapath / filename
    if not path.exists():
        pytest.skip(f"Test file not found: {filename}. Please add it to {datapath}")
    return str(path)


class TestExcelParser:
    """
    Tests the ExcelParser against specific local legacy files (.xls).
    """

    def test_mentawai2008_multisheet(self, local_datapath):
        """
        CASE 1: mentawai2008.xls
        Features: Multiple sheets, large metadata headers, standard tables.
        
        Expected Structure:
        - Readme: Metadata only.
        - Table S1 Isotopes: Metadata -> Header -> Data.
        - Table S2 DMI: Long Metadata (~10 rows) -> Header -> Data.
        """
        file_path = get_file_path(local_datapath, "mentawai2008.xls")
        parser = ExcelParser(file_path=file_path)
        blocks = parser.parse()

        # 1. Sheet Count Check
        # Expect 2 sheets (skip Readme, Table S1, Table S2)
        assert len(parser.sheets) == 2
        sheet_names = [s.name for s in parser.sheets]
        assert "Table S1 Isotopes" in sheet_names
        assert "Table S2 DMI" in sheet_names

        # 2. Verify Table S1 (Standard Table)
        # Find the block for this sheet that became a table
        s1_blocks = [b for b in blocks if b.sheet_name == "Table S1 Isotopes"]
        s1_table = next((b for b in s1_blocks if b.block_type == BlockType.COMPLETE_TABULAR), None)
        
        assert s1_table is not None, "Failed to identify Table S1 as tabular"
        assert s1_table.df is not None
        # Check Columns
        cols_s1 = list(s1_table.df.columns)
        assert "Time" in cols_s1
        assert "d18O (‰)" in cols_s1
        # Check Data (approx value from CSV snippet)
        # 1858.29, -5.5
        assert s1_table.df.iloc[0]["Time"] == 1858.29
        
        # 3. Verify Table S2 (Long Metadata Header)
        # The parser needs to skip ~10 lines of description to find "Year"
        s2_blocks = [b for b in blocks if b.sheet_name == "Table S2 DMI"]
        s2_table = next((b for b in s2_blocks if b.block_type == BlockType.COMPLETE_TABULAR), None)
        
        assert s2_table is not None, "Failed to identify Table S2 as tabular"
        assert "Year" in s2_table.df.columns
        assert "Coral DMI (Saji method)" in s2_table.df.columns
        
        # 4. Verify Block Segmentation
        # We expect the metadata at the top (e.g. "Supplementary Table 2...") 
        # to be separated into a Narrative/Metadata block, not merged into the table.
        # So s2_blocks should contain > 1 block (Metadata + Table)
        assert len(s2_blocks) >= 2 
        # Ensure the table starts lower down (e.g. row > 0)
        assert s2_table.top > 0

    def test_frank1999_borrowing(self, local_datapath):
        """
        CASE 2: frank1999.xls
        Features: Disconnected headers (Header Borrowing).
        
        Expected Structure on 'Data' sheet:
        - Header Block: "d18O, d13C..."
        - Gap / Title Block: "DSDP Site 357..."
        - Data Block: Numeric values (-0.1978...)
        """
        file_path = get_file_path(local_datapath, "frank1999.xls")
        parser = ExcelParser(file_path=file_path)
        blocks = parser.parse()

        # Filter for the 'Data' sheet
        data_sheet_blocks = [b for b in blocks if b.sheet_name == "Data"]
        
        # 1. Identify the Main Data Block
        # We look for the block containing the specific numeric value -0.1978...
        # If borrowing worked, this block is now COMPLETE_TABULAR. 
        # If it failed, it might be DATA_ONLY or NARRATIVE.
        
        target_val = -0.19782403
        data_block = None
        
        for b in data_sheet_blocks:
            if b.df is not None and not b.df.empty:
                # Check first row, first column (approximate)
                val = b.df.iloc[0, 0]
                try:
                    if abs(float(val) - target_val) < 0.001:
                        data_block = b
                        break
                except (ValueError, TypeError):
                    continue
        
        assert data_block is not None, "Could not find the specific data block in frank1999.xls"
        
        # 2. Verify Borrowing Success
        # It must have acquired headers to be useful
        assert data_block.block_type == BlockType.COMPLETE_TABULAR
        assert data_block.headers is not None
        
        # 3. Verify Header Correctness
        # These headers exist physically ~4 rows ABOVE the data block
        expected_headers = ["d18O", "d13C", "Core", "Section", "Interval", "mbsf", "Age (Ma)", "Taxa", "Habitat"]
        actual_headers = list(data_block.df.columns)
        
        # Check a few key headers
        assert "d18O" in actual_headers
        assert "Age (Ma)" in actual_headers
        assert "Taxa" in actual_headers
        
        # 4. Verify Data Alignment
        # Ensure the columns match the data (e.g. Taxa column should have strings)
        # Based on snippet: Row 0, Taxa col -> "Gavelinella beccariiformis"
        assert "Gavelinella" in str(data_block.df.iloc[0]["Taxa"])