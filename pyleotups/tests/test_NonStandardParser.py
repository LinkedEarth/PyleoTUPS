import pytest
import pandas as pd
from pyleotups.utils.Parser.NonStandardParser import NonStandardParser, ParsingError  

@pytest.fixture
def parser():
    return NonStandardParser(file_path=None) 

def test_narrative_block(parser):
    block = {"lines": [
        "This is a description paragraph about the dataset.",
        "It has no numeric data or clear table structure."
    ]}
    parser._compute_statistics(block)
    parser._process_block(block)
    assert block["block_type"] == "narrative"
    assert "df" not in block

def test_header_only_block(parser):
    block = {"lines": [
        "Sample    Age    Value",
        "           (yr)    (unit)"
    ]}
    parser._compute_statistics(block)
    parser._process_block(block)
    assert block["block_type"] == "header-only"

def test_complete_tabular_block(parser):
    block = {"lines": [
        "Sample     Age    Value",
        "1         100    10.5",
        "2         200    12.0",
        "3         300    13.5"
    ]}
    parser._compute_statistics(block)
    parser._process_block(block)
    print(block["mode_space_tokens"])
    print(block["cv_multispace_tokens"])
    print(parser._detect_delimiter(block["lines"]))
    # assert block["block_type"] == "complete-tabular"
    assert isinstance(block["df"], pd.DataFrame)
    assert list(block["df"].columns) == ["Sample", "Age", "Value"]

def test_messy_recoverable_block(parser):
    block = {"lines": [
        "Sample      Age      Value",
        "S1  100  10.5",
        "S2  200  12.0",
        "S3  300  13.5"
    ]}
    parser._compute_statistics(block)
    # print(block["mode_space_tokens"])
    parser._process_block(block)
    assert block["block_type"] == "complete-tabular"
    assert isinstance(block["df"], pd.DataFrame)

def test_unprocessable_block(parser):
    block = {"lines": [
        "------ ------ ------",
        "%%%%% $$$$$ *****",
        "::::: !!!!! :::::"
    ]}
    parser._compute_statistics(block)
    parser._process_block(block)
    assert block["block_type"] == "narrative"
    assert "df" not in block
