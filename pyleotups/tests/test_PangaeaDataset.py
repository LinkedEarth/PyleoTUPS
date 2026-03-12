import pytest
from unittest.mock import patch
import re 

import pandas as pd

from pyleotups.core import PangaeaDataset
from pyleotups.tests.helpers.mock_pangaea_response import (
    get_mock_metadata,
    get_mock_data,
)


# -----------------------------
# Mock Response Object
# -----------------------------
class MockResponse:
        def __init__(self, text, accept):
            self.text = text
            self.status_code = 200
            self.headers = {"Content-Type": accept}

        def raise_for_status(self):
            pass

# -----------------------------
# Mock get_request
# -----------------------------
def mock_requests_get(url, headers=None, **kwargs):
    
    match = re.search(r"PANGAEA\.(\d+)", url)
    if not match:
        raise ValueError(f"Cannot extract StudyID from URL: {url}")

    study_id = int(match.group(1))
    accept = headers.get("Accept") if headers else None

    if accept == "application/vnd.pangaea.metadata+xml":
        text = get_mock_metadata(study_id)
    elif accept == "text/tab-separated-values":
        text = get_mock_data(study_id)
    elif accept == "text/x-bibliography":
        # Minimal valid citation string
        text = f"Mock Author (2020): Mock Dataset {study_id}. PANGAEA."
    else:
        raise ValueError(f"Unexpected Accept header: {accept}")

    return MockResponse(text, accept)


class TestPangaeaDatasetOffline:

    @patch("pangaeapy.pandataset.requests.get", side_effect=mock_requests_get)
    @patch("pyleotups.core.PangaeaDataset.PanQuery")
    def test_search_and_summary_with_collection(self, mock_panquery, mock_requests, caplog):

        mock_panquery.return_value.result = [
            {"URI": "10.1594/PANGAEA.830589"}
        ]

        ds = PangaeaDataset()
        ds.search_studies(q="test")

        assert 830589 in ds.studies

        with caplog.at_level("WARNING"):
            df = ds.get_summary()

        # CollectionMembers column exists
        assert "CollectionMembers" in df.columns

        row = df.loc[df["StudyID"] == 830589].iloc[0]
        assert row["CollectionMembers"] is not None

        # Assert at least one warning logged
        assert any(
            "marked as collection" in rec.message
            for rec in caplog.records
        )
    
    @patch("pangaeapy.pandataset.requests.get", side_effect=mock_requests_get)
    @patch("pyleotups.core.PangaeaDataset.PanQuery")
    def test_get_data_collection_warns(self, mock_panquery, mock_requests, caplog):

        mock_panquery.return_value.result = [
            {"URI": "10.1594/PANGAEA.830589"}
        ]

        ds = PangaeaDataset()
        ds.search_studies(q="test")

        with caplog.at_level("WARNING"):
            df = ds.get_data(830589)

        assert df.empty

        assert any(
            "collection dataset" in rec.message.lower()
            for rec in caplog.records
        )
    

    @patch("pangaeapy.pandataset.requests.get", side_effect=mock_requests_get)
    @patch("pyleotups.core.PangaeaDataset.PanQuery")
    def test_get_data_collection_member_auto_register(self, mock_panquery, mock_requests):

        mock_panquery.return_value.result = [
            {"URI": "10.1594/PANGAEA.830589"}
        ]

        ds = PangaeaDataset()
        ds.search_studies(q="test")

        # Child of 830589
        df = ds.get_data(830586)

        assert not df.empty
        assert 830586 in ds.studies

    #------------------------------
    # Temporal Extracation Test
    #------------------------------
    @patch("pangaeapy.pandataset.requests.get", side_effect=mock_requests_get)
    @patch("pyleotups.core.PangaeaDataset.PanQuery")
    def test_temporal_ce_only(self, mock_panquery, mock_requests):

        mock_panquery.return_value.result = [
            {"URI": "10.1594/PANGAEA.940553"}
        ]

        ds = PangaeaDataset()
        ds.search_studies(q="test")

        summary = ds.get_summary()
        row = summary.iloc[0]

        assert row["EarliestYearCE"] is not None
        assert row["MostRecentYearCE"] is not None
        assert row["EarliestYearBP"] is not None
        assert row["MostRecentYearBP"] is not None
    
    @patch("pangaeapy.pandataset.requests.get", side_effect=mock_requests_get)
    @patch("pyleotups.core.PangaeaDataset.PanQuery")
    def test_temporal_bp_only(self, mock_panquery, mock_requests):

        mock_panquery.return_value.result = [
            {"URI": "10.1594/PANGAEA.976221"}
        ]

        ds = PangaeaDataset()
        ds.search_studies(q="test")

        summary = ds.get_summary()
        row = summary.iloc[0]

        assert row["EarliestYearBP"] is not None
        assert row["MostRecentYearBP"] is not None
        assert row["EarliestYearCE"] is not None
        assert row["MostRecentYearCE"] is not None
    
    @patch("pangaeapy.pandataset.requests.get", side_effect=mock_requests_get)
    @patch("pyleotups.core.PangaeaDataset.PanQuery")
    def test_temporal_ce_and_bp(self, mock_panquery, mock_requests):

        mock_panquery.return_value.result = [
            {"URI": "10.1594/PANGAEA.830586"}
        ]

        ds = PangaeaDataset()
        ds.search_studies(q="test")

        summary = ds.get_summary()
        row = summary.iloc[0]

        assert row["EarliestYearCE"] is not None
        assert row["MostRecentYearCE"] is not None
        assert row["EarliestYearBP"] is not None
        assert row["MostRecentYearBP"] is not None

    @patch("pangaeapy.pandataset.requests.get", side_effect=mock_requests_get)
    @patch("pyleotups.core.PangaeaDataset.PanQuery")
    def test_temporal_no_age(self, mock_panquery, mock_requests):

        mock_panquery.return_value.result = [
            {"URI": "10.1594/PANGAEA.830588"}
        ]

        ds = PangaeaDataset()
        ds.search_studies(q="test")

        summary = ds.get_summary()
        row = summary.iloc[0]

        assert row["EarliestYearCE"] is None
        assert row["MostRecentYearCE"] is None
        assert row["EarliestYearBP"] is None
        assert row["MostRecentYearBP"] is None

    # -----------------------------
    # Variables Test
    # -----------------------------
    @patch("pangaeapy.pandataset.requests.get", side_effect=mock_requests_get)
    @patch("pyleotups.core.PangaeaDataset.PanQuery")
    def test_get_variables(self, mock_panquery, mock_requests):

        mock_panquery.return_value.result = [
            {"URI": "10.1594/PANGAEA.830586"}
        ]

        ds = PangaeaDataset()
        ds.search_studies(q="test")

        df_vars = ds.get_variables(830586)

        assert not df_vars.empty
        assert set(df_vars.columns) == {
            "StudyID",
            "VariableName",
            "ShortName",
            "Unit",
            "OntologyTerms",
        }


    @patch("pangaeapy.pandataset.requests.get", side_effect=mock_requests_get)
    @patch("pyleotups.core.PangaeaDataset.PanQuery")
    def test_get_funding(self, mock_panquery, mock_requests):

        mock_panquery.return_value.result = [
            {"URI": "10.1594/PANGAEA.830586"}
        ]

        ds = PangaeaDataset()
        ds.search_studies(q="test")

        df_funding = ds.get_funding()

        # Funding may or may not exist depending on fixture
        assert isinstance(df_funding, pd.DataFrame)
    

    @patch("pangaeapy.pandataset.requests.get", side_effect=mock_requests_get)
    @patch("pyleotups.core.PangaeaDataset.PanQuery")
    def test_get_publications(self, mock_panquery, mock_requests):

        mock_panquery.return_value.result = [
            {"URI": "10.1594/PANGAEA.830586"}
        ]

        ds = PangaeaDataset()
        ds.search_studies(q="test")

        bib, df_pub = ds.get_publications()

        assert isinstance(df_pub, pd.DataFrame)
        assert len(bib.entries) > 0


class TestPangaeaDatasetErrorHandling:
    
    def test_get_data_before_search(self):
        ds = PangaeaDataset()
        with pytest.raises(KeyError):
            ds.get_data(830586)
    
    def test_get_data_before_search(self):
        ds = PangaeaDataset()
        with pytest.raises(KeyError):
            ds.get_data(830586)