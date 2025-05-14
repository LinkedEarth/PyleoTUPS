# tests/test_Dataset.py

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pyleotups.core import Dataset
from pyleotups.tests.helpers.mock_study_response import get_mock_study_response


# ------------------------
# Functional Tests
# ------------------------

class TestDatasetSearchStudiesFunctional:

    @patch("pyleotups.core.Dataset.requests.get")
    def test_search_studies_t01_parse_response(self, mock_get):
        """Parses mock NOAA study response correctly"""

        ds = Dataset()
        mock_data = get_mock_study_response()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = lambda: None
        mock_response.json.return_value = mock_data
        mock_get.return_value = mock_response

        df = ds.search_studies(keywords="ENSO")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    @patch("pyleotups.core.Dataset.requests.get")
    def test_search_studies_t02_output_structure(self, mock_get):
        """DataFrame has expected columns"""

        ds = Dataset()
        mock_data = get_mock_study_response()

        mock_response = MagicMock()
        mock_response.raise_for_status = lambda: None
        mock_response.json.return_value = mock_data
        mock_get.return_value = mock_response

        df = ds.search_studies(keywords="ENSO")
        expected_cols = {"StudyID", "DataType", "Publications", "Sites", "Funding"}
        assert expected_cols.issubset(df.columns)

    @patch("pyleotups.core.Dataset.requests.get")
    def test_search_studies_t04_internal_object_population(self, mock_get):
        """Dataset internal study objects are populated"""

        ds = Dataset()
        mock_data = get_mock_study_response()

        mock_response = MagicMock()
        mock_response.raise_for_status = lambda: None
        mock_response.json.return_value = mock_data
        mock_get.return_value = mock_response

        ds.search_studies(keywords="ENSO")
        assert len(ds.studies) > 0
        assert isinstance(next(iter(ds.studies.values())).metadata, dict)

    @patch("pyleotups.core.Dataset.requests.get")
    def test_search_studies_t05_handles_empty_study_list(self, mock_get):
        """Handles 'study': [] case without error"""

        ds = Dataset()
        mock_response = MagicMock()
        mock_response.raise_for_status = lambda: None
        mock_response.json.return_value = {"study": []}
        mock_get.return_value = mock_response

        df = ds.search_studies(keywords="ENSO")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    @patch("pyleotups.core.Dataset.requests.get")
    def test_search_studies_t07_dataframe_matches_expected_length(self, mock_get):
        """DataFrame row count matches number of studies in mock JSON"""

        ds = Dataset()
        mock_data = get_mock_study_response()
        mock_response = MagicMock()
        mock_response.raise_for_status = lambda: None
        mock_response.json.return_value = mock_data
        mock_get.return_value = mock_response

        df = ds.search_studies(keywords="ENSO")
        assert len(df) == len(mock_data["study"])


# ------------------------
# Error Handling Tests
# ------------------------

class TestDatasetSearchStudiesErrorHandling:

    def test_search_studies_t01_empty_params_raises(self):
        """Raises ValueError when no parameters are passed"""

        ds = Dataset()
        with pytest.raises(ValueError, match="At least one search parameter must be specified"):
            ds.search_studies()

    def test_search_studies_t02_invalid_publisher_raises(self):
        """Raises NotImplementedError for unsupported publisher"""

        ds = Dataset()
        with pytest.raises(NotImplementedError, match="does not support 'PANGAEA'"):
            ds.search_studies(data_publisher="PANGAEA", keywords="ENSO")

    @patch("pyleotups.core.Dataset.requests.get")
    def test_search_studies_t03_http_error_handled(self, mock_get):
        """Simulates an HTTP error like 503"""

        ds = Dataset()
        mock_get.side_effect = Exception("503 Service Unavailable")

        with pytest.raises(RuntimeError, match="Failed to fetch or parse response"):
            ds.search_studies(keywords="ENSO")

    @patch("pyleotups.core.Dataset.requests.get")
    def test_search_studies_t04_invalid_json_handled(self, mock_get):
        """Simulates malformed JSON"""

        ds = Dataset()

        class FakeResponse:
            def raise_for_status(self): pass
            def json(self): raise ValueError("Invalid JSON")

        mock_get.return_value = FakeResponse()

        with pytest.raises(RuntimeError, match="Failed to fetch or parse response"):
            ds.search_studies(keywords="ENSO")

    @patch("pyleotups.core.Dataset.requests.get")
    def test_search_studies_t05_missing_study_key_handled(self, mock_get):
        """Simulates missing 'study' key in valid JSON"""

        ds = Dataset()
        bad_response = {"foo": "bar"}

        class FakeResponse:
            def raise_for_status(self): pass
            def json(self): return bad_response

        mock_get.return_value = FakeResponse()
        df = ds.search_studies(keywords="ENSO")

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    @patch("pyleotups.core.Dataset.requests.get")
    def test_search_studies_t06_keywords_return_no_results(self, mock_get):
        """Simulates valid query returning no results"""

        ds = Dataset()
        mock_response = MagicMock()
        mock_response.raise_for_status = lambda: None
        mock_response.json.return_value = {"study": []}
        mock_get.return_value = mock_response

        df = ds.search_studies(keywords="no_match_expected")
        assert isinstance(df, pd.DataFrame)
        assert df.empty
