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


# ------------------------
# Metadata Accessor Tests (with length checks + multi-funding support)
# ------------------------

class TestDatasetMetadataAccessors:

    def setup_method(self):
        self.ds = Dataset()
        self.mock_data = get_mock_study_response()
        self.ds._parse_response(self.mock_data)

    def test_get_summary_t01_returns_dataframe(self):
        """get_summary returns non-empty DataFrame with expected columns"""
        summary_df = self.ds.get_summary()
        assert isinstance(summary_df, pd.DataFrame)
        assert not summary_df.empty
        assert {"StudyID", "StudyName", "DataType"}.issubset(summary_df.columns)
        assert len(summary_df) == len(self.mock_data["study"])

    def test_get_geo_t02_lat_lon_structure(self):
        """get_geo returns correct number of geo points and coordinate columns"""
        geo_df = self.ds.get_geo()
        assert isinstance(geo_df, pd.DataFrame)
        assert not geo_df.empty
        # assert {"latitude", "longitude"}.issubset(geo_df.columns)

        expected_geo_count = sum(
            1
            for study in self.mock_data["study"]
            for site in study.get("site", [])
            if site.get("geo", {}).get("geometry", {}).get("type") == "POINT"
        )
        assert len(geo_df) == expected_geo_count

    def test_get_funding_t03_handles_multiple_or_missing_funding(self):
        """get_funding returns one row per funding object across all studies"""
        funding = self.ds.get_funding()
        assert isinstance(funding, pd.DataFrame)

        expected_count = sum(
            len(study.get("funding", []))
            for study in self.mock_data["study"]
        )
        assert len(funding) == expected_count

        # for f in funding:
        #     # assert isinstance(f, dict)
        #     # assert "StudyID" in f
        #     # Optional: check presence of key fields if available
        #     if "fundingAgency" in f:
        #         assert isinstance(f["fundingAgency"], str)


# ------------------------
# Metadata Detailed Field Tests
# ------------------------

class TestDatasetMetadataDetailed:

    def setup_method(self):
        self.ds = Dataset()
        self.mock_data = get_mock_study_response()
        self.ds._parse_response(self.mock_data)

    # --- get_publications() ---

    def test_get_publications_t01_bib_and_dataframe_structure(self):
        """Returns non-empty BibTeX and DataFrame with publication fields"""
        bib, df = self.ds.get_publications()

        # BibTeX
        assert hasattr(bib, "entries")
        # assert isinstance(bib.entries, dict)
        assert len(bib.entries) > 0

        # DataFrame
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert {"Author", "Title", "Journal", "Year", "DOI"}.issubset(df.columns)

    def test_get_publications_t02_saves_bibtex_file(self, tmp_path):
        """Creates a .bib file when save=True"""
        bib_path = tmp_path / "test_output.bib"
        bib, df = self.ds.get_publications(save=True, path=bib_path)

        assert bib_path.exists()
        contents = bib_path.read_text(encoding="utf-8")
        assert "@article" in contents
        assert "title" in contents.lower()

    def test_get_publications_t03_verbose_prints_to_console(self, capsys):
        """Prints BibTeX output when verbose=True"""
        self.ds.get_publications(verbose=True)
        captured = capsys.readouterr()
        assert "@article" in captured.out

    # --- get_tables() ---

    def test_get_tables_t04_table_extraction_valid(self):
        """Returns table info with required fields"""
        tables_df = self.ds.get_tables()
        assert isinstance(tables_df, pd.DataFrame)
        assert not tables_df.empty
        assert {"DataTableID", "FileURL", "StudyID"}.issubset(tables_df.columns)

    def test_get_tables_t05_handles_empty_mapping(self):
        """Returns empty DataFrame if no studies"""
        empty_ds = Dataset()
        df = empty_ds.get_tables()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    # --- get_variables() ---

    def test_get_variables_t06_valid_ids(self):
        """Returns variables for valid NOAA DataTableIDs"""
        tables_df = self.ds.get_tables()
        ids = tables_df["DataTableID"].unique().tolist()

        df = self.ds.get_variables(ids)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert {"VariableName", "StudyID", "FileURL"}.issubset(df.columns)

    def test_get_variables_t07_invalid_id_raises(self):
        """Raises ValueError if ID not in data_table_index"""
        with pytest.raises(ValueError, match="DataTableID 'xyz' not found"):
            self.ds.get_variables("xyz")


# ------------------------
# get_data() Tests (with mocks)
# ------------------------

from unittest.mock import patch, MagicMock
import pandas as pd
import pytest
from pyleotups.core import Dataset, UnsupportedFileTypeError
from pyleotups.tests.helpers.mock_study_response import get_mock_study_response


class TestDatasetGetDataMocked:

    def setup_method(self):
        self.ds = Dataset()
        self.mock_data = get_mock_study_response()
        self.ds._parse_response(self.mock_data)
        self.valid_dt_id = next(iter(self.ds.data_table_index))
        self.valid_file_url = self.ds.data_table_index[self.valid_dt_id]["paleo_data"].file_url

    # --- Test t01: valid DataTableID returns DFs ---
    @patch("pyleotups.core.Dataset.requests.get")
    @patch("pyleotups.core.Dataset.StandardParser")
    def test_get_data_t01_from_datatable_id_returns_df_list(self, mock_parser, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.raise_for_status = lambda: None
        mock_get.return_value.text = "# mock\n# mock\n# mock\n# mock\n# mock"

        dummy_df = pd.DataFrame({"depth": [1, 2, 3]})
        mock_parser.return_value.parse.return_value = dummy_df

        result = self.ds.get_data(dataTableIDs=[self.valid_dt_id])
        assert isinstance(result, list)
        assert isinstance(result[0], pd.DataFrame)
        assert result[0].equals(dummy_df)

    # --- Test t02: invalid DataTableID ---
    def test_get_data_t02_invalid_datatable_id_raises(self):
        with pytest.raises(ValueError, match="No parent study mapping found"):
            self.ds.get_data(dataTableIDs=["invalid-id"])

    # --- Test t03: valid file_url with mapping ---
    @patch("pyleotups.core.Dataset.requests.get")
    @patch("pyleotups.core.Dataset.StandardParser")
    def test_get_data_t03_from_file_url_with_mapping(self, mock_parser, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.raise_for_status = lambda: None
        mock_get.return_value.text = "# mock\n# mock\n# mock\n# mock\n# mock"

        dummy_df = pd.DataFrame({"depth": [1, 2, 3]})
        mock_parser.return_value.parse.return_value = dummy_df

        result = self.ds.get_data(file_urls=[self.valid_file_url])
        assert isinstance(result, list)
        assert result[0].equals(dummy_df)

    # --- Test t04: file_url not in mapping, should still parse ---
    @patch("pyleotups.core.Dataset.requests.get")
    @patch("pyleotups.core.Dataset.StandardParser")
    def test_get_data_t04_unmapped_file_url_warns_and_parses(self, mock_parser, mock_get):
        unmapped_url = "https://example.com/fake.txt"
        mock_get.return_value.status_code = 200
        mock_get.return_value.raise_for_status = lambda: None
        mock_get.return_value.text = "# mock\n# mock\n# mock\n# mock\n# mock"

        dummy_df = pd.DataFrame({"depth": [10, 20]})
        mock_parser.return_value.parse.return_value = dummy_df

        with pytest.warns(UserWarning, match="not linked to any parent study"):
            result = self.ds.get_data(file_urls=[unmapped_url])
            assert isinstance(result[0], pd.DataFrame)

    # --- Test t05: file with unsupported extension ---
    def test_get_data_t05_unsupported_file_type_raises(self):
        with pytest.raises(UnsupportedFileTypeError, match="Only .txt files are supported"):
            self.ds._process_file("https://example.com/data.xlsx")

    # --- Test t06: unparsable file structure ---
    @patch("pyleotups.core.Dataset.requests.get")
    def test_get_data_t06_unparsable_txt_raises_value_error(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.raise_for_status = lambda: None
        mock_get.return_value.text = "lorem\nipsum\nno headers\nno NOAA"

        with pytest.raises(ValueError, match="Unable to determine parser"):
            self.ds.get_data(file_urls=[self.valid_file_url])

    # --- Test t07: HTTP error when downloading file ---
    @patch("pyleotups.core.Dataset.requests.get")
    def test_get_data_t07_http_error_raises_runtime_error(self, mock_get):
        mock_get.side_effect = Exception("connection failed")
        with pytest.raises(RuntimeError, match="Failed to read file"):
            self.ds.get_data(file_urls=[self.valid_file_url])
