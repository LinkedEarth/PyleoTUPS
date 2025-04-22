import pytest
import pandas as pd
import pytups
# from pytups.core.Dataset import Dataset

@pytest.fixture
def noaa_wrapper():
    return pytups.core.Dataset()

@pytest.fixture
def mock_response():
    return {
        "study": [
            {
                "xmlId": "16017",
                "NOAAStudyId": "18315",
                "studyName": "Makassar Strait - Single specimens of P. obliquiloculata d18O and d13C from 704-1851 AD",
                "dataPublisher": "NOAA",
                "dataType": "PALEOCEANOGRAPHY",
                "investigatorDetails": [
                    {
                        "firstName": "Deborah",
                        "lastName": "Khider",
                        "initials": "D.",
                        "orcId": "0000-0001-7501-8430"
                    },
                    {
                        "firstName": "Lowell",
                        "lastName": "Stott",
                        "initials": "L.D.",
                        "orcId": "0000-0002-2025-0731"
                    },
                    {
                        "firstName": "Julien",
                        "lastName": "Emile-Geay",
                        "initials": "J.",
                        "orcId": "0000-0001-5920-4751"
                    },
                    {
                        "firstName": "Robert",
                        "lastName": "Thunell",
                        "initials": "R.C.",
                        "orcId": "0000-0001-7052-1707"
                    },
                    {
                        "firstName": "Doug",
                        "lastName": "Hammond",
                        "initials": "D.E.",
                        "orcId": None
                    }
                ],
                "studyNotes": "This dataset contains the d18O, d13C, and weights of single specimens of P. obliquiloculata used in the reconstruction of ENSO variability over the past 2,000 years.",
                "onlineResourceLink": "https://www.ncei.noaa.gov/access/paleo-search/study/18315",
                "scienceKeywords": [
                    "ENSO",
                    "Medieval Climate Anomaly (MCA)",
                    "Little Ice Age (LIA)"
                ],
                "earliestYearBP": 1246,
                "mostRecentYearBP": 99,
                "earliestYearCE": 704,
                "mostRecentYearCE": 1851,
                "publication": [
                    {
                        "author": {
                            "name": "Khider, D"
                        },
                        "pubYear": 2011,
                        "title": "Assessing El Nino Southern Oscillation variability during the past millennium",
                        "journal": "Paleoceanography",
                        "volume": "26",
                        "edition": None,
                        "issue": None,
                        "pages": None,
                        "reportNumber": "PA3222",
                        "citation": "Khider, D., L. Stott, J. Emile-Geay, R. Thunell, and D.E. Hammond. 2011. Assessing El Nino Southern Oscillation variability during the past millennium. Paleoceanography, 26, PA3222. doi: 10.1029/2011PA002139",
                        "type": "publication",
                        "identifier": {
                            "type": "doi",
                            "id": "10.1029/2011PA002139",
                            "url": "http://dx.doi.org/10.1029/2011PA002139"
                        },
                        "abstract": "We present a reconstruction ...",
                        "pubRank": "1"
                    }
                ],
                "site": [
                    {
                        "NOAASiteId": "53040",
                        "siteName": "MD98-2177",
                        "locationName": "Ocean>Pacific Ocean>Western Pacific Ocean",
                        "geo": {
                            "geoType": "Feature",
                            "geometry": {
                                "type": "POINT",
                                "coordinates": [
                                    "1.4033",
                                    "119.078"
                                ]
                            },
                            "properties": {
                                "minElevationMeters": None,
                                "maxElevationMeters": None
                            }
                        },
                        "paleoData": [
                            {
                                "dataTableName": "MD98-2177 isotopes Khider11",
                                "NOAADataTableId": "28674",
                                "timeUnit": "CE",
                                "dataFile": [
                                    {
                                        "fileUrl": "https://www.ncei.noaa.gov/pub/data/paleo/contributions_by_author/khider2011/khider2011.txt",
                                        "variables": [
                                            {"cvShortName": None},
                                            {"cvShortName": None},
                                            {"cvShortName": None},
                                            {"cvShortName": None},
                                            {"cvShortName": None},
                                            {"cvShortName": None},
                                            {"cvShortName": None}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }

def test_search_studies(noaa_wrapper, mock_response):
    # Directly call the _parse_response method (private, but acceptable for testing)
    noaa_wrapper._parse_response(mock_response)
    # Check that the study is loaded by its NOAAStudyId.
    assert '18315' in noaa_wrapper.studies
    study = noaa_wrapper.studies['18315']
    assert study.metadata['studyName'] == 'Makassar Strait - Single specimens of P. obliquiloculata d18O and d13C from 704-1851 AD'

def test_get_publications_dataframe(noaa_wrapper, mock_response):
    noaa_wrapper._parse_response(mock_response)
    pubs_df = noaa_wrapper.get_publications_dataframe()
    # Ensure at least one publication is returned and it contains the correct StudyID.
    assert len(pubs_df) >= 1
    assert pubs_df.iloc[0]['StudyID'] == '18315'

def test_get_sites_dataframe(noaa_wrapper, mock_response):
    noaa_wrapper._parse_response(mock_response)
    sites_df = noaa_wrapper.get_sites_dataframe()
    # Ensure at least one site is returned and it has the correct site name.
    assert len(sites_df) >= 1
    assert sites_df.iloc[0]['SiteName'] == "MD98-2177"

def test_get_data(monkeypatch, noaa_wrapper, mock_response):
    noaa_wrapper._parse_response(mock_response)
    
    # Create a dummy DataFrame to simulate fetched file data.
    dummy_df = pd.DataFrame({
        'd13CcarbVPDB': ['0.936']
    })
    
    # Define a fake fetch_data function that returns our dummy DataFrame.
    def fake_fetch_data(file_url):
        return dummy_df
    
    # Monkeypatch DataFetcher.fetch_data in the StandardParser module.
    from pytups.utils.Parser.StandardParser import DataFetcher
    monkeypatch.setattr(DataFetcher, 'fetch_data', fake_fetch_data)
    
    data_list = noaa_wrapper.get_data(['28674'])
    # Check that at least one DataFrame is returned and contains the expected value.
    assert len(data_list) >= 1
    assert data_list[0].iloc[0]['d13CcarbVPDB'] == '0.936'

if __name__ == "__main__":
    pytest.main()