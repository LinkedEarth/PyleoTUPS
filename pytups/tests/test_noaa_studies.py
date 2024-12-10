import pytest
from core.noaa_studies import NOAAStudies
import pandas as pd

# Fixtures to simulate response data and expected outcomes
@pytest.fixture
def noaa_studies():
    return NOAAStudies()

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
          "abstract": "We present a reconstruction of El Nino Southern Oscillation (ENSO) variability spanning the Medieval Climate Anomaly (MCA, A.D. 800-1300) and the Little Ice Age (LIA, A.D. 1500-1850). Changes in ENSO are estimated by comparing the spread and symmetry of d18O values of individual speciments of the thermocline-dwelling foraminifer Pulleniatina obliquiloculata extracted from discrete time horizons of a sediment core collected in the Sulawesi Sea, at the edge of the western tropical Pacific warm pool. The spread of individual d18O values is interpreted to be a measure of the strength of both phases of ENSO while the symmetry of the d18O distributions is used to evaluate the relative strength/frequency of El Nino and La Nina events. In contrast to previous studies, we use robust and resistant statistics to quantify the spread and symmetry of the d18O distributions; an approach motivated by the relatively small sample size and the presence of outliers. Furthermore, we use a pseudo-proxy to investigate the effects of the different paleo-environmental factors on the statistics of the d18O distributions, which could bias the paleo-ENSO reconstruction. We find no systematic difference in the magnitude/strength of ENSO during the Nothern Hemisphere MCA or LIA. However, our results suggest that ENSO during the MCA was kewed toward stronger/more frequent La Nina than El Nino, an observation consistent with the medieval megadroughts documented from sites in western North America.",
          "pubRank": "1"
        }
      ],
      "site": [
        {
          "NOAASiteId": "53040",
          "siteName": "MD98-2177",
          "locationName": "Ocean\u003EPacific Ocean\u003EWestern Pacific Ocean",
          "geo": {
            "geoType": "Feature",
            "geometry": {
              "type": "POINT",
              "coordinates": [
                "1.4033",
                "119.078"
              ]
            },
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
                    {
                      "cvShortName": None
                    },
                    {
                      "cvShortName": None
                    },
                    {
                      "cvShortName": None
                    },
                    {
                      "cvShortName": None
                    },
                    {
                      "cvShortName": None
                    },
                    {
                      "cvShortName": None
                    },
                    {
                      "cvShortName": None
                    }
                  ],
                }
              ]
            }
          ]
        }
      ],
    }
    ]}

def test_search_studies(noaa_studies, mock_response):
    noaa_studies.response_parser(mock_response)
    
    assert '18315' in noaa_studies.studies
    assert noaa_studies.studies['18315']['base_meta']['studyName'] == 'Makassar Strait - Single specimens of P. obliquiloculata d18O and d13C from 704-1851 AD'

# Test for get_publications
def test_get_publications(noaa_studies, mock_response):
    # Manually invoke the response parser as if it was filled by search_studies
    noaa_studies.response_parser(mock_response)
    
    result = noaa_studies.get_publications('18315')
    print(result)
    assert len(result) == 1
    assert result.iloc[0]['NOAAStudyId'] == '18315'

# Test for get_sites
def test_get_sites(noaa_studies, mock_response):
    noaa_studies.response_parser(mock_response)
    
    result = noaa_studies.get_sites('18315')
    assert len(result) == 1
    assert result.iloc[0]['siteName'] == "MD98-2177"

# Test for get_data
def test_get_data(noaa_studies, mock_response):
    noaa_studies.response_parser(mock_response)

    data = noaa_studies.get_data(['28674'])
    assert data[0].iloc[0]['d13CcarbVPDB'] == '0.936'

if __name__ == "__main__":
    pytest.main()
