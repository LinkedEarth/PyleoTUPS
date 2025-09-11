# pyleotups/utils/api/constants.py
from __future__ import annotations

# NOAA endpoint (studies)
BASE_URL = "https://www.ncei.noaa.gov/access/paleo-search/study/search.json"

# Project policy
DATA_PUBLISHER = "NOAA"
DEFAULT_LIMIT = 100       # PyleoTUPS default (NOAA's is 10)

# Allowed sets
TIME_FORMATS = {"CE", "BP"}
TIME_METHODS = {"overAny", "entireOver", "overEntire"}
