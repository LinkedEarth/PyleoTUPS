import logging
from typing import Tuple, List
from .constants import DATA_PUBLISHER, DEFAULT_LIMIT
from .validators import (
    to_YN, _coerce_multi,
    normalize_investigator, normalize_species_code, normalize_passthrough, normalize_search_text,
    validate_and_or, validate_int, validate_int_range,
    validate_time_format, validate_time_method, validate_digits,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[%(asctime)s][%(levelname)s] - %(message)s')

# (py_name, api_name, and_or_py, and_or_api, item_normalizer)
MULTI_SPECS = [
    ("investigators",    "investigators",    "investigators_and_or",    "investigatorsAndOr",    normalize_investigator),
    ("locations",        "locations",        "locations_and_or",        "locationsAndOr",        normalize_passthrough),
    ("keywords",         "keywords",         "keywords_and_or",         "keywordsAndOr",         normalize_passthrough),
    ("species",          "species",          "species_and_or",          "speciesAndOr",          normalize_species_code),
    ("variable_name",    "cvWhats",          "variable_name_and_or",    "cvWhatsAndOr",          normalize_passthrough),
    ("cv_materials",     "cvMaterials",      "cv_materials_and_or",     "cvMaterialsAndOr",      normalize_passthrough),
    ("cv_seasonalities", "cvSeasonalities",  "cv_seasonalities_and_or", "cvSeasonalitiesAndOr",  normalize_passthrough),
]


def build_noaa_payload(**kwargs) -> Tuple[dict, List[str]]:
    """
    Normalize user kwargs (Pythonic names) into NOAA study search payload (camelCase).
    Returns (payload, notes). 'notes' contains human-readable info about defaults/normalizations.
    """
    notes: List[str] = []
    payload: dict = {}

    # Defaults
    if kwargs.get("data_type_id") is not None:
        payload["dataTypeID"] = kwargs.get("data_type_id") 
    payload["dataPublisher"] = DATA_PUBLISHER
    
    # Identifier short-circuit
    xml_id  = kwargs.get("xml_id")
    noaa_id = kwargs.get("noaa_id")
    if xml_id is not None or noaa_id is not None:
        if xml_id  is not None:
            payload["xmlId"] = validate_digits(xml_id)
        if noaa_id is not None:
            payload["NOAAStudyId"] = validate_digits(noaa_id)
        
        # Ignore all other filters by design
        # notes.append("Using identifier-only fetch (xml_id/NOAAStudyId). Other parameters will be ignored.")
        # return payload, notes

    
    payload["limit"] = kwargs.get("limit", DEFAULT_LIMIT)
    if payload["limit"] != DEFAULT_LIMIT:
        notes.append(f"Limit set to {payload['limit']}.")
    else:
        notes.append(f"Limit defaulted to {DEFAULT_LIMIT} (PyleoTUPS).")
    if (v := kwargs.get("skip")) is not None:
        payload["skip"] = validate_int("skip", v)

    # search_text
    st = kwargs.get("search_text")
    if st is not None:
        payload["searchText"] = normalize_search_text(st)

    # Multi-value fields
    for py_name, api_name, and_or_py, and_or_api, item_norm in MULTI_SPECS:
        value = kwargs.get(py_name)
        if value is not None:
            joined, n = _coerce_multi(py_name, value, item_norm)
            if joined:
                payload[api_name] = joined
                if n >= 2:
                    payload[and_or_api] = validate_and_or(kwargs.get(and_or_py, "or"))
                else:
                    # do not send xxxAndOr for a single item
                    if kwargs.get(and_or_py) is not None:
                        notes.append(f"{and_or_api} omitted (only one value supplied for {api_name}).")

    # Geo
    if (v := kwargs.get("min_lat")) is not None:
        payload["minLat"] = validate_int_range("min_lat", v, -90, 90)
    if (v := kwargs.get("max_lat")) is not None:
        payload["maxLat"] = validate_int_range("max_lat", v, -90, 90)
    if (v := kwargs.get("min_lon")) is not None:
        payload["minLon"] = validate_int_range("min_lon", v, -180, 180)
    if (v := kwargs.get("max_lon")) is not None:
        payload["maxLon"] = validate_int_range("max_lon", v, -180, 180)
    
    notes.append("Input Query includes geographical bounds. Inspect the results to ensure they match your intended region as one study can contain sites across various parts of the world.")

    # Elevation (any ints allowed)
    if (v := kwargs.get("min_elevation")) is not None:
        payload["minElev"] = validate_int("min_elevation", v)
    if (v := kwargs.get("max_elevation")) is not None:
        payload["maxElev"] = validate_int("max_elevation", v)

    # Time window rule: if either year provided and neither time_format/time_method provided → default time_format='CE'
    ey = kwargs.get("earliest_year")
    ly = kwargs.get("latest_year")
    if ey is not None:
        payload["earliestYear"] = validate_int("earliest_year", ey)
    if ly is not None:
        payload["latestYear"] = validate_int("latest_year", ly)
    if ey is not None or ly is not None:
        tf = kwargs.get("time_format")
        tm = kwargs.get("time_method")
        if tf is None and tm is None:
            tf = "CE"
            notes.append("time_format not provided; defaulted to 'CE'.")
        if tf is not None:
            payload["timeFormat"] = validate_time_format(tf)
        if tm is not None:
            payload["timeMethod"] = validate_time_method(tm)

    # Flags
    recon = to_YN(kwargs.get("reconstruction"))
    if recon is not None:
        payload["reconstructionsOnly"] = recon

    if kwargs.get("recent"):
        payload["recent"] = "true"

    return payload, notes

# -------------------------------------------------------
# PANGAEA QUERY BUILDER
# -------------------------------------------------------

def _ensure_list(v):
    if isinstance(v, (list, tuple, set)):
        return list(v)
    return [v]


def build_pangaea_query(**kwargs):
    """
    Translate NOAA-style kwargs into PANGAEA query parameters.

    Returns
    -------
    dict
        {
            "q": str,
            "bbox": tuple or None,
            "limit": int,
            "offset": int
        }
    """

    import logging
    log = logging.getLogger(__name__)

    parts = []

    # ---------------------------------------------------
    # GEO HANDLING (ALWAYS handled, even if q is provided)
    # ---------------------------------------------------
    min_lat = kwargs.get("min_lat")
    max_lat = kwargs.get("max_lat")
    min_lon = kwargs.get("min_lon")
    max_lon = kwargs.get("max_lon")

    geo_params = [min_lat, max_lat, min_lon, max_lon]

    if any(v is not None for v in geo_params):
        if not all(v is not None for v in geo_params):
            log.warning(
                "Incomplete geographic bounds provided. "
                "PANGAEA requires min_lat, max_lat, min_lon, max_lon together. "
                "Ignoring geographic filter."
            )
            bbox = None
        else:
            bbox = (min_lon, min_lat, max_lon, max_lat)
    else:
        bbox = None

    # ---------------------------------------------------
    # DIRECT q (override other query params)
    # ---------------------------------------------------
    if kwargs.get("q"):
        q = kwargs["q"]

    else:
        # -----------------------------------------------
        # search_text → raw query
        # -----------------------------------------------
        if kwargs.get("search_text"):
            parts.append(str(kwargs["search_text"]))

        # -----------------------------------------------
        # investigators → author:
        # -----------------------------------------------
        if kwargs.get("investigators"):
            vals = _ensure_list(kwargs["investigators"])
            parts.extend([f"author:{v}" for v in vals])

        # -----------------------------------------------
        # variables → parameter:
        # -----------------------------------------------
        if kwargs.get("variable_name"):
            vals = _ensure_list(kwargs["variable_name"])
            parts.extend([f"parameter:{v}" for v in vals])

        # -----------------------------------------------
        # keywords → raw
        # -----------------------------------------------
        if kwargs.get("keywords"):
            vals = _ensure_list(kwargs["keywords"])
            parts.extend([str(v) for v in vals])

        # -----------------------------------------------
        # final query string
        # -----------------------------------------------
        q = " ".join(parts).strip()

    # ---------------------------------------------------
    # VALIDATION
    # ---------------------------------------------------
    if not q and not bbox:
        raise ValueError(
            "At least one query parameter must be provided "
            "(bbox, search_text, investigators, variables, or q)."
        )

    # ---------------------------------------------------
    # LIMIT / OFFSET
    # ---------------------------------------------------
    limit = kwargs.get("limit", 100)
    log.info(f"Limit defaulted to {DEFAULT_LIMIT} (PyleoTUPS).") if limit == 100 else log.info(f"Limit set to {limit}")
    offset = kwargs.get("skip", 0)

    return {
        "q": q,
        "bbox": bbox,
        "limit": limit,
        "offset": offset,
    }