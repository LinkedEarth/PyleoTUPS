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
        payload["dataTypeId"] = kwargs.get("data_type_id") 
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


def _build_logical_block(field_name, values, operator, formatter):
    if not values:
        return None

    if not isinstance(values, (list, tuple, set)):
        values = [values]

    values = [v for v in values if v]

    if not values:
        return None

    parts = [formatter(v) for v in values]

    if len(parts) == 1:
        return parts[0]

    
    return f"({' OR '.join(parts)})" if operator.lower() == "or" else  f"{' '.join(parts)}"   # implicit AND


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

    parts = []

    # ---------------------------------------------------
    # GEO HANDLING (ALWAYS handled, even if q is provided)
    # ---------------------------------------------------
    min_lat = kwargs.get("min_lat")
    max_lat = kwargs.get("max_lat")
    min_lon = kwargs.get("min_lon")
    max_lon = kwargs.get("max_lon")

    # -----------------------------------------------
    # topic → topic:<value>
    # -----------------------------------------------
    VALID_TOPICS = {
    "agriculture", "atmosphere", "biological classification",
    "biosphere", "chemistry", "cryosphere", "ecology",
    "fisheries", "geophysics", "human dimensions",
    "lakes & rivers", "land surface", "lithosphere",
    "oceans", "paleontology"
    }

    topic = kwargs.get("topic")

    if topic:
        # normalize to list
        if not isinstance(topic, (list, tuple, set)):
            topic = [topic]

        if topic:
            # validate
            normalized_topics = []
            invalid = []
            for t in topic:
                key = str(t).strip().lower()
                if key in VALID_TOPICS:
                    normalized_topics.append(t)   # use normalized
                elif key != "all":
                    invalid.append(t)
            if invalid:
                log.warning(
                    f"Invalid topic(s) found. Skipping: {invalid}. "
                    f"Please select from available topics: {sorted(VALID_TOPICS)}"
                )

            # build query block
            block = _build_logical_block(
                "topic",
                normalized_topics,
                kwargs.get("topic_and_or", "or"),
                lambda v: f"topic:{v}"
            )

            if block:
                parts.append(block)

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

    
    # -----------------------------------------------
    # search_text → raw query
    # -----------------------------------------------
    if kwargs.get("search_text"):
        parts.append(str(kwargs["search_text"]))

    # -----------------------------------------------
    # investigators → author:
    # -----------------------------------------------
    block = _build_logical_block(
        "investigators",
        kwargs.get("investigators"),
        kwargs.get("investigators_and_or", "and"),
        lambda v: f"author:{v}"
    )

    if block:
        parts.append(block)

    # -----------------------------------------------
    # variables → parameter:
    # -----------------------------------------------
    block = _build_logical_block(
        "variable_name",
        kwargs.get("variable_name"),
        kwargs.get("variable_name_and_or", "and"),
        lambda v: f"parameter:{v}"
    )

    if block:
        parts.append(block)


    # -----------------------------------------------
    # final query string
    # -----------------------------------------------
    q = " ".join(parts).strip()

    if not q and not bbox:
        raise ValueError(
        "At least one valid (non-null) search parameter or geographic bound must be provided to build a query." 
        "To view available parameters and usage examples, run: help(PangaeaDataset.search_studies)"
        )



    # ---------------------------------------------------
    # LIMIT / OFFSET
    # ---------------------------------------------------
    limit = kwargs.get("limit", 100)
    if limit > 500:
        log.warning("Limit exceeds maximum allowed (500). Using 500.")
        limit = 500
    else:
        log.info(f"Limit set to {limit}")
    offset = kwargs.get("skip", 0)

    return {
        "q": q,
        "bbox": bbox,
        "limit": limit,
        "offset": offset,
    }
