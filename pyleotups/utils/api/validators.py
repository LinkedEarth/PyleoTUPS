# pyleotups/utils/api/validators.py
from __future__ import annotations
from typing import Iterable, Tuple, Callable, List, Optional
from .constants import TIME_FORMATS, TIME_METHODS

class ValidationError(ValueError):
    """Raised when a parameter fails validation."""

def to_YN(value) -> Optional[str]:
    """Coerce common truthy/falsy values to 'Y'/'N'. Return None for not-provided."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "Y" if value else "N"
    s = str(value).strip().lower()
    if s in {"true", "yes", "y", "1"}:
        return "Y"
    if s in {"false", "no", "n", "0"}:
        return "N"
    raise ValidationError(f"Cannot coerce {value!r} to 'Y'/'N'.")

def _coerce_multi(name: str, value, item_normalizer: Callable[[str], str]) -> Tuple[str, int]:
    """
    Accepts str or iterable[str]. Returns (joined, count) with '|' as separator.
    - If value is str, it's used as-is (trimmed); count is number of segments split by '|'.
    - If value is list/tuple/set, each item is normalized and joined with '|'.
    """
    if value is None:
        return "", 0

    if isinstance(value, str):
        joined = value.strip()
        items = [x.strip() for x in joined.split("|") if x.strip()]
        return joined, len(items)

    if isinstance(value, Iterable):
        items: List[str] = []
        for v in value:
            sv = item_normalizer(str(v))
            if sv:
                items.append(sv)
        joined = "|".join(items)
        return joined, len(items)

    raise ValidationError(f"{name} must be a string or a list/tuple/set of strings.")

def normalize_investigator(s: str) -> str:
    s = str(s).strip()
    if not s:
        raise ValidationError("Empty investigator.")
    # Keep commas and case as provided (NOAA expects 'LastName, Initials')
    return s

def normalize_species_code(s: str) -> str:
    s = str(s).strip().upper()
    if len(s) != 4 or not s.isalpha():
        raise ValidationError(f"Species code must be exactly 4 letters: {s!r}")
    return s

def normalize_passthrough(s: str) -> str:
    s = str(s).strip()
    if not s:
        raise ValidationError("Empty value.")
    # allow embedded '>' (hierarchies) and other allowed chars
    return s

def normalize_search_text(s: str) -> str:
    s = str(s).strip()
    # NOAA forbids HTML reserved characters; Oracle ops are allowed
    if any(ch in s for ch in "<>&"):
        raise ValidationError("search_text cannot contain '<', '>', '&'. Escape special chars as needed.")
    return s

def validate_and_or(s: str) -> str:
    v = str(s).strip().lower()
    if v not in {"and", "or"}:
        raise ValidationError("Value must be 'and' or 'or'.")
    return v

def validate_int(name: str, v) -> int:
    try:
        return int(v)
    except Exception:
        raise ValidationError(f"{name} must be an integer.")

def validate_int_range(name: str, v, lo: int, hi: int) -> int:
    iv = validate_int(name, v)
    if iv < lo or iv > hi:
        raise ValidationError(f"{name} must be in [{lo}, {hi}].")
    return iv

def validate_limit(v, lo: int, hi: int) -> int:
    iv = validate_int("limit", v)
    if iv < lo or iv > hi:
        raise ValidationError(f"limit must be in [{lo}, {hi}].")
    return iv

def validate_time_format(s: str) -> str:
    v = str(s).strip().upper()
    if v not in TIME_FORMATS:
        raise ValidationError(f"time_format must be one of {sorted(TIME_FORMATS)}.")
    return v

def validate_time_method(s: str) -> str:
    v = str(s).strip()
    if v not in TIME_METHODS:
        raise ValidationError(f"time_method must be one of {sorted(TIME_METHODS)}.")
    return v

def validate_digits(v) -> int:
    iv = validate_int("id", v)
    if iv < 0:
        raise ValidationError("id must be non-negative.")
    return iv
