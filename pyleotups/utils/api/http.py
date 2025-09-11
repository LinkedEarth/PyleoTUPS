import time
import requests
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[%(asctime)s][%(levelname)s] - %(message)s')

def get(url: str, params: dict, *, retries: int = 2, backoff: float = 0.8) -> requests.Response:
    """Return the raw Response so callers can handle 204 before attempting .json()."""
    last_exc = None
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params)
            print(f"Request URL: {resp.url}")
            return resp
        except Exception as e:
            last_exc = e
            if attempt < retries:
                time.sleep(backoff * (2 ** attempt))
    raise requests.HTTPError(f"HTTP request failed after {retries+1} attempts: {last_exc}")