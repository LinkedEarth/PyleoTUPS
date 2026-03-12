import os
import gzip

BASE_DIR = os.path.join(
    os.path.dirname(__file__),
    "..",
    "data",
    "pangaea"
)


def _fixture_path(study_id: int, suffix: str) -> str:
    """
    Construct absolute path to a fixture file.

    Parameters
    ----------
    study_id : int
        Numeric PANGAEA StudyID.
    suffix : str
        Either "_metadata.xml" or "_data.tsv".

    Returns
    -------
    str
        Absolute path to fixture file.
    """
    filename = f"{study_id}{suffix}"
    return os.path.join(BASE_DIR, filename)


def get_mock_metadata(study_id: int) -> str:
    """
    Load metadata XML fixture for a study.

    Parameters
    ----------
    study_id : int

    Returns
    -------
    str
        Raw XML text.
    """
    path = _fixture_path(study_id, "_metadata.xml")

    if not os.path.exists(path):
        raise FileNotFoundError(f"Metadata fixture not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def get_mock_data(study_id: int) -> str:
    """
    Load TSV data fixture for a study.

    Supports plain .tsv and .tsv.gz.

    Parameters
    ----------
    study_id : int

    Returns
    -------
    str
        Raw TSV text.
    """
    path = _fixture_path(study_id, "_data.tsv")

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    # Support compressed TSV
    gz_path = path + ".gz"
    if os.path.exists(gz_path):
        with gzip.open(gz_path, "rt", encoding="utf-8") as f:
            return f.read()

    raise FileNotFoundError(f"Data fixture not found: {path}")