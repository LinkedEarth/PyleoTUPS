__all__ = ['save_pangaea_credentials', 'load_pangaea_credentials', 'remove_pangaea_credentials']

import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


ENV_FILENAME = ".env"
ENV_KEY = "PANGAEA_API"

class CredentialError(Exception):
    """Custom exception for credential-related errors."""
    pass

def save_pangaea_credentials(api_key: str, overwrite: bool = False) -> None:
    """
    Save the PANGAEA API key to a local .env file securely.

    Parameters
    ----------
    api_key : str
        Your PANGAEA login token.
    overwrite : bool, default=False
        If True, overwrite existing PANGAEA_API entry.

    Raises
    ------
    FileExistsError
        If .env exists and overwrite=False.
    ValueError
        If api_key is empty.
    """

    if not api_key or not isinstance(api_key, str):
        raise ValueError("A valid PANGAEA API key must be provided.")

    env_path = Path(ENV_FILENAME)

    # If .env exists
    if env_path.exists():
        content = env_path.read_text()

        if ENV_KEY in content and not overwrite:
            raise FileExistsError(
                f"{ENV_KEY} already exists in {ENV_FILENAME}. "
                "Use overwrite=True to replace it."
            )

        if overwrite:
            lines = []
            for line in content.splitlines():
                if not line.startswith(f"{ENV_KEY}="):
                    lines.append(line)
            lines.append(f'{ENV_KEY}="{api_key}"')
            env_path.write_text("\n".join(lines) + "\n")
        else:
            with env_path.open("a", encoding="utf-8") as f:
                f.write(f'\n{ENV_KEY}="{api_key}"\n')
    else:
        env_path.write_text(f'{ENV_KEY}="{api_key}"\n')

    # Restrict file permissions (best effort)
    try:
        os.chmod(env_path, 0o600)
    except Exception:
        pass  # Windows may ignore this

    _ensure_gitignore()

    print("PANGAEA credentials saved securely to .env")


def load_pangaea_credentials() -> Optional[str]:
    """
    Load PANGAEA API key from .env file.

    Returns
    -------
    str
        The API key if found, else None.

    Raises
    ------
    CredentialError
        If no PANGAEA_API key is found.
    """

    if load_dotenv is None:
        raise ImportError(
            "python-dotenv is required to load credentials. "
            "Install via `pip install python-dotenv`."
        )

    load_dotenv()
    key = os.getenv(ENV_KEY)

    if key is None:
        raise CredentialError(
            "No PANGAEA_API key found.\n"
            "If accessing protected datasets, please run:\n\n"
            "    save_pangaea_credentials('your_token_here')\n\n"
            "Public datasets remain accessible without authentication."
        )

    return key


def remove_pangaea_credentials() -> None:
    """
    Remove PANGAEA_API entry from .env file.
    """

    env_path = Path(ENV_FILENAME)

    if not env_path.exists():
        print(".env file not found.")
        return

    content = env_path.read_text().splitlines()
    new_lines = [line for line in content if not line.startswith(f"{ENV_KEY}=")]

    env_path.write_text("\n".join(new_lines) + "\n")

    print("PANGAEA credentials removed from .env")


def _ensure_gitignore() -> None:
    """
    Ensure .env is listed in .gitignore.
    """

    gitignore = Path(".gitignore")

    if gitignore.exists():
        content = gitignore.read_text()
        if ENV_FILENAME not in content:
            with gitignore.open("a", encoding="utf-8") as f:
                f.write(f"\n{ENV_FILENAME}\n")