import os
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()

SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]  # Power BI API access scope

_app_cache: dict = {}


def _require_env(key: str) -> str:
    """Read a required environment variable or raise ValueError."""
    value = os.environ.get(key)
    if not value:
        raise ValueError(f"Required environment variable '{key}' is not set. Check your .env file.")
    return value


def get_access_token() -> str:
    tenant_id = _require_env("FABRIC_TENANT_ID")
    client_id = _require_env("FABRIC_CLIENT_ID")
    client_secret = _require_env("FABRIC_CLIENT_SECRET")

    cache_key = (tenant_id, client_id)
    if cache_key not in _app_cache:
        _app_cache[cache_key] = ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
        )
    app = _app_cache[cache_key]

    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"Failed to acquire token: {result.get('error_description')}")
    return result["access_token"]
