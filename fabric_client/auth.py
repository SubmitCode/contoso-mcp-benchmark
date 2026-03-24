import os
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

load_dotenv()

AUTHORITY_TEMPLATE = "https://login.microsoftonline.com/{tenant_id}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]


def get_access_token() -> str:
    authority = AUTHORITY_TEMPLATE.format(tenant_id=os.environ.get("FABRIC_TENANT_ID", ""))
    app = ConfidentialClientApplication(
        client_id=os.environ.get("FABRIC_CLIENT_ID", ""),
        client_credential=os.environ.get("FABRIC_CLIENT_SECRET", ""),
        authority=authority,
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"Failed to acquire token: {result.get('error_description')}")
    return result["access_token"]
