import subprocess


def get_access_token() -> str:
    """Get Power BI API access token via Azure CLI (`az login` required)."""
    result = subprocess.run(
        [
            "az", "account", "get-access-token",
            "--resource", "https://analysis.windows.net/powerbi/api",
            "--query", "accessToken",
            "-o", "tsv",
        ],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()
