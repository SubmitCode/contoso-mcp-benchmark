from unittest.mock import patch, MagicMock
from fabric_client.auth import get_access_token

def test_get_access_token_returns_string():
    mock_result = {"access_token": "fake-token-123"}
    with patch("fabric_client.auth.ConfidentialClientApplication") as MockApp:
        MockApp.return_value.acquire_token_for_client.return_value = mock_result
        token = get_access_token()
    assert token == "fake-token-123"

def test_get_access_token_raises_on_failure():
    mock_result = {"error": "invalid_client", "error_description": "bad creds"}
    with patch("fabric_client.auth.ConfidentialClientApplication") as MockApp:
        MockApp.return_value.acquire_token_for_client.return_value = mock_result
        import pytest
        with pytest.raises(RuntimeError, match="Failed to acquire token"):
            get_access_token()

import httpx
from unittest.mock import AsyncMock
from fabric_client.dax import execute_dax

def test_execute_dax_returns_rows():
    mock_response = {
        "results": [{"tables": [{"rows": [{"[Revenue]": 1000}, {"[Revenue]": 2000}]}]}]
    }
    with patch("fabric_client.dax.get_access_token", return_value="fake-token"):
        with patch("fabric_client.dax.httpx.post") as mock_post:
            mock_post.return_value = MagicMock(
                status_code=200,
                json=lambda: mock_response,
                raise_for_status=lambda: None,
            )
            rows = execute_dax("EVALUATE Sales")
    assert len(rows) == 2
    assert rows[0]["[Revenue]"] == 1000

def test_execute_dax_raises_on_http_error():
    import pytest
    with patch("fabric_client.dax.get_access_token", return_value="fake-token"):
        with patch("fabric_client.dax.httpx.post") as mock_post:
            mock_post.return_value = MagicMock(
                status_code=400,
                raise_for_status=MagicMock(side_effect=httpx.HTTPStatusError(
                    "bad request", request=MagicMock(), response=MagicMock(status_code=400)
                )),
            )
            with pytest.raises(httpx.HTTPStatusError):
                execute_dax("EVALUATE Sales")
