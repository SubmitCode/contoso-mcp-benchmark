import os
import pytest
import httpx
from unittest.mock import patch, MagicMock
from fabric_client.auth import get_access_token
from fabric_client.dax import execute_dax


def test_get_access_token_returns_string():
    mock_result = {"access_token": "fake-token-123"}
    with patch.dict("os.environ", {"FABRIC_TENANT_ID": "t", "FABRIC_CLIENT_ID": "c", "FABRIC_CLIENT_SECRET": "s"}):
        with patch("fabric_client.auth.ConfidentialClientApplication") as MockApp:
            MockApp.return_value.acquire_token_for_client.return_value = mock_result
            # Clear the app cache so our mock is used
            import fabric_client.auth as auth_mod
            auth_mod._app_cache.clear()
            token = get_access_token()
    assert token == "fake-token-123"


def test_get_access_token_raises_on_failure():
    mock_result = {"error": "invalid_client", "error_description": "bad creds"}
    with patch.dict("os.environ", {"FABRIC_TENANT_ID": "t", "FABRIC_CLIENT_ID": "c", "FABRIC_CLIENT_SECRET": "s"}):
        with patch("fabric_client.auth.ConfidentialClientApplication") as MockApp:
            MockApp.return_value.acquire_token_for_client.return_value = mock_result
            import fabric_client.auth as auth_mod
            auth_mod._app_cache.clear()
            with pytest.raises(RuntimeError, match="Failed to acquire token"):
                get_access_token()


def test_execute_dax_returns_rows():
    mock_response = {
        "results": [{"tables": [{"rows": [{"[Revenue]": 1000}, {"[Revenue]": 2000}]}]}]
    }
    with patch.dict("os.environ", {"FABRIC_DATASET_ID": "ds-123"}):
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
    with patch.dict("os.environ", {"FABRIC_DATASET_ID": "ds-123"}):
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


def test_execute_dax_raises_on_missing_env_var():
    env = {k: v for k, v in os.environ.items() if k != "FABRIC_DATASET_ID"}
    with patch.dict("os.environ", env, clear=True):
        with pytest.raises(ValueError, match="FABRIC_DATASET_ID"):
            execute_dax("EVALUATE Sales")
