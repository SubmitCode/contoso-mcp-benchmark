import pytest
import httpx
from unittest.mock import patch, MagicMock
from fabric_client.dax import execute_dax


def test_execute_dax_returns_rows():
    mock_response = {
        "results": [{"tables": [{"rows": [{"Sales[Quantity]": 1000}, {"Sales[Quantity]": 2000}]}]}]
    }
    with patch.dict("os.environ", {"FABRIC_WORKSPACE_ID": "ws-1", "FABRIC_DATASET_ID": "ds-1"}):
        with patch("fabric_client.dax.get_access_token", return_value="fake-token"):
            with patch("fabric_client.dax.httpx.post") as mock_post:
                mock_post.return_value = MagicMock(
                    status_code=200,
                    json=lambda: mock_response,
                    raise_for_status=lambda: None,
                )
                rows = execute_dax("EVALUATE Sales")
    assert len(rows) == 2
    # Table prefix should be stripped
    assert rows[0]["Quantity"] == 1000


def test_execute_dax_strips_measure_prefix():
    mock_response = {
        "results": [{"tables": [{"rows": [{"[Net Sales]": 12345.67}]}]}]
    }
    with patch.dict("os.environ", {"FABRIC_WORKSPACE_ID": "ws-1", "FABRIC_DATASET_ID": "ds-1"}):
        with patch("fabric_client.dax.get_access_token", return_value="fake-token"):
            with patch("fabric_client.dax.httpx.post") as mock_post:
                mock_post.return_value = MagicMock(
                    status_code=200,
                    json=lambda: mock_response,
                    raise_for_status=lambda: None,
                )
                rows = execute_dax("EVALUATE ROW(\"Net Sales\", [Net Sales])")
    assert rows[0]["Net Sales"] == 12345.67


def test_execute_dax_raises_on_http_error():
    with patch.dict("os.environ", {"FABRIC_WORKSPACE_ID": "ws-1", "FABRIC_DATASET_ID": "ds-1"}):
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


def test_execute_dax_raises_on_missing_workspace_id():
    import os
    env = {k: v for k, v in os.environ.items() if k not in ("FABRIC_WORKSPACE_ID", "FABRIC_DATASET_ID")}
    with patch.dict("os.environ", env, clear=True):
        with pytest.raises(ValueError, match="FABRIC_WORKSPACE_ID"):
            execute_dax("EVALUATE Sales")


def test_execute_dax_raises_on_missing_dataset_id():
    import os
    env = {k: v for k, v in os.environ.items() if k != "FABRIC_DATASET_ID"}
    env["FABRIC_WORKSPACE_ID"] = "ws-1"
    with patch.dict("os.environ", env, clear=True):
        with pytest.raises(ValueError, match="FABRIC_DATASET_ID"):
            execute_dax("EVALUATE Sales")
