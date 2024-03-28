from fastapi.testclient import TestClient
from httpx import Response

# import pytest


# @pytest.mark.skip(reason="CI failing")
def test_get():
    from main import app

    with TestClient(app) as client:
        result: Response = client.get(f"/healthcheck")
        assert result.status_code == 200

        response = result.json()
        assert response["status"] == "ok"
