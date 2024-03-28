from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_checkhealth():
    response = client.get("/checkhealth")
    assert response.status_code == 200
    assert response.json() == "OK"
