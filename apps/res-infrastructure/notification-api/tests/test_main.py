from fastapi.testclient import TestClient


def test_main_app_up():
    from main import app

    client = TestClient(app)
    response = client.get("/healthcheck")
    assert response.status_code == 200
    assert response.json() == "OK"
