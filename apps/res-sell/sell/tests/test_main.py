from fastapi.testclient import TestClient


def test_can_import_app():
    from main import app

    test_client = TestClient(app)

    response = test_client.get("/healthcheck")

    assert response.status_code == 200
