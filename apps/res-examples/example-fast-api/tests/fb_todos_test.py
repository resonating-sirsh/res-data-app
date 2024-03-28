from fastapi.testclient import TestClient

from main import app
from routes.todo import Todo, todos

client = TestClient(app)

todos.append(
    Todo(
        id=1,
        name="Test",
        description="Test",
        done=False,
    )
)


def test_get_todos():
    response = client.get("/example-fast-api/fb-todos/")
    assert response.status_code == 200
    dc_response = response.json()["todos"]
    assert len(dc_response) == 1
    print(dc_response)
    assert dc_response[0]["id"] == 1
    assert dc_response[0]["name"] == "Test"
    assert dc_response[0]["description"] == "Test"
    assert dc_response[0]["done"] == False


# TODO: How do I made an request pass and fail for the required authentication routes
