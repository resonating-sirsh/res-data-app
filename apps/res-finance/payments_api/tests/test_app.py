# from fastapi.testclient import TestClient
# from httpx import Response
# import pytest
# from main import app

# from . import sql_queries
# from schemas.pydantic.payments import *

# import re
# import pytest


# def test_get():
#     with TestClient(app) as client:
#         result: Response = client.get(f"/healthcheck")
#         assert result.status_code == 200

#         response = result.json()
#         assert response["status"] == "ok"
