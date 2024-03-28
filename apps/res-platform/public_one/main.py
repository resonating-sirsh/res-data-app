""" nudge """

from __future__ import annotations
import traceback
from typing import List, Optional, Union
from typing import Sequence
from warnings import filterwarnings
from fastapi import APIRouter, Depends, FastAPI, HTTPException, status
from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import res
from res.utils import secrets_client
from source.routes import PROCESS_NAME, set_meta_one_routes

filterwarnings("ignore")


def verify_api_key(key: str):
    RES_META_ONE_API_KEY = secrets_client.get_secret("RES_PUBLIC_TEST_META_ONE_API_KEY")

    if key == RES_META_ONE_API_KEY:
        return True
    else:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized. Invalid API KEY",
        )


DESCRIPTION = """
**Resonance API to query the designing, selling and making of your styles**

--- 
- Use the `Design` endpoints to check on styles that have been onboarded on the platform. 


- Use the `Sell` endpoints to check on customer orders.


- Use the `Make` endpoints to check on the status of _Make_ production requests (manufacturing).
---
"""
app = FastAPI(
    title="ONE API",
    description=DESCRIPTION,
    openapi_url=f"/{PROCESS_NAME}/openapi.json",
    docs_url=f"/{PROCESS_NAME}/docs",
    redoc_url="/{PROCESS_NAME}/documentation",
)
security = HTTPBearer()
api_router = APIRouter()


def get_current_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    if not verify_api_key(token):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized. Invalid API KEY",
        )
    return token


@app.get("/")
@app.get("/healthcheck", include_in_schema=False)
async def healthcheck():
    return {"status": "ok"}


origins = [
    "http://localhost:3000",
    "http://localhost:3001",
    "http://localhost:5000",
    "https://strictly-mutual-newt.ngrok-free.app",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
set_meta_one_routes(app)

if __name__ == "__main__":
    # Use this for debugging purposes only
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=5001, log_level="debug", reload=True)
