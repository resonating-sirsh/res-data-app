from fastapi import FastAPI
from res.utils.fastapi.commons import add_commons
from source.routes import add_routes

SERVER_PORT = 5000

app = FastAPI(
    title="Sell API",
    version="0.0.1",
    openapi_url="/sell/openapi.json",
    docs_url="/sell/docs",
)

add_commons(app)
add_routes(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=SERVER_PORT, reload=True)
