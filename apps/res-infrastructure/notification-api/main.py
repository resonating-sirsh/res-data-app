from fastapi import FastAPI

from routes import router

from res.utils.fastapi.commons import add_commons

SERVER_PORT = 5000

app = FastAPI(
    title="Notifaction API",
    version="0.0.1",
    openapi_url="/notification-api/openapi.json",
    docs_url="/notification-api/docs",
)

add_commons(app)
app.include_router(router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=SERVER_PORT, reload=True)
