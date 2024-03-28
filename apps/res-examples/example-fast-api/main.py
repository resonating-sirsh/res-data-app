from fastapi import FastAPI
from res.utils.fastapi.commons import add_commons

from routes.class_todo import router as class_router, on_startup
from routes.todo import router as function_router

SERVER_PORT = 5000

app = FastAPI(
    title="Example Fast API",
    version="0.0.1",
    openapi_url="/example-fast-api/openapi.json",
    docs_url="/example-fast-api/docs",
)

add_commons(app)

app.include_router(class_router, prefix="/example-fast-api/class_todos")
app.include_router(function_router, prefix="/example-fast-api/fb-todos")


if __name__ == "__main__":
    import uvicorn

    on_startup()
    uvicorn.run(app, host="0.0.0.0", port=SERVER_PORT)
