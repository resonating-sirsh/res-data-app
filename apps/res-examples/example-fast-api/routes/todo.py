"""
This is an example of a FastAPI route that uses the Cognito authentication
and function based views.
"""
from typing import List

from fastapi import APIRouter, Depends, Security
from pydantic import BaseModel

from res.utils.fastapi.authentication import auth_required


class Todo(BaseModel):
    id: int
    name: str
    description: str | None
    done: bool = False


router = APIRouter(
    tags=["example", "function-based views"],
)

todos: List[Todo] = []
next_id = 0


class CreateTodoRequest(BaseModel):
    name: str
    description: str | None


class TodosResponse(BaseModel):
    todos: List[Todo]


@router.get(
    "/",
    status_code=200,
    response_model=TodosResponse,
)
def get_todos():
    return TodosResponse(todos=todos)


@router.post(
    "/",
    status_code=201,
    response_model=TodosResponse,
    dependencies=[Depends(auth_required)],
)
def add_todo(
    request: CreateTodoRequest,
):
    global next_id
    todos.append(
        Todo(
            id=next_id,
            name=request.name,
            description=request.description,
        )
    )
    next_id += 1
    return TodosResponse(todos=todos)


@router.delete(
    "/{id}",
    status_code=200,
    response_model=TodosResponse,
)
def delete_todo_by_id(
    id: int,
):
    global todos
    todos = [todo for todo in todos if todo.id != id]
    return TodosResponse(todos=todos)


class UpdateTodoRequest(BaseModel):
    name: str | None
    description: str | None
    done: bool


@router.put(
    "/{id}",
    status_code=200,
    response_model=TodosResponse,
)
def update_todo_by_id(id: int, request: UpdateTodoRequest):
    global todos
    for todo in todos:
        if todo.id == id:
            if request.name is not None:
                todo.name = request.name
            if request.description is not None:
                todo.description = request.description
            todo.done = request.done
    return TodosResponse(todos=todos)
