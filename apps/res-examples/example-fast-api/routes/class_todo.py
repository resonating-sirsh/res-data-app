"""
This is an example of a class-based view using FastAPI.

WIP: This module is not yet complete

We have an issue with Pydantic version in order to this work
we need to migrate from pydantic v1 to v2 which inforce of to migrate
the fastapi version too.
"""

from uuid import UUID, uuid4
from typing import Optional
from fastapi import APIRouter
from fastapi.exceptions import HTTPException
from fastapi_utils.api_model import APIModel
from fastapi_utils.cbv import cbv
from sqlmodel import Session, Field, SQLModel, create_engine, select

router = APIRouter(
    tags=["class-based views", "example"],
)


class Todo(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    title: str = Field(description="The title of the todo")
    description: Optional[str] = Field(
        default=None,
        description="A description of the todo",
        nullable=True,
    )
    done: bool = False


engine = create_engine("sqlite:///./test.db", echo=True)


def on_startup():
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)


class CreateTodoRequest(APIModel):
    title: str
    description: Optional[str] = None


class UpdateTodoRequest(APIModel):
    title: Optional[str] = None
    description: Optional[str] = None
    done: Optional[bool] = None


@cbv(router)
class TodoView:
    @router.get("/")
    def list(self):
        engine = create_engine("sqlite:///./test.db", echo=True)
        with Session(engine) as session:
            statement = select(Todo).limit(10)
            todos = session.exec(statement).all()
            return todos

    @router.post("/")
    def create(self, todo: CreateTodoRequest):
        engine = create_engine("sqlite:///./test.db", echo=True)
        with Session(engine) as session:
            new_todo = Todo(**todo.dict())
            session.add(new_todo)
            session.commit()
            todos = session.exec(select(Todo)).all()
            return todos

    @router.get("/{id}")
    def get(self, id: UUID) -> Todo:
        engine = create_engine("sqlite:///./test.db", echo=True)
        with Session(engine) as session:
            statement = select(Todo).where(Todo.id == id)
            todo = session.exec(statement).first()
            if not todo:
                raise HTTPException(status_code=404, detail="Todo not found")
            return todo

    @router.put("/{id}")
    def update(self, id: int, input: UpdateTodoRequest):
        engine = create_engine("sqlite:///./test.db", echo=True)
        with Session(engine) as session:
            statement = select(Todo).where(Todo.id == id)
            todo = session.exec(statement).first()
            if not todo:
                raise HTTPException(status_code=404, detail="Todo not found")
            for field, value in input.dict(exclude_unset=True).items():
                setattr(todo, field, value)
            session.add(todo)
            session.commit()
            todos = session.exec(select(Todo)).all()
            return todos
