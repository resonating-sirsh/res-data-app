"""Asyncio helper methods."""
from __future__ import annotations
from asyncio import Task, create_task, get_event_loop, new_event_loop, set_event_loop
from functools import partial
from typing import Callable, Coroutine, TypeVar
import warnings
from typing_extensions import ParamSpec
from compose import acompose

_BACKGROUND_TASKS_ = set()

P = ParamSpec("P")
R = TypeVar("R", bound=Coroutine)


def apply_next(
    name: str, func: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs
) -> Task[R]:
    """Create a Task with the coroutine and hold it strongly until done."""
    task = create_task(func(*args, **kwargs), name=name)

    # Add task to the set. This creates a strong reference.
    _BACKGROUND_TASKS_.add(task)

    # To prevent keeping references to finished tasks forever,
    # make each task remove its own reference from the set after
    # completion:
    task.add_done_callback(_BACKGROUND_TASKS_.discard)
    return task


def acompose_next(*funs):
    """Return an acomposition that returns a Task when called."""
    acomposition = acompose(*funs)
    return partial(apply_next, repr(acomposition), acomposition)


def get_or_set_event_loop():
    """Get or set event loop if new thread."""
    # Get the current asyncio event loop
    # Or create a new event loop if there isn't one (in a new Thread)
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="There is no current event loop")
            loop = get_event_loop()
    except RuntimeError:
        loop = new_event_loop()
        set_event_loop(loop)
    return loop
