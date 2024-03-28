"""Functions for some sweet FP jank."""
# pylint: disable=invalid-name
from __future__ import annotations
from functools import WRAPPER_ASSIGNMENTS, partial, singledispatch, update_wrapper
from importlib import import_module
from itertools import filterfalse, repeat
from operator import attrgetter
from collections.abc import Callable as ABCCallable
from reprlib import recursive_repr
from typing import (
    Any,
    Callable,
    Generator,
    Generic,
    Iterable,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from compose import compose
from typing_extensions import Concatenate, ParamSpec

from .members import non_dunder_dir


import errno
import os
import signal
import functools


class TimeoutError(Exception):
    pass


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wrapper

    return decorator


R = TypeVar("R")
P = ParamSpec("P")
T = TypeVar("T")
Ty = Union[Type[T], Tuple[Type, ...]]

_sentinel_ = object()


def get_logger():
    """Get the logger."""
    return import_module(".logging", __name__).logger


class right_partial(partial, Generic[R]):
    """Partially right-applied functions."""

    __slots__ = ("right_args",)

    right_args: Tuple[Any, ...]

    def __new__(cls, func, /, *right_args: Any, **keywords):
        """Create a right_partial via handoff to partial.__new__.

        If `func` is itself a partial, make sure its stored args are right-applied their
        args.
        """
        left_args = ()
        if hasattr(func, "func"):
            if isinstance(func, cls):
                right_args = right_args + func.right_args
            left_args = func.args
            keywords = {**func.keywords, **keywords}
            func = func.func

        self = super().__new__(cls, func, *left_args, **keywords)

        self.right_args = right_args
        return self

    def __call__(self, /, *args, **keywords):
        """Call `self.func` with the stored args, right_args arounf the passed ones."""
        keywords = {**self.keywords, **keywords}
        return self.func(*self.args, *args, *self.right_args, **keywords)

    @recursive_repr()
    def __repr__(self):
        """Return a repr like `partial`'s repr with "[args], ..., [right_args]"."""
        qualname = type(self).__qualname__
        args = [repr(self.func)]
        args.extend(repr(x) for x in self.args)
        args.append("...")
        args.extend(repr(x) for x in self.right_args)
        args.extend(f"{k}={v!r}" for (k, v) in self.keywords.items())
        if type(self).__module__ == "functools":
            return f"functools.{qualname}({', '.join(args)})"
        return f"{qualname}({', '.join(args)})"

    def __reduce__(self):
        """Pickle the right_partial."""
        return (
            type(self),
            (self.func,),
            (
                self.right_args,
                self.func,
                self.args,
                self.keywords or None,
                self.__dict__ or None,
            ),
        )

    def __setstate__(self, state):
        """Unickle the right_partial."""
        print(f"{self} __setstate__ called with {state}")
        right_args, *state = state
        print(f"calling super.__setstate__ with {state}")
        super().__setstate__(state)  # type: ignore
        right_args = tuple(right_args)
        self.right_args = right_args


def resolve_attr(attr: str, obj: object, *, default: Any = _sentinel_) -> Any:
    """Alternative to getattr with path resolution, orded for use w/ partial."""
    attriter = iter(attr.split("."))
    while True:
        try:
            obj = getattr(obj, next(attriter))
        except AttributeError:
            if default is _sentinel_:
                raise
            return default
        except StopIteration:
            return obj


def identity_unpacked(*args, **_):
    """Return the input arguments unaltered, but unpack first arg when alone."""
    if _:
        get_logger().warn("Keyword arguments are ignored by `identity_unpacked`!")
    if not args:
        return None
    first, *rest = args
    if rest:
        return first, *rest
    return first


def apply(func: Callable[..., T], *args: Any, **kwargs) -> T:
    """Apply `func` to args formed by prepending `args[:-1]` to `args[-1]`.

    Like `apply` in clojure. Sort of the dual of `identity_unpacked`.

    Ex:
    --------
        apply(compose(sum, identity_unpacked), 1, 2, 3, range(4, 9)) == sum(range(1, 9))
        apply(list)
    """
    if not args:
        # let apply invoke a function without args if none were passed.
        return func(**kwargs)
    *head, tail = args
    if isinstance(tail, (str, Mapping)) or not isinstance(tail, Iterable):
        tail = (tail,)
    return func(*head, *tail, **kwargs)


def safe_attrgetter(*attrs, default: Any = None) -> Callable[[Any], Tuple[Any, ...]]:
    """Alternative to `operator.attrgetter` with defaults."""
    if default is _sentinel_:
        return attrgetter(*attrs)
    if not all(map(right_partial(isinstance, str), attrs)):
        raise TypeError(f"All keys passed to 'map_by' must be strings! ({attrs!r})")
    return compose(
        partial(apply, identity_unpacked),
        tuple,
        partial(map, right_partial(resolve_attr, default=default), attrs),
        partial(repeat, times=len(attrs)),
    )


def constant(val: T) -> Callable[..., T]:
    """Return a function that constantly returns val regardless of input."""

    def _inner(*_: Any, **__: Any) -> T:
        return val

    return _inner


class juxt(Generic[P, R]):  # pylint: disable=invalid-name
    """Callable that lazily forwards parameters to the provided list of callables."""

    def __init__(self, *funs: Callable[P, R]):
        """Return a function that lazily forwards calls to functions in `funs`.

        Like `juxt` in clojure.
        """
        self.funs = funs

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Generator[R, None, None]:
        """Return a generator over calls to the stored functions."""
        return (fun(*args, **kwargs) for fun in self.funs)

    @recursive_repr()
    def __repr__(self):
        """Return a repr like `partial`'s repr with "[args], ..., [right_args]"."""
        qualname = type(self).__qualname__
        args = (repr(fun) for fun in self.funs)
        if type(self).__module__ == "functools":
            return f"functools.{qualname}({', '.join(args)})"
        return f"{qualname}({', '.join(args)})"


def if_(check: bool, if_true: Any, if_false: Any):
    """Return if_true if check else if_false."""
    return if_true if check else if_false


def allow_dispatch_on_empty_args(func: Callable):
    """Wrap the the function so it gets called with `None` if no args were passed.

    This is mainly for appeasing singledispatch.
    """

    def permissive_wrapper(*args, **kwargs):
        if not args:
            args = (None,)
        return func(*args, **kwargs)

    update_wrapper(
        permissive_wrapper,
        func,
        assigned=(*WRAPPER_ASSIGNMENTS, *non_dunder_dir(func)),
    )

    return permissive_wrapper


@allow_dispatch_on_empty_args
@singledispatch
def get_mapper_for(*args, **kwargs) -> Callable[..., Any]:
    """Get a mapper for map_by.

    If not registered, just return identity function.
    """
    get_logger().warn(
        f"Don't know what to do with {args[0]}!", args=args, kwargs=kwargs
    )
    return identity_unpacked


get_mapper_for.register(ABCCallable, right_partial)
get_mapper_for.register(str, safe_attrgetter)
get_mapper_for.register(type(None), lambda *_, **__: identity_unpacked)


def map_by_attr(
    itr: Iterable[T],
    *attrs: str,
    **kwargs,
) -> Union[map[T], map[Any]]:
    """Map over `itr` with `safe_attrgetter` (or `identity_unpacked`)."""
    return map(get_mapper_for(*attrs, default=kwargs.get("default")), itr)


def map_by_function(
    itr: Iterable[T],
    func: Callable[Concatenate[T, P], R],
    *args: P.args,
    **kwargs: P.kwargs,
) -> Iterable[R]:
    """Map over `itr` with right-partially-applied`func`."""
    return map(right_partial(func, *args, **kwargs), itr)


def map_by(itr: Iterable, *args, **kwargs):
    """Map over `itr` with `key`."""
    if args and callable(args[0]):
        return map_by_function(itr, *args, **kwargs)
    return map_by_attr(itr, *args, **kwargs)


class partialfilter(right_partial[R], Generic[P, R]):
    """Partially applied filters."""

    def __new__(
        cls,
        func: Optional[Callable[Concatenate[Any, P], Any]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> partialfilter[P, R]:
        """Pass through to super, with placeholder if `func` is `None`."""
        return super().__new__(cls, bool if func is None else func, *args, **kwargs)

    def __call__(
        self, /, itr: Iterable[R], *args: P.args, **kwargs: P.kwargs
    ) -> filter[R]:
        """Return a filter over `itr`, yielding true items from `self.func`.

        If `self.func` is partial_filter._filter_none_, simply return filter(None, itr).
        """
        return filter(
            right_partial(super(right_partial, self).__call__, *args, **kwargs), itr
        )


class partialfilterfalse(partialfilter[P, R], Generic[P, R]):
    """Partially applied filterfalses."""

    def __call__(
        self, itr: Iterable[R], *args: P.args, **kwargs: P.kwargs
    ) -> filterfalse[R]:
        """Return a filterfalse over `itr`, yielding false items from `self.func`.

        If `self.func` is `partial_filter_false._filter_none_`, simply return
        `filterfalse(None, itr)`.
        """
        return filterfalse(
            right_partial(super(right_partial, self).__call__, *args, **kwargs), itr
        )


def instancesof(types: Ty[T], itr: Iterable[Any]) -> filter[T]:
    """Filter items from `itr` that are instances of `types`."""
    return filter(right_partial(isinstance, types), itr)


def notinstancesof(types: Ty[T], itr: Iterable[Any]) -> filterfalse[Any]:
    """Filter items from `itr` that are not instances of `types`."""
    return filterfalse(right_partial(isinstance, types), itr)


def filter_empty(itr: Iterable[T], include_empty: bool = False) -> Iterable[T]:
    """Filter empty values from `itr` unless `include_empty=True`."""
    if include_empty:
        return itr
    return filter(None, itr)


def compose_filters(
    *filters: Callable[[Any], Any], include_empty: bool = True
) -> Callable[[Iterable[T]], filter[T]]:
    """Compose `(filter_empty, *filters)` into a filter-returning function."""
    return compose(
        *map(partialfilter, (constant(True) if include_empty else None, *filters))
    )


def apply_filters(
    itr: Iterable[T], *filters: Callable[[Any], Any], include_empty: bool = True
) -> filter[T]:
    """Apply `(filter_empty, *filters)` to itr."""
    return compose_filters(*filters, include_empty=include_empty)(itr)
