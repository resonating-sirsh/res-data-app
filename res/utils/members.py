"""Extensions of python functions for object members."""
from __future__ import annotations
from typing import Any, Callable, Dict, Set
from weakref import WeakSet


def _is_dunder_(name: str) -> bool:
    return name.startswith("__") and name.endswith("__", 2)


def _is_under_(name: str) -> bool:
    return not _is_dunder_(name) and name.startswith("_") and name.endswith("_", 1)


def _is_public_(name: str) -> bool:
    return not _is_dunder_(name) and not _is_under_(name)


def dunder_dir(obj: object) -> Set[str]:
    """Produce a set of the dunder names in obj."""
    return {key for key in dir(obj) if _is_dunder_(key)}


def dunder_vars(obj: object) -> Dict[str, Any]:
    """Produce a dict of the dunder vars of obj."""
    return {key: val for key, val in vars(obj).items() if _is_dunder_(key)}


def non_dunder_dir(obj: object) -> Set[str]:
    """Produce a set of the non-dunder names in obj."""
    return {key for key in dir(obj) if not _is_dunder_(key)}


def non_dunder_vars(obj: object) -> Dict[str, Any]:
    """Produce a dict of the non-dunder vars of obj."""
    return {key: val for key, val in vars(obj).items() if not _is_dunder_(key)}


def under_dir(obj: object) -> Set[str]:
    """Produce a set of the under names in obj."""
    return {key for key in dir(obj) if _is_under_(key)}


def under_vars(obj: object) -> Dict[str, Any]:
    """Produce a dict of the under vars of obj."""
    return {key: val for key, val in vars(obj).items() if _is_under_(key)}


def public_dir(obj: object) -> Set[str]:
    """Produce a set of the public names in obj."""
    return {key for key in dir(obj) if _is_public_(key)}


def public_vars(obj: object) -> Dict[str, Any]:
    """Produce a dict of the public vars of obj."""
    return {key: val for key, val in vars(obj).items() if _is_public_(key)}


class SetAndForget(property):
    """Descriptor class for descriptors that can only be set once."""

    def __init__(
        self,
        fget: Callable[[Any], Any] | None = ...,
        fset: Callable[[Any, Any], None] | None = ...,
        fdel: Callable[[Any], None] | None = ...,
        doc: str | None = ...,
    ) -> None:
        """Initialize a property with an extra `_set_instances_` WeakSet."""
        super().__init__(fget, fset, fdel, doc)
        self._set_instances_ = WeakSet()

    def __set__(self, inst, value):
        """Call the setter if not `_set_instances_`."""
        if inst in self._set_instances_:
            return
        result = super().__set__(inst, value)
        self._set_instances_.add(inst)
        return result

    def __delete__(self, inst):
        """Remove inst from _set_instances_."""
        result = super().__delete__(inst)
        if inst in self._set_instances_:
            self._set_instances_.discard(inst)
        return result

    def settable(self, inst):
        """Return True if property is settable for the passed instance."""
        return self.fset is not None and inst not in self._set_instances_
