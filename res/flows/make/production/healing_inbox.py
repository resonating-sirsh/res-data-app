"""
inherit from inbox just to create a healing module
"""

from .inbox import healing_handler


def handler(event, context=None):
    return healing_handler(event, context)


def generator(event, context=None):
    return {}
