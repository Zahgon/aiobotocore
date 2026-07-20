from contextlib import asynccontextmanager
from copy import deepcopy
from functools import wraps

from botocore.context import (
    ClientContext,
    get_context,
    reset_context,
    set_context,
)

from ._helpers import resolve_awaitable


@asynccontextmanager
async def start_as_current_context(ctx=None):
    current = ctx or get_context()
    if current is None:
        new = ClientContext()
    else:
        new = deepcopy(current)
    token = set_context(new)
    try:
        yield
    finally:
        reset_context(token)


def with_current_context(hook=None):
    def decorator(func):
        pass

    return decorator
