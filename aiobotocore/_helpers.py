import inspect


async def resolve_awaitable(obj):
    if inspect.isawaitable(obj):
        return await obj

    return obj


async def async_any(items):
    pass
