"""Join owned blocking work before releasing its asynchronous custody scope."""
from __future__ import annotations

import asyncio


async def wait_owned(task):
    """Wait through repeated cancellation; return whether cancellation was requested."""
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            break
    return cancelled


async def joined_thread(function, *args, **kwargs):
    """Keep the caller's scope until its worker finishes, even after cancellation."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    cancelled = await wait_owned(task)
    if cancelled:
        # Retrieve any worker error before preserving the requested cancellation.
        try:
            task.result()
        except BaseException:
            pass
        raise asyncio.CancelledError
    return task.result()
