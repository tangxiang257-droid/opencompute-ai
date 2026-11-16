"""Isolated startup ownership prototype; not wired to host or authorization.

Trusted same-loop private factory, no deadline or permission decisions.
Borrowed dependencies stay owned by their existing owners.
"""
import asyncio
import inspect

def _with_cleanup(primary, cleanup):
    """Preserve an existing cause/context graph; keep cancellation at the top."""
    if primary is cleanup:return primary
    if primary.__cause__ is None and primary.__context__ is None:
        primary.__cause__=cleanup
        return primary
    group=BaseExceptionGroup('operation and cleanup failures',[primary,cleanup])
    if isinstance(primary,asyncio.CancelledError):
        cancelled=asyncio.CancelledError('operation cancelled; internal failures retained')
        cancelled.__cause__=group
        return cancelled
    return group

class DependencyEnded(RuntimeError):
    pass

async def supervise_startup(factory, required_tasks):
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    if (not callable(factory) or type(required_tasks) is not tuple or not required_tasks
            or len({id(t) for t in required_tasks}) != len(required_tasks)
            or any(not isinstance(t, asyncio.Task) or t.get_loop() is not loop or t is current
                   for t in required_tasks)):
        raise ValueError('invalid startup ownership')
    if any(t.done() for t in required_tasks):
        raise DependencyEnded('required startup reader ended')
    coroutine = factory()
    if not inspect.iscoroutine(coroutine):
        raise ValueError('invalid startup factory')
    try:
        child = asyncio.create_task(coroutine)
    except BaseException:
        coroutine.close()
        raise
    primary = None
    value = None
    try:
        await asyncio.wait((child, *required_tasks), return_when=asyncio.FIRST_COMPLETED)
        if any(t.done() for t in required_tasks):
            raise DependencyEnded('required startup reader ended')
        value = child.result()
    except BaseException as error:
        primary = error

    async def settle():
        cancelled_by_supervisor = not child.done()
        if cancelled_by_supervisor:
            child.cancel()
        try:
            await child
        except BaseException as error:
            if (cancelled_by_supervisor and isinstance(error, asyncio.CancelledError)
                    and error.__cause__ is None and error.__context__ is None):
                return None
            return error
        return None

    settlement = asyncio.create_task(settle())
    while not settlement.done():
        try:
            await asyncio.shield(settlement)
        except asyncio.CancelledError as error:
            primary = error if primary is None else _with_cleanup(error, primary)
    cleanup = settlement.result()
    if cleanup is not None:
        primary = cleanup if primary is None else _with_cleanup(primary, cleanup)
    if primary is not None:
        raise primary
    if any(t.done() for t in required_tasks):
        raise DependencyEnded('required startup reader ended')
    return value
