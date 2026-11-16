"""Unmounted offline ASGI observation. No authenticity or execution assertion.

Trusted local callable only. Cancellation/deadline remain caller-owned.
Never persist exception objects, scope, headers or traceback locals.
"""
import asyncio
from dataclasses import dataclass
import hashlib

class ObservationError(ValueError):
    pass

@dataclass(frozen=True)
class LocalObservation:
    body: bytes
    body_sha256: str
    byte_length: int
    event_count: int

async def observe(app, scope, receive):
    owner = asyncio.current_task()
    chunks = []
    count = 0
    total = 0
    started = False
    terminal = False
    failed = False

    def reject():
        nonlocal failed
        failed = True
        raise ObservationError('observation_failed')

    async def send(message):
        nonlocal count, total, started, terminal
        if failed or asyncio.current_task() is not owner:
            reject()
        count += 1
        if count > 1024 or terminal or type(message) is not dict:
            reject()
        if len(message) > 3:
            reject()
        if any(type(k) is not str for k in message):
            reject()
        kind = message.get('type')
        if type(kind) is not str:
            reject()
        if kind == 'http.response.start':
            if started or set(message) != {'type', 'status', 'headers'}:
                reject()
            if type(message['status']) is not int or message['status'] != 200:
                reject()
            headers = message['headers']
            if type(headers) is not list or len(headers) > 32:
                reject()
            size = 0
            for pair in headers:
                if type(pair) not in (list, tuple) or len(pair) != 2:
                    reject()
                name, value = pair
                if type(name) is not bytes or type(value) is not bytes:
                    reject()
                if len(name) > 128 or len(value) > 4096:
                    reject()
                size += len(name) + len(value)
                if size > 8192:
                    reject()
            started = True
        elif kind == 'http.response.body':
            if not started or set(message) not in ({'type', 'body'}, {'type', 'body', 'more_body'}):
                reject()
            data = message['body']
            more = message.get('more_body', False)
            if type(data) is not bytes or type(more) is not bool:
                reject()
            if len(data) > 8 * 1024 * 1024 or total + len(data) > 8 * 1024 * 1024:
                reject()
            total += len(data)
            chunks.append(data)
            terminal = not more
        else:
            reject()

    try:
        await app(scope, receive, send)
        if owner.cancelling():
            raise asyncio.CancelledError()
        if failed or not terminal:
            reject()
        data = b''.join(chunks)
        digest = hashlib.sha256(b'opencompute/local-response/v1\0' + data).hexdigest()
        return LocalObservation(data, digest, total, count)
    except Exception:
        raise ObservationError('observation_failed') from None
    finally:
        chunks.clear()
