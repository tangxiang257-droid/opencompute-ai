"""Bounded, fair request admission queue for OpenCompute AI v0.4.9.

Transport/model-agnostic scheduling primitive; not yet wired to Qwen2 Gloo
pipeline or remote Workers.
"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
import time
from typing import Any


class QueueFullError(RuntimeError):
    """The global or per-client pending-request limit has been reached."""


class DuplicateRequestError(ValueError):
    """A request ID is already pending or in flight."""


@dataclass
class Request:
    request_id: str
    client_id: str
    payload: Any
    enqueued_at: float
    deadline: float
    future: asyncio.Future = field(repr=False)
    state: str = "queued"


@dataclass(frozen=True)
class RequestTicket:
    request: Request

    async def result(self, timeout: float | None = None) -> Any:
        """Wait for completion, respecting queue expiry without cancelling work.

        A queued request's admission deadline ends when a consumer dispatches
        it. If it remains queued until that deadline, this waiter raises even
        when no consumer is currently polling the queue. Once in flight, the
        queue deadline no longer limits execution; an optional caller timeout
        still does. Shielding keeps waiter cancellation from cancelling the
        shared result Future.
        """
        if timeout is not None and (
            isinstance(timeout, bool) or not isinstance(timeout, (int, float))
            or timeout <= 0
        ):
            raise ValueError("timeout must be positive")
        caller_deadline = None if timeout is None else time.monotonic() + float(timeout)

        while True:
            if self.request.future.done():
                return self.request.future.result()

            if self.request.state != "queued":
                if caller_deadline is None:
                    return await asyncio.shield(self.request.future)
                remaining = caller_deadline - time.monotonic()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                return await asyncio.wait_for(
                    asyncio.shield(self.request.future), timeout=remaining
                )

            queue_remaining = self.request.deadline - time.monotonic()
            if queue_remaining <= 0:
                if self.request.state == "queued":
                    raise TimeoutError(
                        f"request {self.request.request_id} expired in queue"
                    )
                continue

            if caller_deadline is None:
                wait_time = queue_remaining
            else:
                caller_remaining = caller_deadline - time.monotonic()
                if caller_remaining <= 0:
                    raise asyncio.TimeoutError
                wait_time = min(queue_remaining, caller_remaining)

            try:
                return await asyncio.wait_for(
                    asyncio.shield(self.request.future), timeout=wait_time
                )
            except asyncio.TimeoutError:
                # Dispatch may have won the race with the queue deadline.
                if self.request.state != "queued":
                    continue
                if time.monotonic() >= self.request.deadline:
                    raise TimeoutError(
                        f"request {self.request.request_id} expired in queue"
                    )
                # The caller's independent wait timeout elapsed first.
                raise


class FairRequestQueue:
    """FIFO within each client, round-robin between clients.

    max_pending bounds queued work (not in-flight work). The consumer owns
    in-flight concurrency and must call complete or fail exactly once.
    Deadlines use monotonic time, avoiding wall-clock adjustments.
    """

    def __init__(self, *, max_pending: int = 128,
                 max_pending_per_client: int = 16):
        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending must be a positive integer")
        if (type(max_pending_per_client) is not int
                or max_pending_per_client < 1
                or max_pending_per_client > max_pending):
            raise ValueError("invalid per-client pending limit")
        self.max_pending = max_pending
        self.max_pending_per_client = max_pending_per_client
        self._condition = asyncio.Condition()
        self._queues: dict[str, deque[Request]] = {}
        self._rotation: deque[str] = deque()
        self._known_ids: set[str] = set()
        self._pending = 0
        self._closed = False

    @property
    def pending_count(self) -> int:
        return self._pending

    async def put(self, request_id: str, client_id: str, payload: Any,
                  *, timeout: float) -> RequestTicket:
        if not isinstance(request_id, str) or not request_id.strip():
            raise ValueError("request_id must be non-empty")
        if not isinstance(client_id, str) or not client_id.strip():
            raise ValueError("client_id must be non-empty")
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or timeout <= 0):
            raise ValueError("timeout must be positive")
        loop = asyncio.get_running_loop()
        async with self._condition:
            if self._closed:
                raise RuntimeError("request queue is closed")
            if request_id in self._known_ids:
                raise DuplicateRequestError(request_id)
            if self._pending >= self.max_pending:
                raise QueueFullError("global pending-request limit reached")
            client_queue = self._queues.get(client_id)
            if client_queue is not None and len(client_queue) >= self.max_pending_per_client:
                raise QueueFullError("per-client pending-request limit reached")
            now = time.monotonic()
            request = Request(
                request_id=request_id, client_id=client_id, payload=payload,
                enqueued_at=now, deadline=now + float(timeout),
                future=loop.create_future())
            self._known_ids.add(request_id)
            self._pending += 1
            if client_queue is None:
                self._queues[client_id] = deque([request])
                self._rotation.append(client_id)
            else:
                client_queue.append(request)
            self._condition.notify()
            return RequestTicket(request)

    def _expire_and_prune(self, now: float) -> None:
        for client_id in tuple(self._rotation):
            queue = self._queues.get(client_id)
            if queue is None:
                continue
            kept = deque()
            for request in queue:
                if request.state != "queued":
                    continue
                if request.deadline <= now:
                    request.state = "expired"
                    self._pending -= 1
                    self._known_ids.discard(request.request_id)
                    if not request.future.done():
                        request.future.set_exception(TimeoutError(
                            f"request {request.request_id} expired in queue"))
                else:
                    kept.append(request)
            if kept:
                self._queues[client_id] = kept
            else:
                self._queues.pop(client_id, None)
                try:
                    self._rotation.remove(client_id)
                except ValueError:
                    pass

    async def get(self) -> Request:
        """Return the next live request, using client round-robin fairness."""
        async with self._condition:
            while True:
                if self._closed and self._pending == 0:
                    raise RuntimeError("request queue is closed")
                self._expire_and_prune(time.monotonic())
                if self._rotation:
                    client_id = self._rotation.popleft()
                    queue = self._queues[client_id]
                    request = queue.popleft()
                    request.state = "in_flight"
                    if queue:
                        self._rotation.append(client_id)
                    else:
                        self._queues.pop(client_id, None)
                    self._pending -= 1
                    return request
                if self._closed:
                    raise RuntimeError("request queue is closed")
                deadlines = [
                    req.deadline for queue in self._queues.values()
                    for req in queue if req.state == "queued"
                ]
                if deadlines:
                    delay = max(0.001, min(deadlines) - time.monotonic())
                    try:
                        await asyncio.wait_for(self._condition.wait(), delay)
                    except asyncio.TimeoutError:
                        pass
                else:
                    await self._condition.wait()

    async def complete(self, request: Request, result: Any) -> None:
        await self._settle(request, result=result)

    async def fail(self, request: Request, error: BaseException) -> None:
        if not isinstance(error, BaseException):
            raise TypeError("error must be an exception")
        await self._settle(request, error=error)

    async def _settle(self, request: Request, *, result: Any = None,
                      error: BaseException | None = None) -> None:
        async with self._condition:
            if request.state != "in_flight":
                raise RuntimeError("request is not in flight or already settled")
            request.state = "failed" if error else "completed"
            self._known_ids.discard(request.request_id)
            if not request.future.done():
                if error is not None:
                    request.future.set_exception(error)
                else:
                    request.future.set_result(result)
            self._condition.notify_all()

    async def close(self) -> None:
        """Reject new work and fail queued requests; in-flight work may finish."""
        async with self._condition:
            self._closed = True
            for queue in self._queues.values():
                for request in queue:
                    request.state = "closed"
                    self._pending -= 1
                    self._known_ids.discard(request.request_id)
                    if not request.future.done():
                        request.future.set_exception(
                            RuntimeError("request queue closed before dispatch"))
            self._queues.clear()
            self._rotation.clear()
            self._condition.notify_all()
