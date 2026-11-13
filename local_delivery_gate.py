"""Local byte-delivery ordering only. No network, DB or execution authority.

A LocalAuthority is a trusted in-process wiring capability, not authentication.
Do not construct one from remote claims or treat it as distributed authorization.
Payloads are immutable bytes: no mutable storage is recycled. Accounting bounds
owned payload bytes, not retained consumer copies, allocator memory or RSS.
"""
from dataclasses import dataclass
import threading
import time


class Denied(ValueError):
    def __init__(self):
        super().__init__('delivery unavailable')


class LocalAuthority:
    """Explicit local bootstrap capability; carries no credentials or truth claim."""
    __slots__ = ()


@dataclass(frozen=True, slots=True)
class Scope:
    task: str
    epoch: int
    plan: str
    layout: str
    direction: str
    port: str
    session: str
    attempt: str
    peer_node: str
    peer_certificate_der_sha256: str
    incarnation: str


def _scope(s):
    if type(s) is not Scope or type(s.epoch) is not int or not 1 <= s.epoch < 2**31:
        raise Denied()
    for field in ('task', 'direction', 'port', 'session', 'attempt', 'peer_node', 'incarnation'):
        v = getattr(s, field)
        if type(v) is not str or not 1 <= len(v) <= 128 or not v.isascii():
            raise Denied()
    for field in ('plan', 'layout', 'peer_certificate_der_sha256'):
        v = getattr(s, field)
        if type(v) is not str or len(v) != 64 or any(c not in '0123456789abcdef' for c in v):
            raise Denied()


class Delivery:
    """Unique local handle; release never revokes previously obtained bytes."""
    __slots__ = ('_gate', '_key')
    def __init__(self, gate, key):
        self._gate, self._key = gate, key
    @property
    def payload(self):
        return self._gate._payload(self)
    def release(self):
        self._gate._release(self)


class DeliveryGate:
    """One scope, one incarnation; permanently fenced on revoke/close/expiry.

    Default construction cannot authorize anything. Authority token identity and
    all mutable state are local. No external authority callback or clock is called
    while holding the lock. Deadlines are local monotonic timestamps only.
    """
    def __init__(self, *, authority=None, capacity=1048576, max_tiles=64):
        if authority is not None and type(authority) is not LocalAuthority:
            raise Denied()
        if type(capacity) is not int or not 1 <= capacity <= 1048576:
            raise Denied()
        if type(max_tiles) is not int or not 1 <= max_tiles <= 64:
            raise Denied()
        self._authority = authority
        self._capacity, self._max_tiles = capacity, max_tiles
        self._cv = threading.Condition(threading.Lock())
        self._state = 'UNAUTHORIZED'
        self._scope = None
        self._deadline = 0
        self._mapping = ()
        self._seen = set()
        self._queued = {}
        self._claimed = {}
        self._used = 0

    def authorize(self, authority, scope, *, deadline, tiles):
        _scope(scope)
        if type(deadline) not in (int, float) or not time.monotonic() < deadline <= time.monotonic() + 3600:
            raise Denied()
        # Canonical contiguous Tile->Token mapping. Bounded, retained for lifetime.
        if type(tiles) is not tuple or not 1 <= len(tiles) <= self._max_tiles:
            raise Denied()
        end = 0
        for entry in tiles:
            if type(entry) is not tuple or len(entry) != 2 or any(type(x) is not int for x in entry):
                raise Denied()
            if entry[0] != end or not end < entry[1] <= 131072:
                raise Denied()
            end = entry[1]
        with self._cv:
            if self._authority is None or authority is not self._authority or self._state != 'UNAUTHORIZED':
                raise Denied()
            if time.monotonic() >= deadline:
                raise Denied()
            self._scope, self._deadline, self._mapping = scope, deadline, tiles
            self._state = 'OPEN'
            self._cv.notify_all()

    def _fence(self, state):
        self._state = state
        self._used -= sum(len(v) for v in self._queued.values())
        self._queued.clear()  # Exact immutable bytes; no custom finalizers/callbacks.
        self._cv.notify_all()

    def _check(self, scope):
        if self._state == 'OPEN' and time.monotonic() >= self._deadline:
            self._fence('EXPIRED')
        if self._state != 'OPEN' or scope != self._scope:
            raise Denied()

    def revoke(self, authority):
        with self._cv:
            if self._authority is None or authority is not self._authority:
                raise Denied()
            self._fence('REVOKED')

    def close(self):
        with self._cv:
            self._fence('CLOSED')

    def publish(self, scope, tile, token_range, payload, *, timeout=0):
        _scope(scope)
        if type(tile) is not int or type(token_range) is not tuple or len(token_range) != 2 or any(type(x) is not int for x in token_range):
            raise Denied()
        if type(payload) is not bytes or not 1 <= len(payload) <= self._capacity:
            raise Denied()
        if type(timeout) not in (int, float) or not 0 <= timeout <= 3600:
            raise Denied()
        until = time.monotonic() + timeout
        with self._cv:
            while True:
                self._check(scope)
                if not 0 <= tile < len(self._mapping) or token_range != self._mapping[tile] or tile in self._seen:
                    raise Denied()
                if self._used + len(payload) <= self._capacity:
                    break
                remaining = min(until, self._deadline) - time.monotonic()
                if remaining <= 0:
                    raise Denied()
                self._cv.wait(remaining)
            # Prepare allocating updates before publishing any state.
            queued = dict(self._queued)
            queued[tile] = payload
            seen = self._seen | {tile}
            used = self._used + len(payload)
            self._queued, self._seen, self._used = queued, seen, used
            self._cv.notify_all()

    def claim(self, scope, tile, *, timeout=0):
        _scope(scope)
        if type(tile) is not int or type(timeout) not in (int, float) or not 0 <= timeout <= 3600:
            raise Denied()
        until = time.monotonic() + timeout
        handle = Delivery(self, object())
        with self._cv:
            while True:
                self._check(scope)  # Including after each wait, before queue lookup.
                if not 0 <= tile < len(self._mapping):
                    raise Denied()
                if tile in self._queued:
                    payload = self._queued[tile]
                    self._claimed[handle._key] = (handle, payload)
                    del self._queued[tile]  # Linearization: ownership transferred.
                    return handle
                if tile in self._seen:
                    raise Denied()
                remaining = min(until, self._deadline) - time.monotonic()
                if remaining <= 0:
                    raise Denied()
                self._cv.wait(remaining)

    def _payload(self, handle):
        with self._cv:
            item = self._claimed.get(handle._key)
            if item is None or item[0] is not handle:
                raise Denied()
            return item[1]

    def _release(self, handle):
        with self._cv:
            item = self._claimed.get(handle._key)
            if item is None or item[0] is not handle:
                raise Denied()
            del self._claimed[handle._key]
            self._used -= len(item[1])
            self._cv.notify_all()
