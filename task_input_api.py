"""Unmounted, storage-only ASGI factory; default user verifier always rejects.

No global app, environment credentials, login/registration, network, workers or
execution imports. A trusted caller supplies an exclusively owned ManagedLedger
and, optionally, a real USER verifier. Test verifiers live only in tests.
"""
from dataclasses import dataclass
import hashlib
import json
import re
from typing import Protocol

from token_input_snapshot import InputBinding

_MAX_BODY = 2 * 1024 * 1024
_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z')
_HASH = re.compile(r'[0-9a-f]{64}\Z')
_TASK = re.compile(r'/tasks/([0-9a-f]{32})\Z')
_FORBIDDEN = frozenset((b'x-client-id', b'x-owner-id', b'owner-id', b'owner',
                        b'x-user-id', b'x-authenticated-user', b'x-forwarded-user',
                        b'x-admin-key', b'x-api-key', b'x-node-token', b'x-join-key',
                        b'x-worker-token'))


@dataclass(frozen=True, slots=True)
class Principal:
    """Verifier result only. Constructing this object alone grants no authority."""
    issuer: str
    subject: str


class UserVerifier(Protocol):
    def verify(self, bearer: str) -> Principal | None:
        """Validate actual USER credentials, issuer, audience, expiry/revocation.

        Never accept admin/worker secrets as user credentials. Do not log bearer.
        This protocol does not implement an identity provider or prove verification.
        """
        ...


class RejectAllUsers:
    def verify(self, bearer):
        return None


class _Rejected(ValueError):
    pass


def _need(condition):
    if not condition:
        raise _Rejected()


def _owner(principal):
    _need(type(principal) is Principal)
    values = []
    for value in (principal.issuer, principal.subject):
        _need(type(value) is str and 1 <= len(value) <= 256)
        _need(all(ord(c) >= 32 and ord(c) != 127 for c in value))
        raw = value.encode('utf-8')
        _need(len(raw) <= 512)
        values.append(len(raw).to_bytes(4, 'big') + raw)
    return 'usr_' + hashlib.sha256(b'opencompute/user-owner/v1\0' + b''.join(values)).hexdigest()


def _headers(scope):
    rows = scope.get('headers', [])
    _need(type(rows) in (list, tuple) and len(rows) <= 32)
    result = {}
    size = 0
    for key, value in rows:
        _need(type(key) is bytes and type(value) is bytes)
        key = key.lower()
        size += len(key) + len(value)
        _need(size <= 16384 and key not in result)
        result[key] = value
    return result


def _pairs(pairs):
    _need(len(pairs) <= 6)
    result = {}
    for key, value in pairs:
        _need(key not in result)
        result[key] = value
    return result


def _integer(text):
    _need(len(text) <= 10 and text.isascii() and text.isdecimal())
    return int(text)


def _forbidden_number(_):
    raise _Rejected()


def _payload(raw, owner):
    # Bound nesting and node count before allocating a JSON tree.
    text = raw.decode('utf-8')
    quoted = escaped = False
    depth = commas = 0
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in '[{':
            depth += 1
            _need(depth <= 2)
        elif char in ']}':
            depth -= 1
            _need(depth >= 0)
        elif char == ',':
            commas += 1
            _need(commas <= 131080)
    _need(not quoted and depth == 0)
    value = json.loads(text, object_pairs_hook=_pairs, parse_int=_integer,
                       parse_float=_forbidden_number, parse_constant=_forbidden_number)
    fields = {'request_id', 'model_digest', 'preprocessing_version',
              'preprocessing_sha256', 'vocab_size', 'token_ids'}
    _need(type(value) is dict and set(value) == fields)
    for key in ('request_id', 'preprocessing_version'):
        item = value[key]
        _need(type(item) is str and len(item) <= 128 and _ID.fullmatch(item))
    for key in ('model_digest', 'preprocessing_sha256'):
        item = value[key]
        _need(type(item) is str and len(item) == 64 and _HASH.fullmatch(item))
    vocab, tokens = value['vocab_size'], value['token_ids']
    _need(type(vocab) is int and 1 <= vocab <= 262144)
    _need(type(tokens) is list and 1 <= len(tokens) <= 131072)
    _need(all(type(t) is int and 0 <= t < vocab for t in tokens))
    return InputBinding(owner, value['request_id'], value['model_digest'],
                        value['preprocessing_version'], value['preprocessing_sha256'], vocab), tuple(tokens)


async def _body(receive, headers):
    _need(headers.get(b'content-type', b'').lower() == b'application/json')
    declared = headers.get(b'content-length')
    if declared is not None:
        _need(len(declared) <= 7 and declared.isdigit() and int(declared) <= _MAX_BODY)
    parts = bytearray()
    for _ in range(4096):
        event = await receive()
        _need(event.get('type') == 'http.request')
        chunk = event.get('body', b'')
        _need(type(chunk) is bytes and len(parts) + len(chunk) <= _MAX_BODY)
        parts.extend(chunk)
        if not event.get('more_body', False):
            _need(declared is None or int(declared) == len(parts))
            return bytes(parts)
    raise _Rejected()


def _metadata(task):
    _need(type(task.task_id) is str and re.fullmatch(r'[0-9a-f]{32}', task.task_id))
    _need(task.state in ('STORED', 'CANCELLED'))
    return {'task_id': task.task_id,
            'input_status': 'registered' if task.state == 'STORED' else 'cancelled'}


async def _reply(send, status, payload):
    raw = json.dumps(payload, separators=(',', ':'), ensure_ascii=True).encode('ascii')
    await send({'type': 'http.response.start', 'status': status, 'headers': [
        (b'content-type', b'application/json'), (b'cache-control', b'no-store'),
        (b'x-content-type-options', b'nosniff'), (b'content-length', str(len(raw)).encode('ascii'))]})
    await send({'type': 'http.response.body', 'body': raw})


class _InputApplication:
    def __init__(self, ledger, verifier):
        self._ledger = ledger
        self._verifier = verifier

    async def _dispatch(self, scope, receive):
        try:
            headers = _headers(scope)
            _need(not (_FORBIDDEN & headers.keys()))
            authorization = headers.pop(b'authorization', b'')
            _need(authorization.startswith(b'Bearer ') and 7 < len(authorization) <= 4103)
            token = authorization[7:].decode('ascii')
            _need(all(33 <= ord(c) <= 126 for c in token))
            owner = _owner(self._verifier.verify(token))
        except Exception:
            return 401, {'error': 'user authentication required'}
        finally:
            token = authorization = None
        if scope.get('query_string', b''):
            return 400, {'error': 'invalid request'}
        method, path = scope.get('method'), scope.get('path')
        if method == 'POST' and path == '/tasks':
            try:
                key = headers.get(b'idempotency-key', b'').decode('ascii')
                _need(len(key) <= 128 and _ID.fullmatch(key))
                raw = await _body(receive, headers)
                binding, tokens = _payload(raw, owner)
            except Exception:
                return 400, {'error': 'invalid request'}
            try:
                task = self._ledger.create(owner, key, binding, tokens)
                metadata = _metadata(task)
            except Exception:
                return 503, {'error': 'input registration unavailable'}
            return 200, metadata
        match = _TASK.fullmatch(path) if type(path) is str else None
        if method == 'GET' and match:
            try:
                task, stored = self._ledger.read(owner, match.group(1))
                del stored
                metadata = _metadata(task)
            except Exception:
                return 404, {'error': 'task unavailable'}
            return 200, metadata
        return 404, {'error': 'task unavailable'}

    async def __call__(self, scope, receive, send):
        # No lifespan startup hook or websocket surface, no service startup here.
        if scope.get('type') != 'http':
            raise RuntimeError('only isolated HTTP scopes supported')
        status, payload = await self._dispatch(scope, receive)
        # Dispatch frame has exited all exception handlers before calling send.
        # Host-owned request scopes, access logs and memory dumps remain host duties.
        del scope, receive
        await _reply(send, status, payload)


def create_app(ledger, *, verifier=None):
    """Explicit unmounted factory. No env discovery or test-auth auto-loading.

    Caller owns storage lifecycle. Default rejects every credential, including
    admin/worker keys. A configured verifier must independently verify USER identity.
    """
    return _InputApplication(ledger, RejectAllUsers() if verifier is None else verifier)
