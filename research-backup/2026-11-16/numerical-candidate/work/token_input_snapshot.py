"""Unwired Linux-only immutable token data store; never task admission.

A trusted caller supplies a dedicated private directory FD, held open throughout
an operation. Local filesystem hard-link and fsync semantics must be qualified
before use. Same-UID hostile writers, storage rollback and OS compromise are not
within this boundary. No imports of application, model or execution modules.
"""
from dataclasses import dataclass
import hashlib
import json
import os
import re
import secrets
import stat
import struct
import sys

_MAGIC = b'OCTOKN01'
_DOMAIN = b'opencompute/token-input-snapshot/v1\0'
_MAX_TOKENS = 131072
_MAX_HEADER = 2048
_MAX_FILE = 12 + _MAX_HEADER + 8 * _MAX_TOKENS
_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z')
_HASH = re.compile(r'[0-9a-f]{64}\Z')


class SnapshotRejected(ValueError):
    """No token, identity, path, hash, parser detail or OS cause in message."""

    def __init__(self):
        super().__init__('token snapshot rejected')


@dataclass(frozen=True, slots=True)
class InputBinding:
    owner_id: str
    request_id: str
    model_digest: str
    preprocessing_version: str
    preprocessing_sha256: str
    vocab_size: int


@dataclass(frozen=True, slots=True)
class SnapshotRef:
    """Data locator only. Constructible by anyone; never an authorization token."""
    digest: str


@dataclass(frozen=True, slots=True)
class StoredInput:
    binding: InputBinding
    tokens: tuple[int, ...]
    token_blob_sha256: str
    reference: SnapshotRef


def _require(condition):
    if not condition:
        raise SnapshotRejected()


def _binding(value):
    _require(type(value) is InputBinding)
    for item in (value.owner_id, value.request_id, value.preprocessing_version):
        _require(type(item) is str and len(item) <= 128 and _ID.fullmatch(item))
    for item in (value.model_digest, value.preprocessing_sha256):
        _require(type(item) is str and len(item) == 64 and _HASH.fullmatch(item))
    _require(type(value.vocab_size) is int and 1 <= value.vocab_size <= 262144)


def _canonical(record):
    return json.dumps(record, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode('ascii')


def _encode(binding, tokens):
    _binding(binding)
    _require(type(tokens) is tuple and 1 <= len(tokens) <= _MAX_TOKENS)
    _require(all(type(t) is int and 0 <= t < binding.vocab_size for t in tokens))
    payload = b''.join(struct.pack('<q', t) for t in tokens)
    blob_hash = hashlib.sha256(payload).hexdigest()
    header = _canonical({
        'version': 1, 'kind': 'TokenInputSnapshot',
        'owner_id': binding.owner_id, 'request_id': binding.request_id,
        'model_digest': binding.model_digest,
        'preprocessing_version': binding.preprocessing_version,
        'preprocessing_sha256': binding.preprocessing_sha256,
        'vocab_size': binding.vocab_size, 'token_count': len(tokens),
        'token_encoding': 'int64-le', 'batch_size': 1,
        'position_mode': 'contiguous-zero', 'mask_mode': 'causal-unpadded',
        'token_blob_sha256': blob_hash})
    _require(len(header) <= _MAX_HEADER)
    return _MAGIC + struct.pack('<I', len(header)) + header + payload, blob_hash


def _pairs(pairs):
    _require(len(pairs) <= 16)
    result = {}
    for key, value in pairs:
        _require(key not in result and type(value) in (str, int))
        result[key] = value
    return result


def _forbidden(_):
    raise SnapshotRejected()


def _integer(text):
    _require(len(text) <= 6 and text.isascii() and text.isdecimal())
    return int(text)


def _decode(raw, binding, reference):
    _binding(binding)
    _require(type(reference) is SnapshotRef and type(reference.digest) is str
             and len(reference.digest) == 64 and _HASH.fullmatch(reference.digest))
    _require(12 < len(raw) <= _MAX_FILE and raw[:8] == _MAGIC)
    _require(hashlib.sha256(_DOMAIN + raw).hexdigest() == reference.digest)
    length = struct.unpack('<I', raw[8:12])[0]
    _require(0 < length <= _MAX_HEADER and 12 + length < len(raw))
    header = raw[12:12 + length].decode('ascii')
    # Containers are never legal within the one flat object; scan before parsing.
    quoted = escaped = False
    depth = 0
    for ch in header:
        if quoted:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == '"':
                quoted = False
        elif ch == '"':
            quoted = True
        elif ch in '[{':
            depth += 1
            _require(ch == '{' and depth == 1)
        elif ch in ']}':
            depth -= 1
            _require(ch == '}' and depth == 0)
    _require(not quoted and depth == 0)
    record = json.loads(header, object_pairs_hook=_pairs, parse_int=_integer,
                        parse_float=_forbidden, parse_constant=_forbidden)
    _require(type(record) is dict)
    payload = raw[12 + length:]
    _require(len(payload) % 8 == 0 and 1 <= len(payload) // 8 <= _MAX_TOKENS)
    tokens = tuple(t[0] for t in struct.iter_unpack('<q', payload))
    # Re-encoding enforces exact fields, types, version, ownership, binding,
    # content digest and unique canonical encoding, including no trailing bytes.
    expected, blob_hash = _encode(binding, tokens)
    _require(expected == raw)
    return StoredInput(binding, tokens, blob_hash, reference)


def _root(root_fd):
    _require(sys.platform == 'linux' and type(root_fd) is int and root_fd >= 0)
    _require(all(hasattr(os, name) for name in ('O_NOFOLLOW', 'O_DIRECTORY', 'O_CLOEXEC')))
    fd = os.dup(root_fd)
    try:
        info = os.fstat(fd)
        _require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid()
                 and stat.S_IMODE(info.st_mode) == 0o700)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _read(directory, reference, binding):
    _require(type(reference) is SnapshotRef and type(reference.digest) is str
             and len(reference.digest) == 64 and _HASH.fullmatch(reference.digest))
    name = reference.digest + '.tokens'
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                 dir_fd=directory)
    try:
        before = os.fstat(fd)
        _require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid()
                 and stat.S_IMODE(before.st_mode) == 0o400 and before.st_nlink == 1
                 and 12 < before.st_size <= _MAX_FILE)
        chunks = []
        remaining = before.st_size + 1
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b''.join(chunks)
        after = os.fstat(fd)
        stable = ('st_dev', 'st_ino', 'st_mode', 'st_uid', 'st_gid', 'st_nlink',
                  'st_size', 'st_mtime_ns', 'st_ctime_ns')
        _require(all(getattr(before, key) == getattr(after, key) for key in stable)
                 and len(raw) == before.st_size)
        result = _decode(raw, binding, reference)
        # Do not return a locator observed during another writer's unsynced link.
        os.fsync(fd)
        os.fsync(directory)
        return result
    finally:
        os.close(fd)


def _publish(root_fd, binding, tokens):
    """Persist data before returning a reference; never publish executable work.

    An exception may leave an orphan. Never recover by trusting filenames alone.
    No overwrite, unlink of published objects, retry, GC or task-store mutation.
    """
    directory = fd = None
    try:
        raw, _ = _encode(binding, tokens)
        reference = SnapshotRef(hashlib.sha256(_DOMAIN + raw).hexdigest())
        directory = _root(root_fd)
        temporary = '.pending-' + secrets.token_hex(32)
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        offset = 0
        while offset < len(raw):
            written = os.write(fd, raw[offset:])
            _require(written > 0)
            offset += written
        os.fchmod(fd, 0o400)
        os.fsync(fd)
        closing_fd = fd
        fd = None  # Never retry close: Linux may already have reused the number.
        os.close(closing_fd)
        try:
            os.link(temporary, reference.digest + '.tokens', src_dir_fd=directory,
                    dst_dir_fd=directory, follow_symlinks=False)
        except FileExistsError:
            # Existing content must satisfy the same full read and binding checks.
            _read(directory, reference, binding)
        os.unlink(temporary, dir_fd=directory)
        os.fsync(directory)
        # Verify the published object before issuing the only outward reference.
        _read(directory, reference, binding)
        return reference
    finally:
        try:
            if fd is not None:
                os.close(fd)
        finally:
            if directory is not None:
                os.close(directory)


def _read_input(root_fd, reference, expected_binding):
    """Caller must authenticate expected owner separately; ref grants no access."""
    directory = None
    try:
        _binding(expected_binding)
        directory = _root(root_fd)
        return _read(directory, reference, expected_binding)
    finally:
        if directory is not None:
            os.close(directory)


def publish_input(root_fd, binding, tokens):
    """Persist before returning a data reference. Never authorize or create tasks."""
    try:
        return _publish(root_fd, binding, tokens)
    except Exception:
        pass
    # Raise outside the handler: do not attach internal OS/parser exceptions.
    del root_fd, binding, tokens
    raise SnapshotRejected()


def read_input(root_fd, reference, expected_binding):
    """Verify data against an independently authenticated expected binding."""
    try:
        return _read_input(root_fd, reference, expected_binding)
    except Exception:
        pass
    del root_fd, reference, expected_binding
    raise SnapshotRejected()
