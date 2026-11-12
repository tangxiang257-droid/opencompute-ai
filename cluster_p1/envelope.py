"""Bounded version-one tensor envelope; digest covers header and payload."""
from dataclasses import dataclass
import hashlib
import hmac
import json
import struct
MAX_PAYLOAD = 16 * 1024 * 1024
MAX_HEADER = 4096
PREFIX = struct.Struct('!4sBIQ')
DTYPES = {'float32': 4, 'float16': 2, 'bfloat16': 2, 'int64': 8, 'int32': 4, 'int8': 1, 'uint8': 1, 'bool': 1}

class EnvelopeError(ValueError):
    pass

@dataclass(frozen=True)
class Envelope:
    task_id: str
    session_id: str
    execution_epoch: int
    stage_id: int
    tile_id: int
    token_range: tuple
    model_hash: str
    layout_version: int
    tensor_shape: tuple
    dtype: str
    payload: bytes

    def validate(self, max_payload=MAX_PAYLOAD):
        for s in (self.task_id, self.session_id):
            if not isinstance(s, str) or not 1 <= len(s.encode()) <= 128:
                raise EnvelopeError('identity bounds')
        for n in (self.execution_epoch, self.stage_id, self.tile_id, self.layout_version):
            if type(n) is not int or not 0 <= n < 2 ** 63:
                raise EnvelopeError('integer bounds')
        if self.execution_epoch < 1 or self.layout_version < 1:
            raise EnvelopeError('epoch/layout must be positive')
        if len(self.token_range) != 2 or any((type(n) is not int or not 0 <= n < 2 ** 63 for n in self.token_range)) or self.token_range[0] >= self.token_range[1]:
            raise EnvelopeError('token range')
        if not isinstance(self.model_hash, str) or len(self.model_hash) != 64 or any((c not in '0123456789abcdef' for c in self.model_hash)):
            raise EnvelopeError('model hash')
        if self.dtype not in DTYPES or not 1 <= len(self.tensor_shape) <= 16:
            raise EnvelopeError('dtype/shape')
        size = DTYPES[self.dtype]
        for n in self.tensor_shape:
            if type(n) is not int or not 1 <= n <= max_payload or size > max_payload // n:
                raise EnvelopeError('shape bounds')
            size *= n
        if not isinstance(self.payload, bytes) or size != len(self.payload) or size > max_payload:
            raise EnvelopeError('payload size')

    def encode(self):
        self.validate()
        d = {k: v for k, v in self.__dict__.items() if k != 'payload'}
        h = json.dumps(d, sort_keys=True, separators=(',', ':')).encode()
        if len(h) > MAX_HEADER:
            raise EnvelopeError('header bounds')
        body = PREFIX.pack(b'TENV', 1, len(h), len(self.payload)) + h + self.payload
        return body + hashlib.sha256(body).digest()

def frame_size(prefix, max_payload=MAX_PAYLOAD):
    if len(prefix) != PREFIX.size:
        raise EnvelopeError('prefix truncated')
    magic, version, h, p = PREFIX.unpack(prefix)
    if magic != b'TENV' or version != 1 or (not 1 <= h <= MAX_HEADER) or (not 1 <= p <= max_payload):
        raise EnvelopeError('frame bounds/version')
    return PREFIX.size + h + p + 32

def decode(data, expected=None, max_payload=MAX_PAYLOAD):
    try:
        total = frame_size(data[:PREFIX.size], max_payload)
        if total != len(data):
            raise EnvelopeError('frame length')
        if not hmac.compare_digest(hashlib.sha256(data[:-32]).digest(), data[-32:]):
            raise EnvelopeError('integrity')
        _, _, h, _ = PREFIX.unpack(data[:PREFIX.size])

        def unique(pairs):
            d = {}
            for k, v in pairs:
                if k in d:
                    raise EnvelopeError('duplicate field')
                d[k] = v
            return d
        d = json.loads(data[PREFIX.size:PREFIX.size + h], object_pairs_hook=unique)
        d['token_range'] = tuple(d['token_range'])
        d['tensor_shape'] = tuple(d['tensor_shape'])
        e = Envelope(**d, payload=data[PREFIX.size + h:-32])
        e.validate(max_payload)
        if expected is not None and (e.task_id, e.session_id, e.execution_epoch) != expected:
            raise EnvelopeError('context mismatch')
        return e
    except (TypeError, KeyError, UnicodeError, ValueError, OverflowError, struct.error) as exc:
        raise EnvelopeError(str(exc)) from exc
