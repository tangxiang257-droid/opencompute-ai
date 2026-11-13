"""Isolated structural parser; no authority, blobs, I/O or execution capability.

Only parse_plan_bundle is a public operation. Its result is immutable metadata,
not an admission grant. This module is deliberately not wired into the system.
"""
from dataclasses import dataclass
import hashlib
import json
import re


_MAX_BYTES = 64 * 1024
_MAX_INTEGER = (1 << 53) - 1
_MAX_PAYLOAD = 16 * 1024 * 1024
_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z')
_DIGEST = re.compile(r'[0-9a-f]{64}\Z')


class PlanRejected(ValueError):
    """Deliberately payload-free error: no input, field values or parser cause."""

    def __init__(self):
        super().__init__('plan structure rejected')


@dataclass(frozen=True, slots=True)
class FrozenRecord:
    """Recursively immutable record; fields contain only tuples and scalars."""
    fields: tuple


@dataclass(frozen=True, slots=True)
class StructurallyValidPlan:
    """NOT authorization, executable work, verified model or recovery evidence.

    Construction is not a security boundary; callers must never accept this type
    as a capability. No claim of current epoch or device/process ownership.
    """
    canonical_bundle: bytes
    plan_digest: str
    attempt_digest: str
    records: FrozenRecord


def _need(condition):
    if not condition:
        raise PlanRejected()


def _preflight(raw):
    # Bound allocation/depth before json builds a tree. JSON syntax is checked
    # separately; this scanner merely counts containers outside quoted strings.
    _need(type(raw) is bytes and 0 < len(raw) <= _MAX_BYTES)
    text = raw.decode('ascii')
    depth = 0
    quoted = escaped = False
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
            _need(depth <= 8)
        elif char in ']}':
            depth -= 1
            _need(depth >= 0)
    _need(not quoted and depth == 0)
    return text


def _pairs(pairs):
    _need(len(pairs) <= 32)
    result = {}
    for key, value in pairs:
        _need(key not in result)
        result[key] = value
    return result


def _integer(text):
    _need(len(text) <= 16 and text.isascii() and text.isdecimal())
    value = int(text)
    _need(value <= _MAX_INTEGER)
    return value


def _forbidden(_):
    raise PlanRejected()


def _tree(value, budget):
    budget[0] -= 1
    _need(budget[0] >= 0)
    if type(value) is dict:
        for key, item in value.items():
            _tree(key, budget)
            _tree(item, budget)
    elif type(value) is list:
        _need(len(value) <= 64)
        for item in value:
            _tree(item, budget)
    elif type(value) is str:
        _need(len(value) <= 128 and value.isascii())
    else:
        _need(type(value) is int and 0 <= value <= _MAX_INTEGER)


def _canonical(value):
    # Called only on the bounded parsed tree, never arbitrary caller objects.
    if type(value) is dict:
        return '{' + ','.join(_canonical(k) + ':' + _canonical(value[k])
                              for k in sorted(value)) + '}'
    if type(value) is list:
        return '[' + ','.join(_canonical(v) for v in value) + ']'
    if type(value) is str:
        out = []
        for char in value:
            if char in ('"', '\\'):
                out.append('\\' + char)
            elif ord(char) < 32:
                out.append('\\u%04x' % ord(char))
            else:
                out.append(char)
        return '"' + ''.join(out) + '"'
    return str(value)


def _digest(record):
    domain = ('opencompute/pair/v1/' + record['kind'] + '\0').encode('ascii')
    return hashlib.sha256(domain + _canonical(record).encode('ascii')).hexdigest()


def _record(value, kind, fields):
    _need(type(value) is dict and set(value) == set(fields.split()) | {'kind', 'version'})
    _need(value['kind'] == kind and type(value['version']) is int and value['version'] == 1)


def _number(value, low, high):
    _need(type(value) is int and low <= value <= high)


def _hash(value):
    _need(type(value) is str and _DIGEST.fullmatch(value) is not None)


def _identifier(value):
    _need(type(value) is str and _IDENTIFIER.fullmatch(value) is not None)


def _freeze(value):
    if type(value) is dict:
        return FrozenRecord(tuple((key, _freeze(value[key])) for key in sorted(value)))
    if type(value) is list:
        return tuple(_freeze(item) for item in value)
    return value


def _validate(bundle):
    _record(bundle, 'PlanBundle', 'model input_snapshot layout plan attempt ports')
    model, snapshot, layout, plan, attempt, ports = (
        bundle[key] for key in ('model', 'input_snapshot', 'layout', 'plan', 'attempt', 'ports'))
    _record(model, 'ModelIdentity', 'config_sha256 weights_sha256 semantics weight_format '
            'compute_dtype kv_dtype activation_dtype logits_dtype')
    for key in ('config_sha256', 'weights_sha256'):
        _hash(model[key])
    _need(model['semantics'] == 'stock-qwen2-full-causal-v1')
    _need(model['weight_format'] == 'safetensors-single')
    for key in ('compute_dtype', 'kv_dtype', 'activation_dtype', 'logits_dtype'):
        _need(model[key] == 'float32')

    _record(snapshot, 'InputSnapshot', 'request_id token_blob_sha256 token_count '
            'token_encoding position_mode mask_mode batch_size')
    _identifier(snapshot['request_id'])
    _hash(snapshot['token_blob_sha256'])
    _number(snapshot['token_count'], 1, 131072)
    _need(snapshot['token_encoding'] == 'int64-le')
    _need(snapshot['position_mode'] == 'contiguous-zero')
    _need(snapshot['mask_mode'] == 'causal-unpadded')
    _number(snapshot['batch_size'], 1, 1)

    _record(layout, 'Layout', 'layout_schema_version layer_count hidden_size vocab_size stages')
    _number(layout['layout_schema_version'], 1, 1)
    _number(layout['layer_count'], 2, 48)
    _number(layout['hidden_size'], 1, 4096)
    _number(layout['vocab_size'], 1, 262144)
    stages = layout['stages']
    _need(type(stages) is list and len(stages) == 2)
    previous_end = 0
    for index, stage in enumerate(stages):
        _record(stage, 'StageLayout', 'stage_id layer_start layer_end input_role output_role')
        _number(stage['stage_id'], index, index)
        _number(stage['layer_start'], previous_end, previous_end)
        _number(stage['layer_end'], previous_end + 1, layout['layer_count'])
        _need(stage['input_role'] == ('token_ids', 'hidden_states')[index])
        _need(stage['output_role'] == ('hidden_states', 'logits')[index])
        previous_end = stage['layer_end']
    _need(previous_end == layout['layer_count'])

    _record(plan, 'ExecutionPlan', 'model_digest input_snapshot_digest layout_digest max_tile_tokens mode')
    for key, record in (('model_digest', model), ('input_snapshot_digest', snapshot),
                        ('layout_digest', layout)):
        _hash(plan[key])
        _need(plan[key] == _digest(record))
    _number(plan['max_tile_tokens'], 1, min(1048576, snapshot['token_count']))
    _need(plan['mode'] == 'prefill-only')

    _record(attempt, 'AttemptIdentity', 'task_id session_id epoch plan_digest assignments')
    for key in ('task_id', 'session_id'):
        _identifier(attempt[key])
    _number(attempt['epoch'], 1, _MAX_INTEGER)
    _hash(attempt['plan_digest'])
    _need(attempt['plan_digest'] == _digest(plan))
    assignments = attempt['assignments']
    _need(type(assignments) is list and len(assignments) == 2)
    devices, instances = set(), set()
    for index, assignment in enumerate(assignments):
        _record(assignment, 'StageAssignment', 'stage_id agent_id device_uuid process_instance_id')
        _number(assignment['stage_id'], index, index)
        _identifier(assignment['agent_id'])
        _identifier(assignment['device_uuid'])
        _hash(assignment['process_instance_id'])
        _need(assignment['device_uuid'] not in devices)
        _need(assignment['process_instance_id'] not in instances)
        devices.add(assignment['device_uuid'])
        instances.add(assignment['process_instance_id'])

    # Three descriptors declare the maximum-size Tile ports, in fixed order.
    # Their hashes are claims only: no payload is read or certified here.
    _need(type(ports) is list and len(ports) == 3)
    count = plan['max_tile_tokens']
    profiles = (
        ('token_ids', 'int64', [1, count], ['batch', 'token'], 8),
        ('hidden_states', 'float32', [1, count, layout['hidden_size']], ['batch', 'token', 'hidden'], 4),
        ('logits', 'float32', [1, count, layout['vocab_size']], ['batch', 'token', 'vocab'], 4),
    )
    for port, (role, dtype, shape, axes, width) in zip(ports, profiles):
        _record(port, 'TensorDescriptor', 'role dtype shape axes payload_bytes payload_sha256')
        _need(port['role'] == role and port['dtype'] == dtype)
        _need(type(port['shape']) is list and port['shape'] == shape)
        _need(type(port['axes']) is list and port['axes'] == axes)
        size = width
        for dim in port['shape']:
            _number(dim, 1, _MAX_INTEGER)
            _need(dim <= _MAX_PAYLOAD // size)
            size *= dim
        _number(port['payload_bytes'], size, size)
        _hash(port['payload_sha256'])


def parse_plan_bundle(raw: bytes) -> StructurallyValidPlan:
    """Parse canonical v1 metadata; rejection messages omit payload values.

    No model/config/token bytes are accepted or read. Digest equality certifies
    internal references only. No epoch freshness, alias resolution, global ID
    uniqueness, hardware capability or authorization is established.
    """
    rejected = False
    try:
        text = _preflight(raw)
        bundle = json.loads(text, object_pairs_hook=_pairs, parse_int=_integer,
                            parse_float=_forbidden, parse_constant=_forbidden)
        _tree(bundle, [2048])
        canonical = _canonical(bundle).encode('ascii')
        _need(canonical == raw)
        _validate(bundle)
        result = StructurallyValidPlan(canonical, _digest(bundle['plan']),
                                       _digest(bundle['attempt']), _freeze(bundle))
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, OverflowError):
        rejected = True
    # Raise outside the handler: no JSONDecodeError (which retains source text)
    # is attached as __context__. Caller tracebacks may still contain raw locals;
    # do not log traceback frames/locals for untrusted input.
    if rejected:
        raw = text = bundle = canonical = None
        raise PlanRejected() from None
    return result
