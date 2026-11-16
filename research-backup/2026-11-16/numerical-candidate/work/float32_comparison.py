"""Offline numeric comparison only; no provenance, execution or acceptance authority.

Inputs must already be bound to the intended model/task/reference by the caller.
Tolerance arithmetic uses Python doubles, not torch float32 operations.
"""
import math
import struct

MAX_BYTES = 4 * 1024 * 1024

def _shape(value):
    if type(value) is not tuple or not 1 <= len(value) <= 4:
        raise ValueError('invalid comparison input')
    count = 1
    for n in value:
        if type(n) is not int or not 0 < n <= MAX_BYTES // 4:
            raise ValueError('invalid comparison input')
        count *= n
        if count > MAX_BYTES // 4:
            raise ValueError('invalid comparison input')
    return count

def _tolerance(value):
    if type(value) not in (int, float):
        raise ValueError('invalid comparison input')
    try:
        value = float(value)
    except OverflowError:
        raise ValueError('invalid comparison input') from None
    if not math.isfinite(value) or value < 0:
        raise ValueError('invalid comparison input')
    return value

def compare_float32(actual, reference, *, actual_shape, reference_shape, atol, rtol):
    """Compare finite little-endian float32 values under explicit directional bounds.

    This never authenticates a result or establishes computation correctness.
    No tolerances are selected automatically; shape is supplied, not inferred.
    """
    count = _shape(actual_shape)
    _shape(reference_shape)
    if actual_shape != reference_shape:
        raise ValueError('invalid comparison input')
    if type(actual) is not bytes or type(reference) is not bytes:
        raise ValueError('invalid comparison input')
    if len(actual) != count * 4 or len(reference) != count * 4:
        raise ValueError('invalid comparison input')
    atol, rtol = _tolerance(atol), _tolerance(rtol)
    violations = 0
    max_error = 0.0
    for (a,), (r,) in zip(struct.iter_unpack('<f', actual), struct.iter_unpack('<f', reference)):
        if not math.isfinite(a) or not math.isfinite(r):
            raise ValueError('invalid comparison input')
        error = abs(a-r)
        bound = atol + rtol * abs(r)
        if not math.isfinite(bound):
            raise ValueError('invalid comparison input')
        max_error = max(max_error, error)
        violations += error > bound
    return {'element_count': count, 'violation_count': violations,
            'max_abs_error': max_error, 'within_tolerance': violations == 0}
