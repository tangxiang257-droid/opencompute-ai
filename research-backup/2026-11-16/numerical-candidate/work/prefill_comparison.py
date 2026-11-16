"""Offline full-prefill arithmetic observation; no provenance or authority.

Shapes are declarations, not verified task semantics. Required tolerances use
Python-double arithmetic, not torch FP32. Callers must not persist exceptions
or traceback locals. No task/source authenticity, acceptance or run permission
is established here. Joining adds up to 4 MiB, not a process RSS guarantee.
"""
from result_package import unpack_result_package
from float32_comparison import compare_float32,MAX_BYTES

def observe_prefill_difference(package_raw,reference,*,reference_shape,atol,rtol):
    try:
        if type(reference) is not bytes or not 0<len(reference)<=MAX_BYTES:
            raise ValueError
        if type(reference_shape) is not tuple:
            raise ValueError
        package=unpack_result_package(package_raw)
        shape=(1,sum(tile.shape[1] for tile in package.tiles),package.tiles[0].shape[2])
        actual=b''.join(tile.data for tile in package.tiles)
        return compare_float32(actual,reference,actual_shape=shape,
                               reference_shape=reference_shape,atol=atol,rtol=rtol)
    except ValueError:
        raise ValueError('invalid prefill observation input') from None
