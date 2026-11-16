"""Shared bounded UTF-8 JSON-object decoding, no file or framework imports.

Not canonical JSON: ordinary whitespace, key order, Unicode and valid finite
numbers remain supported. Duplicate decoded keys reject at every object depth.
"""
import json
import math

MAX_JSON_BYTES = 4_000_000

def _pairs(items):
    result={}
    for key,value in items:
        if key in result:raise ValueError()
        result[key]=value
    return result

def _constant(value):
    raise ValueError()

def _float(value):
    number=float(value)
    if not math.isfinite(number):raise ValueError()
    return number

def parse_json_object(raw: bytes, limit: int) -> dict:
    failed=False
    try:
        if (type(limit) is not int or not 1<=limit<=MAX_JSON_BYTES
                or type(raw) is not bytes or not 0<len(raw)<=limit):
            raise ValueError()
        text=raw.decode('utf-8',errors='strict')
        if text.startswith('\ufeff'):raise ValueError()
        result=json.loads(text,object_pairs_hook=_pairs,
                          parse_constant=_constant,parse_float=_float)
        if type(result) is not dict:raise ValueError()
    except (ValueError,TypeError,UnicodeError,RecursionError,OverflowError):
        failed=True
    if failed:
        raw=text=result=None
        raise ValueError('invalid bounded UTF-8 JSON object') from None
    return result
