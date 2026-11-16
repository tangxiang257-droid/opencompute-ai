"""Offline self-consistency only, not authentication, task binding or computation proof.

A manifest digest is only a declaration here: the private manifest is absent.
Callers must establish independent trusted task/reference context before comparison.
"""
import base64
import hashlib
import json
from dataclasses import dataclass

RAW_LIMIT=8*1024*1024
PAYLOAD_LIMIT=4*1024*1024
_ROOT=frozenset(('contract','dtype','axes','total_bytes','tiles','package','manifest_digest','computation_verified','execution_allowed'))
_TILE=frozenset(('tile','start','count','shape','bytes','sha256','data_base64'))

@dataclass(frozen=True,slots=True)
class DeclaredTile:
    index:int
    start:int
    shape:tuple
    data:bytes

@dataclass(frozen=True,slots=True)
class DeclaredPackage:
    declared_manifest_digest:str
    tiles:tuple

def _need(v):
    if not v:raise ValueError('invalid result package')
def _hash(v):
    _need(type(v) is str and len(v)==64 and all(c in '0123456789abcdef' for c in v))
def _int(v,lo,hi):
    _need(type(v) is int and lo<=v<=hi)
def _pairs(pairs):
    obj={}
    for k,v in pairs:
        _need(k not in obj);obj[k]=v
    return obj
def _constant(_):raise ValueError('invalid result package')

def unpack_result_package(raw):
    """Return immutable declared tiles after bounded format/hash checks.

    No model values are evaluated. A self-consistent package may be forged and
    is not associated with an authenticated task by this function.
    """
    try:
        _need(type(raw) is bytes and 0<len(raw)<=RAW_LIMIT)
        p=json.loads(raw.decode('utf-8'),object_pairs_hook=_pairs,parse_constant=_constant)
        _need(type(p) is dict and p.keys()==_ROOT)
        _need(p['package']=='opencompute-result-download-v1' and p['contract']=='prefill_logits_v1')
        _need(p['dtype']=='float32-le' and p['axes']==['batch','token','vocab'])
        _need(p['computation_verified'] is False and p['execution_allowed'] is False)
        _hash(p['manifest_digest']);_int(p['total_bytes'],1,PAYLOAD_LIMIT)
        ts=p['tiles'];_need(type(ts) is list and 1<=len(ts)<=64)
        out=[];end=total=0;vocab=None
        for i,t in enumerate(ts):
            _need(type(t) is dict and t.keys()==_TILE)
            _int(t['tile'],0,63);_need(t['tile']==i)
            _int(t['start'],0,PAYLOAD_LIMIT//4);_need(t['start']==end)
            _int(t['count'],1,PAYLOAD_LIMIT//4)
            shape=t['shape'];_need(type(shape) is list and len(shape)==3)
            for n in shape:_int(n,1,PAYLOAD_LIMIT//4)
            _need(shape[0]==1 and shape[1]==t['count'])
            if vocab is None:vocab=shape[2]
            _need(shape[2]==vocab)
            size=t['count']*vocab*4;_int(t['bytes'],1,PAYLOAD_LIMIT)
            _need(t['bytes']==size and total+size<=PAYLOAD_LIMIT)
            _hash(t['sha256']);encoded=t['data_base64']
            _need(type(encoded) is str and len(encoded)==4*((size+2)//3))
            data=base64.b64decode(encoded,validate=True)
            _need(len(data)==size and base64.b64encode(data).decode('ascii')==encoded)
            _need(hashlib.sha256(data).hexdigest()==t['sha256'])
            out.append(DeclaredTile(i,end,tuple(shape),data));end+=t['count'];total+=size
        _need(total==p['total_bytes'])
        return DeclaredPackage(p['manifest_digest'],tuple(out))
    except (ValueError,TypeError,KeyError,RecursionError,OverflowError):
        raise ValueError('invalid result package') from None
