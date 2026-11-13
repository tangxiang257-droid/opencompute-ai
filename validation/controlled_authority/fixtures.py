import ast
from dataclasses import replace
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import struct
import sys
import tempfile
import threading
import unittest
ROOT=Path(__file__).resolve().parents[2]
ALLOWED={
 'token_input_snapshot':{'dataclasses','hashlib','json','os','re','secrets','stat','struct','sys'},
 'snapshot_task_ledger':{'contextlib','dataclasses','hashlib','json','re','secrets','sqlite3','token_input_snapshot'},
 'inert_attempt_ledger':{'dataclasses','snapshot_task_ledger'},
 'ledger_database':{'contextlib','dataclasses','os','sqlite3','stat','sys','snapshot_task_ledger'},
 'pair_plan_contract':{'dataclasses','hashlib','json','re'},
 'result_artifacts':{'dataclasses','hashlib','json','os','secrets','stat','struct','inert_attempt_ledger','snapshot_task_ledger','token_input_snapshot','ledger_database'},
 'structured_result_binding':{'dataclasses','json','pair_plan_contract','result_artifacts','ledger_database','snapshot_task_ledger','token_input_snapshot'}}
HASHES={}
for name,allowed in ALLOWED.items():
    raw=(ROOT/(name+'.py')).read_bytes();tree=ast.parse(raw)
    found={v for n in ast.walk(tree) for v in ([n.module] if isinstance(n,ast.ImportFrom) else [a.name for a in n.names] if isinstance(n,ast.Import) else [])}
    assert found==allowed,(name,found)
    HASHES[name]=hashlib.sha256(raw).hexdigest()
    spec=importlib.util.spec_from_file_location(name,ROOT/(name+'.py'));mod=importlib.util.module_from_spec(spec);sys.modules[name]=mod;spec.loader.exec_module(mod)
m=sys.modules['result_artifacts'];b=sys.modules['ledger_database'];s=sys.modules['token_input_snapshot'];base=sys.modules['snapshot_task_ledger']
A=(('node-a','device-a','c'*64),('node-b','device-b','d'*64))
B=s.InputBinding('alice','request','a'*64,'prep','b'*64,100)
L=m.Limits(2*1024*1024,4*1024*1024,8*1024*1024,16*1024*1024,100,4)

def chunks(raw):
    for start in range(0,len(raw),65536):yield raw[start:start+65536]
c=sys.modules['pair_plan_contract'];v=sys.modules['structured_result_binding']

def record(kind,**kw):return dict(kind=kind,version=1,**kw)
MODEL=record('ModelIdentity',config_sha256='1'*64,weights_sha256='2'*64,semantics='stock-qwen2-full-causal-v1',weight_format='safetensors-single',compute_dtype='float32',kv_dtype='float32',activation_dtype='float32',logits_dtype='float32')
B=replace(B,model_digest=c._digest(MODEL))

def bundle(task,*,tile=2,hidden=4,session='session'):
    snapshot=record('InputSnapshot',request_id=B.request_id,token_blob_sha256=hashlib.sha256(struct.pack('<qqq',1,2,3)).hexdigest(),token_count=3,token_encoding='int64-le',position_mode='contiguous-zero',mask_mode='causal-unpadded',batch_size=1)
    stages=[record('StageLayout',stage_id=i,layer_start=i,layer_end=i+1,input_role=('token_ids','hidden_states')[i],output_role=('hidden_states','logits')[i]) for i in (0,1)]
    layout=record('Layout',layout_schema_version=1,layer_count=2,hidden_size=hidden,vocab_size=100,stages=stages)
    plan=record('ExecutionPlan',model_digest=c._digest(MODEL),input_snapshot_digest=c._digest(snapshot),layout_digest=c._digest(layout),max_tile_tokens=tile,mode='prefill-only')
    attempt=record('AttemptIdentity',task_id=task,session_id=session,epoch=1,plan_digest=c._digest(plan),assignments=[record('StageAssignment',stage_id=i,agent_id=a[0],device_uuid=a[1],process_instance_id=a[2]) for i,a in enumerate(A)])
    ports=[record('TensorDescriptor',role=role,dtype=dtype,shape=shape,axes=axes,payload_bytes=size,payload_sha256='0'*64) for role,dtype,shape,axes,size in (
        ('token_ids','int64',[1,tile],['batch','token'],8*tile),('hidden_states','float32',[1,tile,hidden],['batch','token','hidden'],4*tile*hidden),('logits','float32',[1,tile,100],['batch','token','vocab'],4*tile*100))]
    return record('PlanBundle',model=MODEL.copy(),input_snapshot=snapshot,layout=layout,plan=plan,attempt=attempt,ports=ports)

def encode(data):return c._canonical(data).encode('ascii')
def rehash(data):
    for key,name in (('model_digest','model'),('input_snapshot_digest','input_snapshot'),('layout_digest','layout')):data['plan'][key]=c._digest(data[name])
    data['attempt']['plan_digest']=c._digest(data['plan']);return data
