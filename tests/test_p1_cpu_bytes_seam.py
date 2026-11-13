"""Actual private factory AST with ordinary stand-ins; never imports Torch."""
import ast
import math
import struct
import threading
import types
import unittest
from pathlib import Path
from test_p1_tile_contract_v2 import fixture
from cluster_p1.contracts import State

SOURCE=Path(__file__).resolve().parents[1]/'compute/qwen2_tiled_stage.py'
class Tensor:
    def __init__(self,values,dtype='float32',device='cpu',shape=None):
        self.values=list(values);self.dtype=dtype;self.device=types.SimpleNamespace(type=device);self.shape=shape or (len(self.values),)
    def reshape(self,shape):
        if shape==-1:shape=(len(self.values),)
        return Tensor(self.values,self.dtype,self.device.type,tuple(shape))
    def tolist(self):return list(self.values)

class Tests(unittest.TestCase):
    def setUp(self):
        self.loads=[];self.calls=[];self.instance=None;self.bad=None
        outer=self
        class Stage:
            @classmethod
            def from_safetensors(cls,root,**kw):
                outer.loads.append((root,kw));s=cls();s.stage_id=kw['stage_id'];s.layer_range=(0,1) if s.stage_id==0 else (1,3)
                s.meta={'hidden':24,'vocab':32,'layers':3,'positions':16};s.device=types.SimpleNamespace(type='cpu');s.cache_length=0
                outer.instance=s;return s
            def prefill(self,value,*,token_start,stage_id):
                outer.calls.append((value,token_start,stage_id));self.cache_length+=value.shape[1]
                width=24 if stage_id==0 else 32
                shape=(1,value.shape[1],width)
                result=Tensor([.125]*(value.shape[1]*width),shape=shape)
                if outer.bad=='shape':result.shape=(1,1,width)
                if outer.bad=='nan':result.values[0]=float('nan')
                if outer.bad=='gpu':result.device.type='cuda'
                return result
        torch=types.SimpleNamespace(Tensor=Tensor,int64='int64',float32='float32',
            tensor=lambda values,*,dtype,device:Tensor(values,dtype,device),
            isfinite=lambda tensor:types.SimpleNamespace(all=lambda:all(math.isfinite(x) for x in tensor.values)))
        tree=ast.parse(SOURCE.read_text());nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_owned_cpu_prefill_bytes']
        self.assertEqual(len(nodes),1,'missing real private stage factory')
        env={'torch':torch,'TiledQwen2Stage':Stage}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),str(SOURCE),'exec'),env)
        self.factory=env['_owned_cpu_prefill_bytes']
        t,s,st,tiles=fixture();self.args=(t.model_copy(update={'state':State.PREPARING}),s,st,tiles)
    def make(self,stage=1,args=None):
        t,s,st,tiles=args or self.args
        return self.factory('not-read',expected_sha256='a'*64,expected_config_sha256='c'*64,
            task=t,session=s,stages=st,tiles=tiles,stage_id=stage)
    def test_actual_loader_arguments_and_bytes(self):
        op=self.make();result=op(b'\0'*192,token_start=0,stage_id=1)
        self.assertEqual(struct.unpack('<64f',result),(.125,)*64)
        self.assertEqual(self.loads[0][1],dict(expected_sha256='a'*64,expected_config_sha256='c'*64,stage_id=1,split_layer=1,device='cpu'))
        self.assertEqual(self.calls[0][0].shape,(1,2,24))
    def test_tokens_and_tail(self):
        op=self.make(0)
        for start,count in ((0,2),(2,2),(4,1)):
            out=op(struct.pack('<'+'q'*count,*([1]*count)),token_start=start,stage_id=0)
            self.assertEqual(len(out),count*24*4)
        self.assertEqual(self.instance.cache_length,5)
    def test_wrong_model_pin_before_loader(self):
        t,s,st,tiles=self.args
        with self.assertRaises(ValueError):self.make(args=(t.model_copy(update={'model_hash':'b'*64}),s,st,tiles))
        self.assertEqual(self.loads,[])
    def test_failed_output_after_kv_advance_is_permanent(self):
        for bad in ('shape','nan','gpu'):
            self.bad=bad;op=self.make();before=len(self.calls)
            with self.assertRaises(ValueError):op(b'\0'*192,token_start=0,stage_id=1)
            self.assertEqual(self.instance.cache_length,2)
            self.bad=None
            with self.assertRaises(ValueError):op(b'\0'*192,token_start=2,stage_id=1)
            self.assertEqual(len(self.calls),before+1)
    def test_bad_range_and_nonfinite_never_prefill(self):
        op=self.make()
        with self.assertRaises(ValueError):op(b'\0'*192,token_start=True,stage_id=1)
        self.assertEqual(self.calls,[])
        op=self.make()
        with self.assertRaises(ValueError):op(struct.pack('<48f',float('nan'),*([0.0]*47)),token_start=0,stage_id=1)
        self.assertEqual(self.calls,[])
    def test_actual_metadata_mismatch_before_call(self):
        real=self.factory.__globals__['TiledQwen2Stage'].from_safetensors
        def wrong(*a,**kw):
            s=real(*a,**kw);s.meta['hidden']=23;return s
        self.factory.__globals__['TiledQwen2Stage'].from_safetensors=wrong
        with self.assertRaises(ValueError):self.make()
        self.assertEqual(self.calls,[])
    def test_budget_rejected_before_loader(self):
        import copy
        t,s,st,tiles=self.args;tiles=copy.deepcopy(tiles)
        for tile in tiles:
            if tile['stage_id']==1:tile['output']['shape']=(1,tile['token_count'],9000)
        with self.assertRaises(ValueError):self.make(args=(t,s,st,tiles))
        self.assertEqual(self.loads,[])
    def test_serializer_failure_after_cache_advance_isolated(self):
        from unittest.mock import patch
        op=self.make()
        with patch('struct.pack',side_effect=RuntimeError('encoding')):
            with self.assertRaises(RuntimeError):op(b'\0'*192,token_start=0,stage_id=1)
        self.assertEqual(self.instance.cache_length,2)
        with self.assertRaises(ValueError):op(b'\0'*192,token_start=2,stage_id=1)
        self.assertEqual(len(self.calls),1)
    def test_concurrent_call_does_not_enter_prefill(self):
        op=self.make();entered=threading.Event();release=threading.Event();errors=[]
        original=self.instance.prefill
        def hold(*a,**kw):
            entered.set()
            if not release.wait(3):raise RuntimeError('barrier timeout')
            return original(*a,**kw)
        self.instance.prefill=hold
        def call():
            try:op(b'\0'*192,token_start=0,stage_id=1)
            except BaseException as e:errors.append(e)
        thread=threading.Thread(target=call);thread.start()
        try:
            self.assertTrue(entered.wait(3))
            with self.assertRaises(ValueError):op(b'\0'*192,token_start=0,stage_id=1)
        finally:release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertFalse(errors);self.assertEqual(len(self.calls),1)
    def test_real_owner_interface_ordinary_standin_only(self):
        import asyncio
        from cluster_p1.stage_ownership import StageOwner,StageBinding
        op=self.make();commits=[]
        async def scenario():
            owner=StageOwner(StageBinding('t','s',1,1,'a'*64,'a'*64),op,
                authorize=lambda spec:True,commit=lambda spec,result:commits.append((spec,result)))
            try:result=await owner.call(b'\0'*192,token_start=0,token_count=2)
            finally:await owner.close()
            self.assertEqual(len(result),256);self.assertEqual(len(commits),1)
        asyncio.run(scenario())
    def test_old_module_ast_unchanged(self):
        import hashlib
        tree=ast.parse(SOURCE.read_text())
        tree.body=[n for n in tree.body if not (isinstance(n,ast.FunctionDef) and n.name=='_owned_cpu_prefill_bytes')]
        self.assertEqual(hashlib.sha256(ast.dump(tree,include_attributes=False).encode()).hexdigest(),'4f2180a5e7b81b2c7dc2f9ce5aa862d043da28b2ac544b8e5159b0c2bc5298ad')
