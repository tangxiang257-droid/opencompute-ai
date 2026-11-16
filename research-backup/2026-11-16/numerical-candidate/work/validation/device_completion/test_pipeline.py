"""Existing A/B SQLite/byte path with real completion logic and fake device API.

Pinned loader's load and model factory are explicitly replaced; no weights,
Torch, forward or GPU. This verifies wiring, not production authorization.
"""
import unittest,struct
from validation.split_roles import test_operations as fixture
from validation.one_token.test_assembly import SyntheticLoader,SyntheticBackend
from validation.device_completion.test_provider import Runtime,Gates
from cluster_p1.private_one_token import PinnedStageLoader
from cluster_p1.device_completion import DeviceCompletion
class BytesRuntime(Runtime):
 def copy(self,tensor,device,count):self.events.append('copy');return [1.]*count
class Tests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=fixture.Tests.asyncSetUp
 asyncTearDown=fixture.Tests.asyncTearDown
 make_receiver=fixture.Tests.make_receiver
 resources_local=fixture.Tests.resources_local
 a=fixture.Tests.a
 b=fixture.Tests.b
 def factory(self,stage,loaded,spec,bridge):
  b=SyntheticBackend(stage,spec,bridge,self.calls);b._device='cuda:0';return b
 def components(self,spec,stage):
  loader=PinnedStageLoader({0:'unused',1:'unused'},{0:'cuda:0',1:'cuda:0'},'a'*64,'b'*64,'c'*64,1,{0:'1'*64,1:'2'*64})
  substitute=SyntheticLoader();loader.load=substitute.load
  r=BytesRuntime();g=Gates();p=DeviceCompletion(spec,stage=stage,device='cuda:0',torch_version='synthetic',authority=g,capacity=g,_runtime=r)
  return loader,p,r
 async def test_actual_local_AB_persistent_bytes_same_scopes(self):
  loader,p,r=self.components(self.spec,0);a=self.a(loader=loader,device=p)
  a.begin('A');await a('A');a.finish('A')
  value=self.sender.query(0)['output'];self.assertEqual(p.state,'FINISHED');self.assertNotEqual(p.worker,__import__('threading').get_ident())
  edge=self.receiver._receipt_edge
  from cluster_p1.envelope import Envelope
  frame=Envelope(self.spec.request,self.spec.session,1,0,0,(0,1),self.spec.model,edge.layout_version,(1,1,self.hidden),'float32',value)
  self.receiver.make_receiver().receive_bytes(frame.encode())
  from dataclasses import replace
  import hashlib
  actual=replace(self.spec,inputs=(hashlib.sha256(value).hexdigest(),))
  loader,q,s=self.components(actual,1);b=self.b(value,loader=loader,device=q)
  b.begin('B');await b('B');b.finish('B')
  self.assertEqual(q.state,'FINISHED');self.assertEqual(tuple(b.bridge.scopes),('B',));self.assertEqual(b.spool.path.read_bytes(),struct.pack('<f',1.)*self.spec.vocab)
 async def test_mismatched_loader_device_rejected_before_load(self):
  loader,p,r=self.components(self.spec,0);loader.args[1][0]='cuda:1';calls=[];loader.load=lambda *x:calls.append(x)
  a=self.a(loader=loader,device=p);a.begin('A')
  with self.assertRaises(Exception):await a('A')
  self.assertEqual(calls,[]);self.assertEqual(r.events,[])
 async def test_callback_loader_device_drift_rejected_at_last_check(self):
  from contextlib import contextmanager
  loader,p,r=self.components(self.spec,0);calls=[];loader.load=lambda *x:calls.append(x)
  @contextmanager
  def mutate(*args):
   loader.args[1][0]='cuda:1'
   yield None
  self.authority.hold_load=mutate
  a=self.a(loader=loader,device=p);a.begin('A')
  with self.assertRaises(Exception):await a('A')
  self.assertEqual(calls,[]);self.assertEqual(p.state,'UNKNOWN')
if __name__=='__main__':unittest.main()
