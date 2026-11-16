"""Actual Channel fences over LAB memory TLS metadata; no real node identity."""
import asyncio,copy,unittest
from dataclasses import asdict,replace
from cluster_p1.channel import Channel,Grant
from validation.role_bridge.test_transport import streams,config

class Cases(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.channels=[];self.c,self.other=self.pair();await asyncio.gather(self.c.start(),self.other.start())
  self.scope=dict(command='command',reservation='rid',charge=1,binding={'task':'task'})
 def pair(self):
  r,w,s,v=streams('issuer','a');g=Grant('task','1'*32,1,0,(0,1),'a'*64,2)
  c=Channel(r,w,config('a'),{'a':{g}});other=Channel(s,v,config('issuer'),{'issuer':{g}});self.channels.extend((c,other));return c,other
 def view(self):
  from cluster_p1.scoped_identity import ScopedChannelIdentity
  return ScopedChannelIdentity(self.c,self.scope,expected_peer=asdict(self.c.authenticated_identity))
 async def asyncTearDown(self):await asyncio.gather(*(c.close() for c in self.channels))
 async def test_exact_scope_and_peer(self):
  view=self.view()
  with view.hold(copy.deepcopy(self.scope),self.c.authenticated_identity) as value:self.assertIsNone(value)
  self.assertFalse(view.retired)
 async def test_scope_change_retires_without_closing_borrowed(self):
  view=self.view();bad=copy.deepcopy(self.scope);bad['binding']['task']='other'
  with self.assertRaises(Exception):
   with view.hold(bad,self.c.authenticated_identity):self.fail('entered')
  self.assertTrue(view.retired);self.assertFalse(self.c._closed)
  with self.assertRaises(Exception):
   with view.hold(self.scope,self.c.authenticated_identity):self.fail('restored')
 async def test_original_generation_only(self):
  view=self.view();new,_=self.pair()
  self.assertEqual(new.authenticated_identity.node_id,self.c.authenticated_identity.node_id)
  with self.assertRaises(Exception):
   with view.hold(self.scope,new.authenticated_identity):self.fail('replayed')
 async def test_revocation_inside_yield_denies_exit(self):
  view=self.view();entered=[]
  with self.assertRaises(Exception):
   with view.hold(self.scope,self.c.authenticated_identity):entered.append(1);self.c.revoke_identity()
  self.assertEqual(entered,[1]);self.assertTrue(view.retired)
 async def test_wrong_expected_identity_refuses_construction(self):
  from cluster_p1.scoped_identity import ScopedChannelIdentity
  peer=asdict(self.c.authenticated_identity);peer['certificate_sha256']='0'*64
  with self.assertRaises(Exception):ScopedChannelIdentity(self.c,self.scope,expected_peer=peer)
 async def test_scope_snapshot_not_mutable(self):
  view=self.view();original=copy.deepcopy(self.scope);self.scope['binding']['task']='altered'
  with view.hold(original,self.c.authenticated_identity):pass
 async def test_body_failure_retires_view(self):
  view=self.view()
  with self.assertRaises(RuntimeError):
   with view.hold(self.scope,self.c.authenticated_identity):raise RuntimeError('ordinary failure')
  self.assertTrue(view.retired);self.assertFalse(self.c._closed)

if __name__=='__main__':unittest.main()
