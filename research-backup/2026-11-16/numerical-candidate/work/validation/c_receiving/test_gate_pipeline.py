"""Actual installed B, C ingress and memory Channels; all external policies LAB."""
import asyncio,unittest
from unittest.mock import patch
from validation.role_assembly.test_pipeline import Tests as Base
from cluster_p1.c_dispatch_gate import BDispatchGate
class Stop(Exception):pass
class Tests(Base):
 async def dispatch(self,role,coordinator):
  if role.startup.stage==0:return await super().dispatch(role,coordinator)
  self.brole=role
  if self.mode=='missing':role.role._service._receiver_gate=None
  elif self.mode=='active-only':self.gate_c_store.move(self.gate_c_role.d,self.gate_c_role.original,('RECEIVING',),'ACTIVE')
  elif self.mode=='wrong-input':self.gate_b.binding['input_digest']='0'*64
  elif self.mode=='missing-capacity':self.gate_c_role.resources=None
  elif self.mode=='missing-B-identity':self.gate_b.identity=None
  elif self.mode=='unknown-commit':
   old=self.gate_c_store._commit
   def commit(c):
    old(c);self.gate_c_store._commit=old;raise OSError('C gate commit response lost')
   self.gate_c_store._commit=commit
  elif self.mode=='after-consume':
   old=role.journal.consume
   def consume(*a):r=old(*a);self.gate_b.channel._abort();return r
   role.journal.consume=consume
  elif self.mode in ('after-ack','C-close','control-eof'):
   old=self.gate_b.acquire
   async def acquire(*a):
    await old(*a)
    if self.mode=='after-ack':await self.gate_b.close()
    elif self.mode=='C-close':
     await self.gate_c_role.close()
     await asyncio.gather(self.gate_b.channel._task,return_exceptions=True)
    else:
     # No role.serve_once or caller role.close: original Channel termination
     # must itself invalidate the independent ACK channel.
     self.gate_c_role.channel.r.feed_eof()
     await asyncio.gather(self.gate_c_role.channel._task,return_exceptions=True)
     await asyncio.gather(self.gate_b.channel._task,return_exceptions=True)
   self.gate_b.acquire=acquire
  if self.mode=='missing':
   # Bypass CoordinatorControl.dispatch entirely; raw authenticated execute.
   d=coordinator.descriptor
   _,self.result=await asyncio.gather(role.serve_once(),coordinator._client.request('execute',d.command_id,d.reservation_id,d.binding,d.charge))
  else:self.result=await super().dispatch(role,coordinator)
  if self.mode!='good':raise Stop()
  return self.result
 async def failure(self,mode):
  self.mode=mode;await self.online_bootstrap()
  with self.assertRaises(Stop):await asyncio.wait_for(self.run_split(),10)
  self.assertEqual(self.role_loaders[1].calls,[])
  self.assertTrue(self.result['retired']);self.assertEqual(self.result['state'],'UNKNOWN')
  self.assertEqual(self.result['charge'],2097152)
 async def test_full_chain_requires_live_C_ack(self):
  self.mode='good';await self.online_bootstrap();await self.run_split()
  self.assertEqual(self.gate_c_store.history(self.spec.owner,self.spec.request)['state'],'RECEIVED')
  self.assertEqual(self.brole.journal._db.execute('SELECT state FROM control_receiving_gate').fetchall(),[('CONSUMED',)])
 async def test_raw_execute_without_gate_is_denied(self):await self.failure('missing')
 async def test_active_without_ingress_rejects(self):await self.failure('active-only')
 async def test_wrong_actual_input_rejects(self):await self.failure('wrong-input')
 async def test_missing_C_capacity_rejects(self):await self.failure('missing-capacity')
 async def test_missing_B_identity_rejects(self):await self.failure('missing-B-identity')
 async def test_C_commit_unknown_keeps_slot_and_charge(self):
  await self.failure('unknown-commit')
  self.assertEqual(self.gate_c_store.db.execute('SELECT count(*) FROM c_dispatch_gate').fetchone(),(1,))
 async def test_disconnect_after_ACK_before_consume_rejects(self):await self.failure('after-ack')
 async def test_disconnect_after_consume_before_body_rejects(self):await self.failure('after-consume')
 async def test_C_close_after_ACK_rejects_observed_disconnect(self):await self.failure('C-close')
 async def test_original_control_EOF_closes_independent_gate(self):await self.failure('control-eof')
 async def test_ack_waiting_writer_rechecks_C_revocation(self):
  self.mode='writer';old=self.receiving_gate;entered=asyncio.Event();release=asyncio.Event()
  async def install(*a):
   ri=await old(*a);gate=self.gate_c
   send=gate.send
   async def blocked(*args):
    await gate.channel._write.acquire();entered.set();return await send(*args)
   gate.send=blocked
   async def revoke():
    await entered.wait()
    with self.gate_c_role.gate:self.gate_c_role.receiving=False
    gate.channel._write.release();release.set()
   task=asyncio.create_task(revoke());self.addAsyncCleanup(lambda:asyncio.gather(task,return_exceptions=True))
   return ri
  self.receiving_gate=install
  await self.failure('writer');self.assertTrue(release.is_set())
  self.assertEqual(sum(frame[:1]==b'D' for frame in self.gate_c.channel.w.frames),0)
if __name__=='__main__':
 r=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_')));raise SystemExit(not r.wasSuccessful())
