import asyncio,unittest
from unittest.mock import patch
from validation.authority_chain.test_chain import Chain
class Tests(Chain):
 async def test_cancel_then_revoke_still_retires_original_scope(self):
  role,control=await self.prepare_a();op=self.a_operation
  self.assertEqual((await control.dispatch())['state'],'COMMITTED')
  loop=next(t for t in self.jobs if t.get_coro().__name__=='control_loop')
  original=op.completion.quarantine;entered=asyncio.Event();completed=asyncio.Event()
  def revoke_then_retire(binding,scope):
   entered.set();self.launchers[0].startup.channel.revoke_identity();value=original(binding,scope);completed.set();return value
  with patch.object(op.completion,'quarantine',revoke_then_retire):
   loop.cancel()
   try:await loop
   except BaseException as e:self.observed_business_outcomes.append(e)
  self.assertTrue(entered.is_set());self.assertTrue(completed.is_set(),'bound completion quarantine itself was blocked')
  self.assertTrue(op.retired,'original bound operation was not permanently retired after revocation')
  with op.sender._store._tx() as c:self.assertEqual(c.execute('SELECT retired FROM local_stage_calls').fetchone(),(1,))
  self.assertEqual(op.resources.query(op.ticket.reservation_id)['state'],'UNKNOWN')
  with self.assertRaises(Exception):op.ready()
  await self.settle_expected_role_failure()
 async def committed_operation(self):
  role,control=await self.prepare_a()
  self.assertEqual((await control.dispatch())['state'],'COMMITTED')
  return self.a_operation,role.descriptor.binding
 async def test_wrong_scope_and_binding_do_not_retire(self):
  op,binding=await self.committed_operation()
  with self.assertRaises(Exception):op.completion.quarantine(binding,'foreign-scope')
  bad=dict(binding);bad['owner']='foreign-owner'
  with self.assertRaises(Exception):op.completion.quarantine(bad,op.scope)
  self.assertFalse(op.retired)
 async def test_provider_swap_refuses_foreign_cleanup(self):
  op,binding=await self.committed_operation();original=op.resources;called=[]
  class Foreign:
   def quarantine(self,*args):called.append(True)
  op.resources=Foreign()
  try:
   with self.assertRaises(Exception):op.completion.quarantine(binding,op.scope)
   self.assertEqual(called,[])
  finally:op.resources=original
 async def test_cleanup_failure_still_retires_and_keeps_charge(self):
  op,binding=await self.committed_operation();marker=OSError('LAB device quarantine unknown')
  before=op.resources.query(op.ticket.reservation_id)
  with patch.object(op.bridge,'quarantine',side_effect=marker):
   with self.assertRaises(BaseExceptionGroup) as caught:op.completion.quarantine(binding,op.scope)
  self.assertIn(marker,caught.exception.exceptions);self.assertTrue(op.retired)
  after=op.resources.query(op.ticket.reservation_id)
  self.assertEqual(after['state'],'UNKNOWN')
  self.assertEqual(before['demand'],after['demand'])
  with self.assertRaises(Exception):op.ready()
 async def test_B_completed_scope_retires_after_install_revoke(self):
  await self.run_chain();op=self.b_operation
  books=(op.spool.source,op.spool.destination)
  charges=tuple(b.record(op.spool.rid)['charge'] for b in books)
  self.launchers[1].startup.channel.revoke_identity()
  op.completion.quarantine(op.descriptor.binding,op.scope)
  self.assertTrue(op.retired)
  self.assertEqual(tuple(b.record(op.spool.rid)['charge'] for b in books),charges)
  self.assertTrue(all(b.record(op.spool.rid)['retired'] for b in books))
  self.assertEqual(op.resources.query(op.ticket.reservation_id)['state'],'UNKNOWN')
  with self.assertRaises(Exception):op.check_export()
 async def test_callback_cannot_redirect_original_resource_retirement(self):
  op,binding=await self.committed_operation();resources=op.resources;seen=[]
  class Foreign:
   def quarantine(self,*args):seen.append(True)
  original=op.bridge.quarantine
  def changing(scope):
   original(scope);op.resources=Foreign()
  try:
   with patch.object(op.bridge,'quarantine',changing):
    with self.assertRaises(BaseExceptionGroup):op.completion.quarantine(binding,op.scope)
   self.assertEqual(seen,[])
   self.assertEqual(resources.query(op.ticket.reservation_id)['state'],'UNKNOWN')
  finally:op.resources=resources
 async def test_local_retire_cannot_redirect_original_device(self):
  op,binding=await self.committed_operation();device=op.device;seen=[];scope=op.scope
  original=op._retire_local;before=device.quarantined.count(scope)
  class Foreign:
   def quarantine(self,got):seen.append(got)
  def changing():
   original();op.bridge.device=Foreign()
  try:
   # Direct original terminal owner path: injected failure happens inside the
   # first retirement action, after responsibility snapshots are captured.
   with patch.object(op,'_retire_local',changing):
    with self.assertRaises(BaseExceptionGroup):op.quarantine()
   self.assertEqual(seen,[])
   self.assertEqual(device.quarantined.count(scope),before+1)
   self.assertTrue(op.bridge.retired)
  finally:op.bridge.device=device
def load_tests(*args):return unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
