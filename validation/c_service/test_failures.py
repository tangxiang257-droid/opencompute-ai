import asyncio,unittest
from contextlib import contextmanager
from unittest.mock import patch
from validation.c_service.test_service import Flow as Base
class Finished(Exception):pass
class Failures(Base):
 async def deliver_c_result(self,r,control,sender,actual,spool,op,d):
  touched=[];original=lambda:self.cp.hold(d,'receive')
  charge=r.role.store.history(d['owner'],d['task'])['charge']
  if self.mode=='concurrent':
   entered=asyncio.Event()
   async def waiting():entered.set();await asyncio.Event().wait()
   with patch.object(r.gate,'serve_once',waiting):
    job=asyncio.create_task(r.serve_result_once(self.artifacts,lambda:None,original,enabled=True));await entered.wait()
    for call in (r.serve_gate(),r.receive_result(self.artifacts,lambda:None,original),r.serve_result_once(self.artifacts,lambda:None,original,enabled=True)):
     with self.assertRaises(Exception):await call
    job.cancel()
    with self.assertRaises(asyncio.CancelledError):await job
   row=r.role.store.history(d['owner'],d['task']);self.assertEqual(row['state'],'UNKNOWN');self.assertEqual(row['charge'],charge)
   raise Finished()
  if self.mode=='close-failure':
   marker=OSError('LAB close uncertain');close=r.role.close
   async def broken_close():await close();raise marker
   async def denied():raise ValueError('LAB ACK failed')
   with patch.object(r.gate,'serve_once',denied),patch.object(r.role,'close',broken_close):
    with self.assertRaises(BaseException) as caught:await r.serve_result_once(self.artifacts,lambda:None,original,enabled=True)
   self.expected_cleanup_error=r._close_task.exception();self.assertIsNotNone(self.expected_cleanup_error)
   pending=[caught.exception];seen=set();found=False
   while pending:
    e=pending.pop()
    if id(e) in seen:continue
    seen.add(id(e));found|=e is marker
    pending.extend(x for x in (e.__cause__,e.__context__) if x is not None)
    if isinstance(e,BaseExceptionGroup):pending.extend(e.exceptions)
   self.assertTrue(found);self.assertEqual(r.role.store.history(d['owner'],d['task'])['charge'],charge)
   raise Finished()
  async def gate():touched.append(True);raise Finished()
  @contextmanager
  def fence():
   with original():yield None
   if self.mode=='remove-hook':r.gate._receive_preflight=r.gate._receive_preflight_pin=None
  if self.mode=='cancelled-task':self.artifacts.cancel(actual.owner,actual.request)
  if self.mode=='disabled':
   with self.assertRaises(Exception):await r.serve_result_once(self.artifacts,lambda:None,fence)
  else:
   with patch.object(r.gate,'serve_once',gate):
    with self.assertRaises(Exception):await r.serve_result_once(self.artifacts,lambda:None,fence,enabled=True)
  self.assertEqual(touched,[],'invalid preflight reached ACK service')
  self.assertEqual(self.calls[1],[])
  raise Finished()
 async def test_cancelled_source_before_ACK(self):
  self.mode='cancelled-task'
  with self.assertRaises(Finished):await self.run_chain()
 async def test_provider_exit_cannot_remove_ACK_guard(self):
  self.mode='remove-hook'
  with self.assertRaises(Finished):await self.run_chain()
 async def test_default_refuses(self):
  self.mode='disabled'
  with self.assertRaises(Finished):await self.run_chain()
 async def test_concurrent_entries_and_cancel_retain_charge(self):
  self.mode='concurrent'
  with self.assertRaises(Finished):await self.run_chain()
 async def test_close_failure_preserves_evidence_and_charge(self):
  self.mode='close-failure'
  with self.assertRaises(Finished):await self.run_chain()
def load_tests(*args):return unittest.TestSuite(Failures(n) for n in Failures.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
