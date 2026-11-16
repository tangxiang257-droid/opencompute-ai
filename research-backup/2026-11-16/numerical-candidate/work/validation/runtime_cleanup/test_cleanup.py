"""Actual fixture ownership/SQLite cleanup order; no devices or networks."""
import asyncio,unittest
from unittest.mock import patch
from validation.installation_session.test_pipeline import Pipeline
from validation.authority_chain.test_chain import Chain
class Cleanup(Pipeline):
 async def shutdown(self):
  if getattr(self,'checked_cleanup',False):return await Chain.shutdown(self)
  self.checked_cleanup=True
  before=[(v.startup.store.closed,v.startup.runtime['resources'].closed) for v in self.launchers.values()]
  await Chain.shutdown(self)
  self.assertFalse(any(a or b for a,b in before),'stores/resources closed before tasks drained')
  self.assertTrue(all(t.done() for t in self.jobs))
  self.assertTrue(all(c._task.done() for c in self.channels))
  self.assertTrue(all(v._close_task is not None and v._close_task.done() and v._close_task.exception() is None for v in self.launchers.values()))
  for v in self.launchers.values():
   self.assertTrue(v.startup.channel._task.done() and v.control._task.done() and v.startup.client._session.task.done())
   row=v.startup.runtime['resources'].db.execute('select charge,state from promises').fetchall()
   self.assertEqual(row,[(2097152,'UNKNOWN')])
  expected=getattr(self,'expected_cleanup_error',None)
  self.assertEqual(self.cleanup_errors,[] if expected is None else [expected])
  if hasattr(self,'b_operation'):
   for book in (self.b_operation.spool.source,self.b_operation.spool.destination):
    rows=book.db.execute('select charge,state,retired from reservation').fetchall()
    self.assertEqual(rows,[(33168,'ISOLATED',1)])
 async def test_automatic_cleanup_keeps_dependencies_live_until_join(self):await self.run_chain()
 async def test_cleanup_error_is_recorded_without_skipping_other_owners(self):
  await self.run_chain();owner=self.owned[-1];old=owner.close;fault=OSError('LAB post-close error')
  self.expected_cleanup_error=fault
  async def fail():await old();raise fault
  with patch.object(owner,'close',fail):await self.shutdown()
  self.assertIn(fault,getattr(self,'cleanup_errors',[]),'cleanup failure was silently discarded')
 async def test_cancelled_cleanup_keeps_dependencies_until_owned_join(self):
  await self.run_chain();entered=asyncio.Event();release=asyncio.Event();old=self.shutdown
  async def pause():entered.set();await release.wait();await old()
  with patch.object(self,'shutdown',pause):
   closing=asyncio.create_task(self._cleanup_runtime());await asyncio.wait_for(entered.wait(),1)
   closing.cancel();checkpoint=asyncio.Event();asyncio.get_running_loop().call_soon(checkpoint.set);await checkpoint.wait()
   closing.cancel();self.assertFalse(closing.done())
   self.assertTrue(all(not v.startup.store.closed and not v.startup.runtime['resources'].closed for v in self.launchers.values()))
   release.set();await asyncio.gather(closing,return_exceptions=True)
  self.assertTrue(self._runtime_cleanup_task.done());self.assertEqual(self.cleanup_errors,[])
  self.assertTrue(all(v.startup.store.closed and v.startup.runtime['resources'].closed for v in self.launchers.values()))

class UnknownCleanup(unittest.IsolatedAsyncioTestCase):
 async def test_shutdown_early_failure_retains_live_dependency(self):
  import sqlite3
  entered=asyncio.Event();release=asyncio.Event();closed=[]
  async def owner():entered.set();await release.wait()
  job=asyncio.create_task(owner());await entered.wait()
  db=sqlite3.connect(':memory:');db.execute('create table charge(value integer)');db.execute('insert into charge values(7)')
  def close():closed.append(True);db.close()
  c=Chain('runTest');c.jobs=[job];c.channels=[];c.cleanup_errors=[];c._dependency_closers=[close];c._runtime_cleanup_task=None
  async def failed():raise RuntimeError('LAB shutdown failed before join')
  c.shutdown=failed
  try:
   with self.assertRaises(BaseExceptionGroup):await c._cleanup_runtime()
   self.assertFalse(job.done());self.assertFalse(closed);self.assertEqual(db.execute('select value from charge').fetchall(),[(7,)])
  finally:release.set();await job;db.close()
 async def test_recorded_unknown_is_raised_not_silently_accepted(self):
  closed=[];c=Chain('runTest');c.jobs=[];c.channels=[];c.cleanup_errors=[];c._dependency_closers=[lambda:closed.append(True)];c._runtime_cleanup_task=None
  async def failed():c.cleanup_errors.append(OSError('LAB cleanup failure'))
  c.shutdown=failed
  with self.assertRaises(BaseExceptionGroup):await c._cleanup_runtime()
  self.assertFalse(closed)
 async def test_cancelled_owner_cleanup_cause_is_retained(self):
  entered=asyncio.Event();closed=[]
  async def owner():
   entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError:raise asyncio.CancelledError('cancelled with cleanup failure') from OSError('LAB cleanup write failed')
  job=asyncio.create_task(owner());await entered.wait()
  c=Chain('runTest');c.jobs=[job];c.owned=[];c.channels=[];c.cleanup_errors=[];c._dependency_closers=[lambda:closed.append(True)];c._runtime_cleanup_task=None
  with self.assertRaises(BaseExceptionGroup):await c._cleanup_runtime()
  self.assertTrue(job.done());self.assertFalse(closed)

def load_tests(loader,tests,pattern):
 return unittest.TestSuite([Cleanup(n) for n in Cleanup.__dict__ if n.startswith('test_')]+[UnknownCleanup(n) for n in UnknownCleanup.__dict__ if n.startswith('test_')])
if __name__=='__main__':unittest.main()
