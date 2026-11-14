"""Actual launcher ownership; ordinary event tasks, LAB installation providers."""
import asyncio,unittest
from validation.role_entry.test_entry import Flow
class Cleanup(Flow):
 async def probe(self,cause=None,implicit=False):
  launcher=self.launchers[0];entered=asyncio.Event()
  async def body():
   entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError as error:
    if cause is not None:
     if implicit:
      try:raise cause
      except BaseException:raise asyncio.CancelledError()
     raise error from cause
    raise
  job=asyncio.create_task(launcher._run(body));await entered.wait()
  if cause is None:
   await launcher.close();self.assertTrue(job.done());return
  caught=None
  try:await launcher.close()
  except BaseException as e:caught=e;self.expected_cleanup_error=e
  self.assertIsNotNone(caught,'launcher reported clean shutdown despite owned cleanup failure')
  self.assertTrue(job.done());self.assertIs(caught,launcher._close_task.exception())
  graph=[caught];seen=set();found=False
  while graph:
   e=graph.pop()
   if id(e) in seen:continue
   seen.add(id(e));found|=e is cause
   graph.extend(x for x in (e.__cause__,e.__context__) if x is not None)
   if isinstance(e,BaseExceptionGroup):graph.extend(e.exceptions)
  self.assertTrue(found,'original cleanup evidence lost')
 async def test_cancel_cause_must_fail_close(self):await self.probe(OSError('LAB device completion unknown'))
 async def test_nested_cleanup_failure_must_fail_close(self):await self.probe(BaseExceptionGroup('LAB cleanup',[asyncio.CancelledError(),OSError('LAB resource unknown')]))
 async def test_implicit_context_must_fail_close(self):await self.probe(OSError('LAB close unknown'),implicit=True)
 async def test_plain_cancellation_closes(self):await self.probe()
def load_tests(*args):return unittest.TestSuite(Cleanup(n) for n in Cleanup.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
