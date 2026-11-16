import pathlib,sys,importlib.util,unittest,asyncio,dataclasses
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'work'))
spec=importlib.util.spec_from_file_location('captured_fixture',ROOT/'work/validation/result_download/test_download.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
from local_response_observation import ObservationError
class Tests(m.Checks):
 def candidate(self):
  self.assertIsNotNone(importlib.util.find_spec('captured_prefill'),'missing private composition')
  import captured_prefill
  return captured_prefill
 def scope(self):return {'type':'http','method':'GET','path':'/tasks/'+self.task+'/result','query_string':b'','headers':[(b'authorization',b'Bearer alice-token')]}
 async def receive(self):return {'type':'http.request','body':b'','more_body':False}
 async def invoke(self,**kw):
  c=self.candidate();args=dict(service=self.service,scope=self.scope(),receive=self.receive,reference=b''.join(self.payloads),reference_shape=(1,3,100),atol=0,rtol=0);args.update(kw)
  return await c._observe_selected_prefill(**args)
 async def test_one_capture_exact_body_and_checks(self):
  c=self.candidate();self.users.calls.clear()
  carrier=await m.api._observe_selected_download(service=self.service,scope=self.scope(),receive=self.receive);trace=tuple(self.users.calls);self.users.calls.clear()
  before=self.ledger._db.total_changes
  with patch.object(self.ledger,'fetch',wraps=self.ledger.fetch) as fetch:
   value=await self.invoke();self.assertEqual(fetch.call_count,1)
  self.assertEqual(value.association.observation.body,carrier.observation.body);self.assertEqual(value.association.selection,carrier.selection)
  self.assertEqual(tuple(self.users.calls),trace);self.assertEqual(value.numeric.element_count,300);self.assertTrue(value.numeric.within_tolerance)
  self.assertEqual(before,self.ledger._db.total_changes)
  with self.assertRaises(dataclasses.FrozenInstanceError):value.numeric.element_count=0
 async def test_numeric_mismatch_not_verified(self):
  self.candidate();value=await self.invoke(reference=b'\0'*1200)
  self.assertFalse(value.numeric.within_tolerance);self.assertEqual(value.numeric.violation_count,300)
  self.assertFalse(hasattr(value,'verified'));self.assertEqual(value.association.selection.task,self.task)
 async def test_final_revoke_no_comparison(self):
  c=self.candidate();old=self.service.download_result
  async def revoked(*a,**k):
   value=await old(*a,**k);self.users.revoked=True;return value
  with patch.object(self.service,'download_result',revoked),patch.object(c,'observe_prefill_difference',wraps=c.observe_prefill_difference) as comparison:
   with self.assertRaises(ObservationError):await self.invoke()
   comparison.assert_not_called()
  self.assertFalse(self.service._authority._poisoned)
 async def test_comparison_failure_no_partial(self):
  self.candidate();before=self.ledger._db.total_changes
  with self.assertRaises(ValueError):await self.invoke(reference_shape=(1,1,300))
  self.assertEqual(before,self.ledger._db.total_changes);self.assertFalse(self.service._authority._poisoned)
 async def test_cancel_waiting_lock(self):
  self.candidate();seen=asyncio.Event()
  async def receive():seen.set();return await self.receive()
  async with self.service._authority._lock:
   task=asyncio.create_task(self.invoke(receive=receive))
   try:
    await seen.wait();task.cancel()
    with self.assertRaises(asyncio.CancelledError):await task
   finally:
    if not task.done():task.cancel()
    await asyncio.gather(task,return_exceptions=True)
 async def test_concurrent_call_local_association(self):
  self.candidate();second=Tests('test_numeric_mismatch_not_verified');seen=asyncio.Event();arrivals=[]
  async def run(instance,label):
   async def receive():
    arrivals.append(label)
    if len(arrivals)==2:seen.set()
    await seen.wait();return await instance.receive()
   return await instance.invoke(receive=receive,reference=b''.join(instance.payloads) if label=='a' else b'\0'*1200)
  tasks=[]
  try:
   await second.asyncSetUp()
   tasks=[asyncio.create_task(run(self,'a')),asyncio.create_task(run(second,'b'))];a,b=await asyncio.gather(*tasks)
   self.assertEqual((a.association.selection.task,b.association.selection.task),(self.task,second.task));self.assertNotEqual(a.association.observation.body,b.association.observation.body)
   self.assertTrue(a.numeric.within_tolerance);self.assertFalse(b.numeric.within_tolerance)
  finally:
   for t in tasks:
    if not t.done():t.cancel()
   await asyncio.gather(*tasks,return_exceptions=True)
   if hasattr(second,'service'):await second.asyncTearDown()
   second.doCleanups()
if __name__=='__main__':
 suite=unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_'))
 raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
