"""Prospective wrapper-bound RED; missing entry prevents downstream assertions."""
import importlib.util,pathlib,sys,unittest,asyncio,os,json,dataclasses
from unittest.mock import patch
ROOT=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'work'))
spec=importlib.util.spec_from_file_location('ordinary_base',ROOT/'work/validation/result_download/test_download.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
from local_response_observation import ObservationError
class WrapperRed(m.Checks):
 def candidate(self):
  function=getattr(m.api,'_observe_selected_download',None)
  self.assertTrue(callable(function),'MISSING_PRIVATE_WRAPPER: downstream association/denial assertions NOT_EXECUTED')
  return function
 async def invoke(self,function):
  scope={'type':'http','method':'GET','path':'/tasks/'+self.task+'/result','query_string':b'','headers':[(b'authorization',b'Bearer alice-token')]}
  async def receive():return {'type':'http.request','body':b'','more_body':False}
  return await function(service=self.service,scope=scope,receive=receive)
 async def test_same_fetch_exact_body(self):
  self.users.calls.clear();normal=await self.request();lock_trace=tuple(self.users.calls)
  self.assertEqual(normal[0]['status'],200)
  function=self.candidate()
  self.users.calls.clear();result=await self.invoke(function)
  self.assertEqual(result.observation.body,normal[1]['body'])
  self.assertEqual(tuple(self.users.calls),lock_trace)
  self.assertEqual(result.selection.task,self.task)
  self.assertEqual(result.selection.manifest_digest,self.digest)
  # Identity assertions use fixture oracle only, never inputs to the wrapper.
  # Actual same-fetch selection provenance still requires copy-site audit.
 async def test_real_final_revoke_no_association(self):
  function=self.candidate();original=self.service.download_result;revoked=[]
  async def late(*args,**kwargs):
   value=await original(*args,**kwargs);self.users.revoked=True;revoked.append(True);return value
  before=self.ledger._db.total_changes
  with patch.object(self.service,'download_result',late):
   with self.assertRaises(ObservationError):await self.invoke(function)
  self.assertEqual(revoked,[True])
  self.assertEqual(self.ledger._db.total_changes,before)
  self.assertFalse(self.service._authority._poisoned)

 async def test_real_cancel_waiting_lock(self):
  function=self.candidate();seen=asyncio.Event();old=self.users.verify
  def verify(token):
   value=old(token);seen.set();return value
  self.users.verify=verify;before=self.ledger._db.total_changes
  async with self.service._authority._lock:
   pending=asyncio.create_task(self.invoke(function));await seen.wait();pending.cancel()
   with self.assertRaises(asyncio.CancelledError):await pending
  self.assertEqual(self.ledger._db.total_changes,before)
  self.assertFalse(self.service._authority._poisoned)
 async def test_swallowed_real_cancel_before_normal_return(self):
  function=self.candidate()
  scope={'type':'http','method':'GET','path':'/tasks/'+self.task+'/result','query_string':b'','headers':[(b'authorization',b'Bearer alice-token')]}
  async def receive():
   asyncio.current_task().cancel()
   try:await asyncio.get_running_loop().create_future()
   except asyncio.CancelledError:pass
   return {'type':'http.request','body':b'','more_body':False}
  pending=asyncio.create_task(function(service=self.service,scope=scope,receive=receive))
  with self.assertRaises(asyncio.CancelledError):await pending
 async def test_real_file_corruption_preserves_poison(self):
  function=self.candidate()
  writer=self.ledger._db.execute('SELECT writer_id FROM artifact_tickets').fetchone()[0]
  path=self.root/'results'/writer/'1.bin';os.chmod(path,0o600);path.write_bytes(b'x'*len(self.payloads[1]));os.chmod(path,0o400)
  with self.assertRaises(ObservationError):await self.invoke(function)
  self.assertTrue(self.service._authority._poisoned)
 async def test_metadata_refusal_no_poison(self):
  function=self.candidate();original=self.ledger.fetch
  import result_artifacts
  def refuse(*args):raise result_artifacts.DownloadRefused()
  def fetch(*args,**kwargs):
   kwargs['metadata']=refuse
   return original(*args,**kwargs)
  with patch.object(self.ledger,'fetch',fetch):
   with self.assertRaises(ObservationError):await self.invoke(function)
  self.assertFalse(self.service._authority._poisoned)
 async def test_concurrent_distinct_tasks_do_not_join(self):
  function=self.candidate()
  second=m.Checks('test_complete_download_preserves_tiles_shape_no_owner')
  await second.asyncSetUp()
  ready=asyncio.Event();arrivals=[]
  try:
   async def capture(service,task,label):
    scope={'type':'http','method':'GET','path':'/tasks/'+task+'/result','query_string':b'','headers':[(b'authorization',b'Bearer alice-token')]}
    async def receive():
     arrivals.append(label)
     if len(arrivals)==2:ready.set()
     await ready.wait()
     return {'type':'http.request','body':b'','more_body':False}
    return await function(service=service,scope=scope,receive=receive)
   first,other=await asyncio.gather(capture(self.service,self.task,'a'),capture(second.service,second.task,'b'))
   self.assertEqual(set(arrivals),{'a','b'})
   self.assertEqual((first.selection.task,other.selection.task),(self.task,second.task))
   self.assertIsNot(first,other);self.assertNotEqual(first.observation.body,other.observation.body)
  finally:
   await second.asyncTearDown();second.doCleanups()
 async def test_immutable_private_whitelist(self):
  function=self.candidate();result=await self.invoke(function)
  expected={'task','request','session','epoch','model_claim','input_ref','token_sha256','preprocessing_version','preprocessing_sha256','dtype','axes','token_count','total_bytes','tiles','manifest_digest'}
  self.assertEqual({f.name for f in dataclasses.fields(result.selection)},expected)
  self.assertIs(type(result.selection.axes),tuple);self.assertIs(type(result.selection.tiles),tuple)
  self.assertIs(type(result.selection.tiles[0].shape),tuple)
  with self.assertRaises(dataclasses.FrozenInstanceError):result.selection.task='other'
  with self.assertRaises(dataclasses.FrozenInstanceError):result.selection.tiles[0].tile=8
  public=json.loads(result.observation.body)
  for field in ('owner','request','session','epoch','model_claim','input_ref','token_sha256','assignments'):
   self.assertNotIn(field,public)
if __name__=='__main__':
 names=[name for name in WrapperRed.__dict__ if name.startswith('test_')]
 result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(WrapperRed(n) for n in names));raise SystemExit(not result.wasSuccessful())
