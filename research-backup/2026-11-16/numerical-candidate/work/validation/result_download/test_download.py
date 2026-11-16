"""Ordinary finite bytes + SQLite + in-memory ASGI; no TLS/server/Worker/model."""
import asyncio,base64,hashlib,importlib.abc,importlib.util,json,os,sqlite3,struct,sys,tempfile,unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name.split('.')[0] in {'torch','transformers','safetensors','tensorflow','public_pool'} or name.endswith('.worker'):raise AssertionError('offline import denied')
def guard(event,args):
 if event in ('socket.bind','socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'):raise AssertionError('offline event denied: '+event)
sys.meta_path.insert(0,Block());sys.addaudithook(guard)
spec=importlib.util.spec_from_file_location('pure_result_fixture',ROOT/'validation/controlled_authority/fixtures.py');f=importlib.util.module_from_spec(spec);spec.loader.exec_module(f)
import noncompute_application_service as app
import controlled_delivery_authority as control
import task_input_api as identity
from cluster_p1.channel import TLSConfig
try:import result_download_api as api
except ImportError:api=None
class Users:
 def __init__(self):self.revoked=False;self.subject='alice';self.calls=[];self.authority=None
 def verify(self,token):
  if self.authority:self.calls.append(self.authority._lock.locked())
  if self.revoked:return None
  return identity.Principal('local-download-test',self.subject if token=='alice-token' else 'bob') if token in ('alice-token','bob-token') else None
class Plans:
 def plan_for(self,task,stored):return f.encode(f.bundle(task.task_id))
class Checks(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.assertIsNotNone(api,'unmounted download API missing')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);self.fds=[]
  for name in ('db','inputs','results'):(self.root/name).mkdir(mode=0o700)
  for path in (self.root,self.root/'inputs',self.root/'results'):
   fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY);self.fds.append(fd);self.addCleanup(os.close,fd)
  self.users=Users();self.cfg=TLSConfig('not-read','not-read','not-read',{'unused':'a'*64})
  self.service=app.open_service(f.b.PathTrust(self.fds[0],('db',)),self.fds[1],self.fds[2],f.L,tls=self.cfg,recipient='unused',deployment=control.ControlledDeployment(True),verifier=self.users,local_plans=Plans(),initialize=True,enable_unified=True)
  self.users.authority=self.service._authority
  self.owner=identity._owner(identity.Principal('local-download-test','alice'))
  self.raw=json.dumps(dict(request_id='request',model_digest=f.c._digest(f.MODEL),preprocessing_version='prep',preprocessing_sha256='b'*64,vocab_size=100,token_ids=[1,2,3])).encode()
  task=await self.service.create_input('alice-token','input-key',self.raw);self.task=task.task_id
  self.binding=await self.service.bind_plan('alice-token',self.task)
  self.payloads=(struct.pack('<f',1.0)*200,struct.pack('<f',2.0)*100)
  self.entries=((0,2,hashlib.sha256(self.payloads[0]).hexdigest()),(2,1,hashlib.sha256(self.payloads[1]).hexdigest()))
  self.ledger=self.service._authority._store._managed._ledger
  async with self.service._authority._lock:
   self.digest=self.ledger.publish(self.owner,self.task,'internal-upload-key','prefill_logits_v1',self.binding.layout_digest,self.entries,tuple(f.chunks(raw) for raw in self.payloads))
  self.application=api.create_app(service=self.service)
 async def asyncTearDown(self):
  if hasattr(self,'service'):await self.service.close()
 async def request(self,*,token='alice-token',task=None,application=None,extra=(),path=None,query=b'',body=b'',method='GET',before_send=None):
  messages=[]
  scope={'type':'http','method':method,'path':path or '/tasks/'+(task or self.task)+'/result','query_string':query,'headers':[(b'authorization',('Bearer '+token).encode()),*extra]}
  async def receive():return {'type':'http.request','body':body,'more_body':False}
  async def send(message):
   if before_send:before_send(message)
   messages.append(message)
  await (application or self.application)(scope,receive,send)
  return messages
 def payload(self,msgs):return json.loads(msgs[1]['body'])
 async def test_complete_download_preserves_tiles_shape_no_owner(self):
  before=self.ledger._db.total_changes;messages=await self.request();self.assertEqual(messages[0]['status'],200);data=self.payload(messages)
  self.assertEqual(data['contract'],'prefill_logits_v1');self.assertEqual(data['manifest_digest'],self.digest)
  self.assertFalse(data['computation_verified']);self.assertFalse(data['execution_allowed']);self.assertEqual(data['dtype'],'float32-le')
  self.assertEqual([x['shape'] for x in data['tiles']],[[1,2,100],[1,1,100]])
  self.assertEqual(tuple(base64.b64decode(x['data_base64']) for x in data['tiles']),self.payloads)
  self.assertNotIn('owner',data);self.assertNotIn('assignments',data);self.assertNotIn('internal-upload-key',repr(data));self.assertEqual(before,self.ledger._db.total_changes)
  self.assertIn(True,self.users.calls);self.assertEqual((await self.request())[1]['body'],messages[1]['body'])
 async def test_foreign_missing_unfinished_same_safe_error_no_poison(self):
  self.assertEqual((await self.request(token='bob-token'))[0]['status'],404)
  self.assertEqual((await self.request(task='0'*32))[0]['status'],404)
  raw=json.loads(self.raw);raw['request_id']='second';other=await self.service.create_input('alice-token','second-key',json.dumps(raw).encode())
  denied=await self.request(task=other.task_id);self.assertEqual(self.payload(denied),{'error':'result unavailable'});self.assertFalse(self.service._authority._poisoned)
  self.assertEqual((await self.request())[0]['status'],200)
 async def test_default_denies(self):self.assertEqual((await self.request(application=api.create_app()))[0]['status'],404)
 async def test_no_owner_admin_worker_or_browser_substitute(self):
  for key in (b'x-owner-id',b'x-admin-key',b'x-worker-token',b'cookie',b'origin'):
   self.assertEqual((await self.request(extra=((key,b'alice'),)))[0]['status'],404)
  for token in ('admin','worker','invalid'):
   self.assertEqual((await self.request(token=token))[0]['status'],404)
 async def test_strict_request(self):
  for args in ({'query':b'owner=alice'},{'body':b'{}'},{'method':'POST'},{'extra':((b'range',b'bytes=0-1'),)},{'extra':((b'authorization',b'Bearer alice-token'),)}):
   self.assertEqual((await self.request(**args))[0]['status'],404)
 async def test_revoked_while_waiting_lock_no_output(self):
  seen=asyncio.Event();old=self.users.verify
  def verify(token):value=old(token);seen.set();return value
  self.users.verify=verify
  async with self.service._authority._lock:
   pending=asyncio.create_task(self.request());await seen.wait();self.users.revoked=True
  messages=await pending;self.assertEqual(messages[0]['status'],404);self.assertFalse(self.service._authority._poisoned)
 async def test_principal_changed_while_waiting_lock(self):
  seen=asyncio.Event();old=self.users.verify
  def verify(token):value=old(token);seen.set();return value
  self.users.verify=verify
  async with self.service._authority._lock:
   pending=asyncio.create_task(self.request());await seen.wait();self.users.subject='bob'
  self.assertEqual((await pending)[0]['status'],404);self.assertFalse(self.service._authority._poisoned)
 async def test_size_refusal_not_poison(self):
  limited=api.create_app(service=self.service,max_payload_bytes=1000)
  self.assertEqual((await self.request(application=limited))[0]['status'],404);self.assertFalse(self.service._authority._poisoned)
  self.assertEqual((await self.request())[0]['status'],200)
 async def test_corrupt_file_no_partial_response(self):
  writer=self.ledger._db.execute('SELECT writer_id FROM artifact_tickets').fetchone()[0];path=self.root/'results'/writer/'1.bin'
  os.chmod(path,0o600);path.write_bytes(b'x'*len(self.payloads[1]));os.chmod(path,0o400)
  messages=await self.request();self.assertEqual(len(messages),2);self.assertEqual(messages[0]['status'],404);self.assertEqual(self.payload(messages),{'error':'result unavailable'});self.assertTrue(self.service._authority._poisoned)
 async def test_old_fetch_and_read_result_compatible(self):
  chunks=[];self.assertEqual(self.ledger.fetch(self.owner,self.task,lambda tile,b:chunks.append((tile,b))),self.digest)
  data=[];self.assertEqual(await self.service.read_result('alice-token',self.task,'internal-upload-key',lambda tile,b:data.append((tile,b))),self.digest)
  self.assertEqual(chunks,data)
 async def test_cancel_waiting_has_no_response_or_mutation(self):
  seen=asyncio.Event();old=self.users.verify
  def verify(token):value=old(token);seen.set();return value
  self.users.verify=verify;before=self.ledger._db.total_changes
  async with self.service._authority._lock:
   pending=asyncio.create_task(self.request());await seen.wait();pending.cancel()
   with self.assertRaises(asyncio.CancelledError):await pending
  self.assertEqual(before,self.ledger._db.total_changes);self.assertFalse(self.service._authority._poisoned)
 async def test_post_lock_revocation_prevents_even_provisional_reads(self):
  seen=asyncio.Event();old=self.users.verify;collected=[]
  def verify(token):value=old(token);seen.set();return value
  self.users.verify=verify
  async with self.service._authority._lock:
   pending=asyncio.create_task(self.service.download_result('alice-token',self.task,lambda *x:collected.append(x),max_bytes=4194304,metadata=lambda x:None))
   await seen.wait();self.users.revoked=True
  with self.assertRaises(app.ServiceRejected):await pending
  self.assertEqual(collected,[]);self.assertFalse(self.service._authority._poisoned)
 async def test_post_fetch_and_final_send_revalidation(self):
  original=self.ledger.fetch
  def revoke(*a,**kw):
   result=original(*a,**kw);self.users.revoked=True;return result
  with patch.object(self.ledger,'fetch',revoke):self.assertEqual((await self.request())[0]['status'],404)
  self.assertFalse(self.service._authority._poisoned);self.users.revoked=False
  original_download=self.service.download_result
  async def late(*a,**kw):
   result=await original_download(*a,**kw);self.users.revoked=True;return result
  with patch.object(self.service,'download_result',late):self.assertEqual((await self.request())[0]['status'],404)
  self.assertFalse(self.service._authority._poisoned)
 async def test_encoded_cap_and_attachment_headers(self):
  with patch.object(api,'RESPONSE_LIMIT',100):self.assertEqual((await self.request())[0]['status'],404)
  self.assertFalse(self.service._authority._poisoned)
  messages=await self.request();headers=dict(messages[0]['headers'])
  self.assertEqual(headers[b'content-length'],str(len(messages[1]['body'])).encode())
  self.assertEqual(headers[b'content-disposition'],b'attachment; filename="result.json"')
  self.assertEqual(headers[b'cache-control'],b'no-store');self.assertEqual(headers[b'x-content-type-options'],b'nosniff')
 async def test_send_outside_lock_and_failure_not_retried(self):
  calls=[]
  def fail(message):
   self.assertFalse(self.service._authority._lock.locked());calls.append(message);raise OSError('private send failure')
  with self.assertRaises(OSError):await self.request(before_send=fail)
  self.assertEqual(len(calls),1);self.assertFalse(self.service._authority._poisoned)
 async def test_late_file_failure_discards_provisional_bytes(self):
  old=self.ledger._read_file;reads=[]
  def late(directory,name,*args):
   reads.append(name)
   # _verify_files reads each file first; fail during final delivery second tile.
   if name=='1.bin' and reads.count(name)==2:raise OSError('private path failure')
   return old(directory,name,*args)
  with patch.object(self.ledger,'_read_file',late):
   messages=await self.request()
  self.assertEqual(messages[0]['status'],404);self.assertEqual(self.payload(messages),{'error':'result unavailable'})
  self.assertTrue(self.service._authority._poisoned)
 async def test_collector_cap_and_metadata_refusal_do_not_poison(self):
  import result_artifacts as artifacts
  def refuse(*args):raise artifacts.DownloadRefused()
  with self.assertRaises(app.ServiceRejected):await self.service.download_result('alice-token',self.task,refuse,max_bytes=4194304,metadata=lambda x:None)
  self.assertFalse(self.service._authority._poisoned)
  with self.assertRaises(app.ServiceRejected):await self.service.download_result('alice-token',self.task,lambda *x:None,max_bytes=4194304,metadata=refuse)
  self.assertFalse(self.service._authority._poisoned)
 async def test_awaitable_verifiers_never_run_or_cancel(self):
  import inspect
  ran=[];created=[]
  async def bad():ran.append(True);return identity.Principal('local-download-test','alice')
  def verifier(token):value=bad();created.append(value);return value
  self.users.verify=verifier
  self.assertEqual((await self.request())[0]['status'],404)
  self.assertEqual(ran,[]);self.assertTrue(all(inspect.getcoroutinestate(v)=='CORO_CLOSED' for v in created))
  future=asyncio.get_running_loop().create_future();self.users.verify=lambda token:future
  self.assertEqual((await self.request())[0]['status'],404);self.assertFalse(future.cancelled());self.assertFalse(future.done())
  class Other:
   def __await__(self):raise AssertionError('must not await')
  self.users.verify=lambda token:Other()
  self.assertEqual((await self.request())[0]['status'],404)
 async def test_manifest_limit_precedes_file_read(self):
  with patch.object(self.ledger,'_verify_files',side_effect=AssertionError('must not read')):
   self.assertEqual((await self.request(application=api.create_app(service=self.service,max_payload_bytes=1000)))[0]['status'],404)
  self.assertFalse(self.service._authority._poisoned)
 async def test_v12_plan_deny_and_trigger_remain_byte_identical(self):
  # Fixed reviewed source pin; no dependency on a private sibling checkout.
  self.assertEqual(hashlib.sha256((ROOT/'text_input_ledger.py').read_bytes()).hexdigest(),
                   'd6822e534111d1ba3e3c20cac51ca101e22c929c301cdbadac0a379a07afbb97')
  import text_input_ledger as text
  with self.assertRaises(control.Denied):text.TextInputLedger.bind_plan(object(),'owner','task',b'raw')
  self.assertIn('text_source_no_plan',(ROOT/'text_input_ledger.py').read_text())
 async def test_suspended_coroutine_not_closed_by_verifier_guard(self):
  import inspect
  cleanup=[]
  class Suspend:
   def __await__(self):yield None
  async def creator_owned():
   try:await Suspend()
   finally:cleanup.append(True)
  coroutine=creator_owned();coroutine.send(None)
  self.assertEqual(inspect.getcoroutinestate(coroutine),'CORO_SUSPENDED')
  self.users.verify=lambda token:coroutine
  try:
   self.assertEqual((await self.request())[0]['status'],404)
   self.assertEqual(cleanup,[])
   self.assertEqual(inspect.getcoroutinestate(coroutine),'CORO_SUSPENDED')
  finally:coroutine.close()
  self.assertEqual(cleanup,[True])
if __name__=='__main__':unittest.main(verbosity=2)
