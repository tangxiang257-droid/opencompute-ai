"""Real consent/source/issuer storage; synthetic parent permissions and ordinary work."""
import asyncio,threading,unittest,json
from contextlib import contextmanager
from unittest.mock import patch
from validation.task_consent.test_consent import Tests as Base,Trigger
from validation.authority_resources.test_source import Permission
from cluster_p1.registered_input_source import RegisteredInputSource
from cluster_p1.online_issuance import IssuanceAuthority
from cluster_p1.projection_driver import Spec
from private_consent_runtime import IssuanceConsent
class Parent:
 def __init__(self):self.calls=[]
 @contextmanager
 def hold(self,*args):self.calls.append(('hold',args));yield None
 @contextmanager
 def hold_load(self,*args):self.calls.append(('load',args));yield None
 @contextmanager
 def hold_stage0(self,*args):self.calls.append(('stage0',args));yield None
 @contextmanager
 def hold_call(self,*args):self.calls.append(('call',args));yield None
 def finalize(self,spec,token,digest,commit):self.calls.append(('finalize',));commit()
class Tests(Base):
 async def asyncSetUp(self):
  await super().asyncSetUp();self.receipt=await self.active();self.parent=Parent()
  self.adapter=IssuanceConsent(self.consent,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,parent=self.parent)
  self.source=RegisteredInputSource(self.reader,identity_fence=self.local,consent=self.adapter)
  self.strict=IssuanceAuthority(self.root/'strict.db',namespace='e'*64,source_provider=self.source,authorize=self.adapter,resources=Permission(),require_consent=True);self.addCleanup(self.strict.close)
 async def register_strict(self):
  return await self.source.register(self.strict,self.token,self.ptask,self.session,self.stages,self.tiles,hidden_size=24,vocab_size=100,config_sha256=self.declaration['config_sha256'])
 def spec(self,stage=1):
  b=json.loads(self.ledger._db.execute('SELECT body FROM task_consents').fetchone()[0]);s=b['source'];d=b['declaration']
  return Spec(self.owner,self.task.task_id,self.session.session_id,1,self.ptask.model_hash,self.session.layout_hash,s['input_snapshot'],s['preprocessing'],d['plan_digest'],s['token_digest'],24,100,((s['token_digest'] if stage==0 else 'a'*64),))
 async def view(self,stage=1):
  await self.register_strict();s=self.spec(stage);v=await self.adapter.runtime(stage,s,parent=self.parent,maintenance=self.parent)
  binding=dict(owner=s.owner,task=s.request,session=s.session,epoch=s.epoch,stage=stage,model=s.model,layout=s.layout,plan=s.plan_digest,input_snapshot=s.input_snapshot,token_digest=s.token_digest,preprocessing=s.preprocessing,input_digest=s.inputs[0],device='cuda:0',instance='fresh-'+('a','b')[stage],declaration_digest=__import__('private_task_consent').digest(self.declaration),peer='b'*64)
  v.bind_control(binding);return v,s,binding
 async def test_complete_context_reaches_real_issuer_and_persists(self):
  r=await self.register_strict();request=self.parent.calls[-1][1][0]
  self.assertEqual(request['authorization']['body']['policy'],self.policy);self.assertEqual(request['source_binding']['input_snapshot'],self.ref.digest)
  self.assertEqual(json.loads(r.authorization),request['authorization']);self.assertEqual(self.strict.db.execute('SELECT context FROM authorizations').fetchone(),(r.authorization,))
 async def test_missing_parent_denies_registration(self):
  self.adapter.parent=None
  with self.assertRaises(Exception):await self.register_strict()
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM registrations').fetchone(),(0,))
 async def test_context_tamper_refuses_before_persist(self):
  original=self.adapter.context_locked
  calls=[]
  def bad(*args):
   v=original(*args);calls.append(1)
   if len(calls)==1:v['body']['policy']['privacy']='public'
   return v
  with patch.object(self.adapter,'context_locked',bad):
   with self.assertRaises(Exception):await self.register_strict()
 async def test_revoke_between_registration_callback_and_commit_refuses(self):
  original=self.parent.hold
  @contextmanager
  def late(*args):
   with original(*args):yield None
   self.consent.fence.invalidate(self.task.task_id)
  with patch.object(self.parent,'hold',late):
   with self.assertRaises(Exception):await self.register_strict()
  self.assertTrue(self.strict.retired);self.assertIsNone(self.adapter.registration)
 async def test_registration_commit_unknown_no_runtime(self):
  original=self.strict._tx
  @contextmanager
  def uncertain():
   with original() as c:yield c
   raise OSError('lost commit response')
  with patch.object(self.strict,'_tx',uncertain):
   with self.assertRaises(OSError):await self.register_strict()
  with self.assertRaises(Exception):await self.adapter.runtime(1,self.spec(),parent=self.parent,maintenance=self.parent)
 async def test_runtime_parent_and_context_both_required(self):
  v,s,b=await self.view()
  with v.hold(b,'execute'):pass
  self.assertTrue(any(x[0]=='hold' and x[1][-1]=='execute' for x in self.parent.calls))
  with self.assertRaises(Exception):
   with v.hold(dict(b,device='other'),'execute'):pass
 async def test_revoke_mid_body_allows_exit_but_denies_finalize(self):
  v,s,b=await self.view();entered=asyncio.Event();release=threading.Event();loop=asyncio.get_running_loop();out=[]
  def worker():
   try:
    with v.hold_call(s,0):loop.call_soon_threadsafe(entered.set);release.wait(5);out.append('body')
   except Exception:out.append('denied')
  thread=threading.Thread(target=worker);thread.start();await entered.wait()
  await self.consent.revoke(self.token,self.task.task_id);release.set();await asyncio.to_thread(thread.join,5)
  self.assertEqual(out,['body','denied']);self.assertFalse(thread.is_alive())
  with self.assertRaises(Exception):v.finalize(s,0,'a'*64,lambda:out.append('commit'))
  self.assertNotIn('commit',out)
 async def test_query_and_fail_after_revoke_do_not_restore_execute(self):
  v,s,b=await self.view();await self.consent.revoke(self.token,self.task.task_id)
  for a in ('query','fail'):
   with v.hold(b,a):pass
  with self.assertRaises(Exception):
   with v.hold(b,'execute'):pass
 async def test_task_cancel_invalidates_runtime_view(self):
  v,s,b=await self.view();await self.a.cancel(self.owner,self.task.task_id)
  with self.assertRaises(Exception):v.check('execute')
 async def test_identity_revoke_blocks_even_maintenance(self):
  v,s,b=await self.view();await self.local.revoke_subject('alice')
  with self.assertRaises(Exception):
   with v.hold(b,'query'):pass
 async def test_wrong_runtime_spec_never_calls_parent(self):
  from dataclasses import replace
  v,s,b=await self.view();before=len(self.parent.calls)
  with self.assertRaises(Exception):
   with v.hold_call(replace(s,inputs=('f'*64,)),0):pass
  self.assertEqual(len(self.parent.calls),before)
 async def test_finalize_one_shot_and_late_callback_rejected(self):
  v,s,b=await self.view();out=[];callbacks=[]
  def finalize(spec,token,digest,commit):callbacks.append(commit);commit()
  self.parent.finalize=finalize
  v.finalize(s,0,'e'*64,lambda:out.append('commit'));self.assertEqual(out,['commit'])
  with self.assertRaises(Exception):callbacks[0]()
  with self.assertRaises(Exception):v.finalize(s,0,'e'*64,lambda:out.append('again'))
 async def test_reopen_issuer_history_cannot_restore(self):
  await self.register_strict();self.strict.close()
  other=IssuanceAuthority(self.root/'strict.db',namespace='e'*64,source_provider=self.source,authorize=self.adapter,resources=Permission(),require_consent=True)
  try:
   with self.assertRaises(Exception):other.ready()
  finally:other.close()
 async def test_runtime_missing_parent_or_maintenance_denies(self):
  await self.register_strict()
  for p,m in ((None,self.parent),(self.parent,None)):
   with self.assertRaises(Exception):await self.adapter.runtime(1,self.spec(),parent=p,maintenance=m)
 async def test_actual_memory_issuance_claim_uses_complete_context(self):
  import hashlib
  from cluster_p1.channel import Channel,Grant
  from cluster_p1.online_issuance import RoleInstallStore
  from cluster_p1.online_issuance_transport import IssuanceClient,IssuanceService
  from validation.online_issuance.test_transport import Identity,Holds,Completion
  from validation.role_bridge.test_transport import streams,config,Writer
  r=await self.register_strict();d=self.strict.describe(r,0,node='a',device='cuda:0',instance='fresh-a',resource_id='held-a')
  h=Holds();identity=Identity();store=RoleInstallStore(self.root/'role.db',role_node='a',identity=h,authorize=h,resources=h,device=h);self.addCleanup(store.close)
  cr,cw,sr,sw=streams('a','issuer');g=Grant(self.task.task_id,self.session.session_id,1,0,(0,1),self.ptask.model_hash,2)
  cc=Channel(cr,cw,config('issuer'),{'issuer':{g}},capacity_bytes=32768,capacity_tiles=1);sc=Channel(sr,sw,config('a'),{'a':{g}},capacity_bytes=32768,capacity_tiles=1)
  kw=dict(issuer_node='issuer',issuer_certificate=hashlib.sha256(b'issuer').hexdigest(),identity=identity,authorize=h,resources=h)
  client=IssuanceClient(cc,store,d,completion=Completion(),**kw);server=IssuanceService(sc,self.strict,r,d,**kw)
  old=Writer.drain
  async def drain(w):self.assertFalse(self.a._lock.locked());self.assertEqual(identity.active,0);await old(w)
  try:
   with patch.object(Writer,'drain',drain):handle,response=await asyncio.gather(client.install(),server.serve_once())
   self.assertFalse(response['execution_allowed']);self.assertEqual(self.strict.db.execute('SELECT count(*) FROM slots').fetchone(),(1,))
   calls=[x for x in self.parent.calls if x[0]=='hold' and x[1][-1]=='claim'];self.assertEqual(len(calls),1);self.assertEqual(calls[0][1][0]['authorization']['consent_digest'],self.receipt.proposal_digest)
  finally:await asyncio.gather(client.close(),server.close(),return_exceptions=True)
 async def test_actual_control_service_executes_under_both_checks(self):
  from cluster_p1.channel import Channel,Grant
  from cluster_p1.control_journal import ControlJournal
  from cluster_p1.control_exchange import ControlService,ControlClient
  from cluster_p1.role_control import RegisteredRole,CoordinatorControl
  from validation.control_exchange.test_transport import Holds
  from validation.online_issuance.test_transport import Completion
  from validation.role_bridge.test_transport import streams,config
  await self.register_strict();s=self.spec();v=await self.adapter.runtime(1,s,parent=self.parent,maintenance=self.parent)
  cr,cw,sr,sw=streams('controller','b');g=Grant(s.request,s.session,1,1,(0,1),s.model,2)
  cc=Channel(cr,cw,config('b'),{'b':{g}},capacity_bytes=32768,capacity_tiles=1);sc=Channel(sr,sw,config('controller'),{'controller':{g}},capacity_bytes=32768,capacity_tiles=1)
  journal=ControlJournal(self.root/'control.db',4096,resources=Holds());self.addCleanup(journal.close)
  await asyncio.gather(cc.start(),sc.start());service=ControlService(sc,journal,stage=1,node='b',coordinator_node='controller',device='cuda:0',instance='fresh-b',identity=Holds(),authorize=v)
  declaration=__import__('private_task_consent').digest(self.declaration);service.check(s,declaration);v.bind_control(service.binding(s));calls=[]
  def body(scope):
   with v.hold_call(s,0):calls.append('ordinary body')
   return {'output_digest':'f'*64}
  role=RegisteredRole.register(service,s,body,declaration_digest=declaration,charge=64,completion=Completion());coordinator=CoordinatorControl(ControlClient(cc),role.descriptor)
  async def serve():
   await role.serve_once();await role.serve_once()
  try:
   _,receipt=await asyncio.gather(serve(),coordinator.dispatch());self.assertEqual(receipt['state'],'COMMITTED');self.assertEqual(calls,['ordinary body'])
  finally:
   await coordinator.close();await sc.close()
   for c in (cc,sc):
    if not c._task.done():c._task.cancel()
   await asyncio.gather(cc._task,sc._task,return_exceptions=True)
 async def test_parent_cannot_suppress_failed_commit(self):
  v,s,b=await self.view()
  def swallowing(spec,token,digest,commit):
   try:commit()
   except OSError:pass
  self.parent.finalize=swallowing
  def fail():raise OSError('uncertain local commit')
  with self.assertRaises(Exception):v.finalize(s,0,'f'*64,fail)
  self.assertTrue(v.retired)
 async def test_stage0_cannot_use_stage1_finalize(self):
  v,s,b=await self.view(0);out=[]
  with self.assertRaises(Exception):v.finalize(s,0,'f'*64,lambda:out.append(1))
  self.assertEqual(out,[])
 async def test_revoke_while_parent_finalize_waits_prevents_commit(self):
  v,s,b=await self.view();entered=asyncio.Event();release=threading.Event();out=[];loop=asyncio.get_running_loop()
  def wait(spec,token,digest,commit):
   loop.call_soon_threadsafe(entered.set)
   if not release.wait(5):raise AssertionError('release barrier')
   commit()
  self.parent.finalize=wait
  def worker():
   try:v.finalize(s,0,'f'*64,lambda:out.append('commit'))
   except Exception:out.append('denied')
  t=threading.Thread(target=worker);t.start();await asyncio.wait_for(entered.wait(),5)
  try:await self.consent.revoke(self.token,self.task.task_id)
  finally:release.set();await asyncio.to_thread(t.join,5)
  self.assertFalse(t.is_alive());self.assertEqual(out,['denied'])
 async def test_commit_before_revoke_is_not_retracted(self):
  v,s,b=await self.view();out=[];v.finalize(s,0,'f'*64,lambda:out.append('committed'))
  await self.consent.revoke(self.token,self.task.task_id);self.assertEqual(out,['committed'])
  with self.assertRaises(Exception):v.check('export')
 async def test_user_fence_exit_invalidates_admission(self):
  v,s,b=await self.view();old=self.local.hold
  @contextmanager
  def late(credential):
   with old(credential):yield
   self.consent.fence.invalidate(self.task.task_id)
  with patch.object(self.local,'hold',late):
   with self.assertRaises(Exception):v.check('execute')
 async def test_control_parent_denial_not_replaced_by_consent(self):
  v,s,b=await self.view()
  @contextmanager
  def deny(*args):raise PermissionError('real parent absent');yield
  self.parent.hold=deny
  with self.assertRaises(PermissionError):
   with v.hold(b,'execute'):self.fail('entered')
 async def test_claim_outer_exit_unknown_retires_issuer(self):
  original=self.adapter.claim_online
  async def failed_exit(issuer,registration,intent,peer,admission):
   @contextmanager
   def uncertain():
    with admission():yield None
    raise OSError('outer claim gate exit unknown')
   return await original(issuer,registration,intent,peer,uncertain)
  with patch.object(self.adapter,'claim_online',failed_exit):
   with self.assertRaises(Exception):await self.test_actual_memory_issuance_claim_uses_complete_context()
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM slots').fetchone(),(1,))
  self.assertTrue(self.strict.retired);self.assertTrue(self.adapter.retired)
if __name__=='__main__':
 names=[n for n in Tests.__dict__ if n.startswith('test_')]
 r=unittest.TextTestRunner().run(unittest.TestSuite(Tests(n) for n in names));raise SystemExit(not r.wasSuccessful())
