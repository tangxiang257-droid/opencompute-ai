"""Real confirmation and online stage-slot/client accept; LAB providers/SSL only."""
import asyncio,hashlib,unittest
from contextlib import contextmanager
from unittest.mock import patch
from validation.experiment_confirmation.test_entry import Tests as Base
from private_experiment_confirmation import ExperimentConfirmationApp
from validation.online_issuance.test_transport import Holds,Identity,Completion
from validation.role_bridge.test_transport import streams,config,Writer
from cluster_p1.channel import Channel,Grant
from cluster_p1.online_issuance import RoleInstallStore
from cluster_p1.online_issuance_transport import IssuanceClient
class InstallParent(Holds):
 def __init__(self):super().__init__();self.calls=[];self.exit_fail=False
 @contextmanager
 def hold(self,context,action):
  self.calls.append((context,action))
  with super().hold(context,action):yield None
  if self.exit_fail and action=='claim-installation':raise OSError('LAB claim scope exit')
class Tests(Base):
 async def asyncSetUp(self):
  await super().asyncSetUp();self.install_parent=InstallParent();self.resources=Holds()
  self.web=ExperimentConfirmationApp(self.root/'installation-web',self.consent,source_domain='a'*64,origin='https://lab.invalid',credential_for_scope=lambda s:self.token,policy_authority=self.parent,installation_authority=self.install_parent)
  self.adm=self.web.admission;self.addCleanup(self.adm.close);self.strict.source_provider=self.adm;self.strict.authorize=self.adm;self.strict.resources=self.resources
  self.receipt=await self.prepare_new();self.assertEqual((await self.request('POST',await self.get_form()))[0],200)
  self.channels=[];self.clients=[];self.services=[]
 async def asyncTearDown(self):
  await asyncio.gather(*(x.close() for x in getattr(self,'clients',[])+getattr(self,'services',[])+getattr(self,'channels',[])),return_exceptions=True)
  await super().asyncTearDown()
 async def endpoints(self,stage=0):
  import json
  body=json.loads(self.adm._row(self.owner,self.receipt.proposal_id)[0]);s=body['declaration']['session'];node=body['placements'][stage]['node']
  cr,cw,sr,sw=streams(node,'issuer');g=Grant(body['execution_task'],s['session_id'],1,stage,(0,1),s['model_hash'],2)
  cc=Channel(cr,cw,config('issuer'),{'issuer':{g}},capacity_bytes=32768,capacity_tiles=1);sc=Channel(sr,sw,config(node),{node:{g}},capacity_bytes=32768,capacity_tiles=1);self.channels.extend((cc,sc))
  kw=dict(issuer_node='issuer',issuer_certificate=hashlib.sha256(b'issuer').hexdigest(),identity=Identity(),authorize=Holds(),resources=self.resources)
  service=await self.adm.installation_service(self.token,self.receipt.proposal_id,self.receipt.proposal_digest,stage,sc,**kw);self.services.append(service)
  store=RoleInstallStore(self.root/('role-'+node),role_node=node,identity=Holds(),authorize=Holds(),resources=self.resources,device=Holds());self.addCleanup(store.close)
  client=IssuanceClient(cc,store,service._descriptor,completion=Completion(),**kw);self.clients.append(client)
  return client,service,store
 async def exchange(self,stage=0):
  client,service,store=await self.endpoints(stage)
  result=await asyncio.gather(client.install(),service.serve_once(),return_exceptions=True)
  return result,store
 async def test_both_stages_actual_online_slots_and_accept_no_execution(self):
  for stage in (0,1):
   (handle,response),store=await self.exchange(stage)
   if isinstance(handle,BaseException):raise handle
   if isinstance(response,BaseException):raise response
   self.assertFalse(response['execution_allowed']);self.assertEqual(store.db.execute('SELECT state FROM installs').fetchone(),('ACCEPTED',))
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM slots').fetchone(),(2,))
  for context,action in self.install_parent.calls:
   self.assertEqual(context['body']['policy'],self.policy)
   if action=='claim-installation':self.assertEqual(context['peer']['node_id'],context['descriptor']['node'])
 async def test_no_installation_provider_default_refuses(self):
  # Original fixture web had no installation provider; explicit absence remains deny.
  self.adm.installation_authority=None
  with self.assertRaises(Exception):await self.endpoints()
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM slots').fetchone(),(0,))
 async def test_parent_denial_before_endpoint(self):
  self.install_parent.revoked=True
  with self.assertRaises(Exception):await self.endpoints()
 async def test_source_revoke_after_endpoint_before_claim(self):
  client,service,store=await self.endpoints();await self.consent.revoke(self.token,self.task.task_id)
  result=await asyncio.gather(client.install(),service.serve_once(),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in result));self.assertFalse(store.install_handles)
 async def test_wrong_resource_in_wire_intent_refuses(self):
  client,service,store=await self.endpoints();old=client.send
  async def corrupt(body,seq):return await old(dict(body,intent=dict(body['intent'],resource_id='wrong')),seq)
  client.send=corrupt
  result=await asyncio.gather(client.install(),service.serve_once(),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in result));self.assertEqual(self.strict.db.execute('SELECT count(*) FROM slots').fetchone(),(0,))
 async def test_claim_commit_unknown_burns_eligibility(self):
  client,service,store=await self.endpoints();old=self.strict._commit
  def lost(db):old(db);raise OSError('claim committed reply lost')
  with patch.object(self.strict,'_commit',lost):result=await asyncio.gather(client.install(),service.serve_once(),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in result));self.assertEqual(self.strict.db.execute('SELECT count(*) FROM slots').fetchone(),(1,));self.assertTrue(self.adm.retired);self.assertFalse(store.install_handles)
 async def test_claim_parent_exit_unknown_keeps_slot(self):
  self.install_parent.exit_fail=True;(result,store)=await self.exchange()
  self.assertTrue(all(isinstance(x,BaseException) for x in result));self.assertEqual(self.strict.db.execute('SELECT count(*) FROM slots').fetchone(),(1,));self.assertTrue(self.adm.retired)
 async def test_endpoint_cannot_be_recreated(self):
  await self.endpoints()
  with self.assertRaises(Exception):await self.endpoints()
 async def test_lost_issued_response_no_retry(self):
  client,service,store=await self.endpoints();old=service.send
  async def lost(body,seq):
   if body['kind']=='issued':raise OSError('LAB wire lost')
   return await old(body,seq)
  service.send=lost;result=await asyncio.gather(client.install(),service.serve_once(),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in result));self.assertFalse(store.install_handles)
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM slots').fetchone(),(1,))
  with self.assertRaises(Exception):await self.endpoints()
 async def test_no_local_locks_over_channel_wait(self):
  old=Writer.drain
  async def drain(writer):
   self.assertFalse(self.consent.authority._lock.locked());self.assertEqual(self.install_parent.active,0);return await old(writer)
  with patch.object(Writer,'drain',drain):result,store=await self.exchange()
  self.assertFalse(any(isinstance(x,BaseException) for x in result))
 async def test_direct_claim_or_execution_still_refuses(self):
  for action in ('claim','install','execute','load'):
   with self.assertRaises(Exception):
    with self.adm.hold({},action):pass
 async def test_copied_registration_cannot_create_service(self):
  from dataclasses import replace
  pid=self.receipt.proposal_id;self.adm._registered[pid]=replace(self.adm._registered[pid])
  with self.assertRaises(Exception):await self.endpoints()
 async def test_service_provider_replacement_refuses(self):
  client,service,store=await self.endpoints();service.authorize=Holds()
  result=await asyncio.gather(client.install(),service.serve_once(),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in result));self.assertFalse(store.install_handles)
 async def test_resource_provider_replacement_refuses(self):
  client,service,store=await self.endpoints();service.resources=Holds()
  result=await asyncio.gather(client.install(),service.serve_once(),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in result));self.assertFalse(store.install_handles)
 async def test_revocation_while_issued_write_waits_blocks_grant(self):
  client,service,store=await self.endpoints();old=service.send;entered=asyncio.Event()
  async def blocked(body,seq):
   if body['kind']=='issued':await service.channel._write.acquire();entered.set()
   return await old(body,seq)
  service.send=blocked
  job=asyncio.gather(client.install(),service.serve_once(),return_exceptions=True)
  await asyncio.wait_for(entered.wait(),3);await self.consent.revoke(self.token,self.task.task_id);service.channel._write.release()
  result=await job;self.assertTrue(all(isinstance(x,BaseException) for x in result));self.assertFalse(store.install_handles)
  self.assertEqual(self.strict.db.execute('SELECT count(*) FROM slots').fetchone(),(1,));self.assertEqual(sum(f[:1]==b'D' for f in service.channel.w.frames),1)
 async def test_wire_callback_drift_never_enters_protected_body(self):
  client,service,store=await self.endpoints();wire=service.authorize;old=wire.parent.hold;entered=[]
  @contextmanager
  def drift(*args):
   with old(*args):wire.parent=Holds();yield None
  wire.parent.hold=drift;wire.pin=(wire.parent,drift)
  with self.assertRaises(Exception):
   with service.admission('claim'):entered.append(True)
  self.assertEqual(entered,[]);self.assertTrue(self.adm.retired)
 async def test_absent_installation_provider_checked_before_context(self):
  from private_experiment_admission import ExperimentAdmission
  a=ExperimentAdmission(self.root/'no-install-provider',self.consent,source_domain='a'*64,trigger=self.web.trigger,policy_authority=self.parent);self.addCleanup(a.close)
  with patch.object(a,'_valid') as check:
   with self.assertRaises(Exception):await a.installation_service(self.token,'unused','f'*64,0,None,issuer_node='issuer',issuer_certificate='a'*64)
   check.assert_not_called()
def load_tests(loader,tests,pattern):return unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
