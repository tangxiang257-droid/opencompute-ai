"""Memory Channels and explicit synthetic external providers; no real authority."""
import asyncio,unittest,tempfile,hashlib
from pathlib import Path
from urllib.parse import urlencode
from validation.user_confirmation.test_entry import Tests as Base
from contextlib import contextmanager
from validation.role_bridge.test_transport import streams,config
from cluster_p1.channel import Channel,Grant
try:
 from cluster_p1.c_receiving import ReceivingStore,ReceivingController,ReceivingRole
except ImportError:ReceivingStore=ReceivingController=ReceivingRole=None
class Policy:
 @contextmanager
 def hold(self,*args):yield None
class Identity:
 @contextmanager
 def hold(self,who):yield who.node_id
class Tests(Base):
 async def asyncSetUp(self):
  await super().asyncSetUp()
  self.assertIsNotNone(ReceivingStore,'typed online C receiving protocol absent')
  from private_consent_runtime import IssuanceConsent
  from private_c_receiving_authority import CReceivingPolicy
  from cluster_p1.registered_input_source import RegisteredInputSource
  from cluster_p1.online_issuance import IssuanceAuthority
  r,page,form=await self.review();self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],200)
  adapter=IssuanceConsent(self.consent,self.token,r.proposal_id,r.proposal_digest,parent=Policy())
  source=RegisteredInputSource(self.reader,identity_fence=self.local,consent=adapter)
  self.issuer=IssuanceAuthority(self.root/'c-issuer',namespace='e'*64,source_provider=source,authorize=adapter,resources=Policy(),require_consent=True,enable_receiving=True);self.addCleanup(self.issuer.close)
  await source.register(self.issuer,self.token,self.ptask,self.session,self.stages,self.tiles,hidden_size=24,vocab_size=100,config_sha256=self.declaration['config_sha256'])
  self.policy=await CReceivingPolicy.create(adapter,resource_id='existing-capacity',charge=33168,parent=Policy(),maintenance=Policy());self.d=self.policy.descriptor
  a,aw,c,cw=streams('authority','c');g=Grant(self.d['task'],self.d['session'],1,1,(0,1),self.d['model'],2)
  self.ac=Channel(a,aw,config('c'),{'c':{g}},capacity_bytes=32768,capacity_tiles=1)
  self.cc=Channel(c,cw,config('authority'),{'authority':{g}},capacity_bytes=32768,capacity_tiles=1)
  self.sa=ReceivingStore.for_authority(self.issuer);self.sc=ReceivingStore(self.root/'c');self.addCleanup(self.sa.close);self.addCleanup(self.sc.close)
  self.controller=ReceivingController(self.ac,self.sa,self.d,identity=Identity(),authorize=self.policy,maintenance=self.policy,resources=Policy())
  self.role=ReceivingRole(self.cc,self.sc,self.d,issuer_node='authority',issuer_certificate=hashlib.sha256(b'authority').hexdigest(),identity=Identity(),authorize=Policy(),maintenance=Policy(),resources=Policy())
 async def asyncTearDown(self):
  if hasattr(self,'role'):await asyncio.gather(self.role.close(),self.controller.close(),return_exceptions=True)
  await super().asyncTearDown()
 async def exchange(self,action,*args):return await asyncio.gather(getattr(self.controller,action)(*args),self.role.serve_once())
 async def test_pending_has_no_receiving_qualification(self):
  await self.exchange('pending');self.assertEqual(self.sc.history(self.d['owner'],self.d['task'])['state'],'PENDING')
  with self.assertRaises(Exception):
   with self.role.hold_receiver({},'result-store'):pass
 async def test_revoke_pending_ack_only_after_local_retirement(self):
  await self.exchange('pending');await self.exchange('revoke')
  self.assertEqual(self.sc.history(self.d['owner'],self.d['task'])['state'],'REVOKED')
  self.assertEqual(self.sa.history(self.d['owner'],self.d['task'])['state'],'REVOKED')
 async def test_missing_capacity_denies_before_registration(self):
  self.controller.resources=None
  with self.assertRaises(Exception):await self.controller.pending()
  self.assertIsNone(self.sa.history(self.d['owner'],self.d['task']))
 async def b_control(self):
  from cluster_p1.control_exchange import ControlClient,ControlService
  from cluster_p1.control_journal import ControlJournal
  from cluster_p1.role_control import RegisteredRole,CoordinatorControl
  from cluster_p1.projection_driver import Spec
  from validation.role_query.test_roles import Completion
  from validation.control_exchange.test_transport import Holds
  from cluster_p1.c_receiving_input import CommittedBInput
  d=self.d;spec=Spec(d['owner'],d['task'],d['session'],d['epoch'],d['model'],d['layout'],d['input_snapshot'],d['preprocessing'],d['plan_digest'],d['token_digest'],d['hidden'],d['vocab'],('a'*64,))
  a,aw,b,bw=streams('authority','b');g=Grant(d['task'],d['session'],1,1,(0,1),d['model'],2)
  ca=Channel(a,aw,config('b'),{'b':{g}},capacity_bytes=32768,capacity_tiles=1);cb=Channel(b,bw,config('authority'),{'authority':{g}},capacity_bytes=32768,capacity_tiles=1)
  self.addAsyncCleanup(ca.close);self.addAsyncCleanup(cb.close);await asyncio.gather(ca.start(),cb.start())
  journal=ControlJournal(self.root/'b-control',1024,resources=Holds());self.addCleanup(journal.close)
  service=ControlService(cb,journal,stage=1,node='b',coordinator_node='authority',device='cuda:0',instance='fresh-b',identity=Holds(),authorize=Holds())
  self.bcalls=[]
  # Synthetic B operation/source; real prepare/query transport and persistent state.
  role=RegisteredRole.register(service,spec,lambda scope:self.bcalls.append(scope),declaration_digest=d['declaration_digest'],charge=64,completion=Completion())
  control=CoordinatorControl(ControlClient(ca),role.descriptor)
  return role,control,CommittedBInput(control)
 async def activate(self):
  from unittest.mock import patch
  from cluster_p1.c_receiving_input import CommittedBInput
  await self.exchange('pending');b,control,source=await self.b_control()
  async def synthetic(inner,d):
   # Explicit protocol-boundary double. Actual installed B receipt proof is tested separately.
   return dict(input_digest='a'*64,receipt_digest='b'*64,session=d['session'],epoch=d['epoch'],plan_digest=d['plan_digest'],tile=0)
  with patch.object(CommittedBInput,'read_committed',synthetic):result=await self.exchange('activate',source)
  self.assertEqual(self.bcalls,[]);return result,b,control
 async def test_C_binds_input_from_adapter_boundary_without_executing_B(self):
  await self.activate();row=self.sc.history(self.d['owner'],self.d['task'])
  self.assertEqual(row['state'],'ACTIVE');self.assertEqual(row['actual']['input_digest'],'a'*64)
 async def test_plain_digest_provider_cannot_activate(self):
  await self.exchange('pending')
  class Fake:
   async def read_committed(inner,d):raise AssertionError('must not call')
  with self.assertRaises(Exception):await self.controller.activate(Fake())
  self.assertEqual(self.sa.history(self.d['owner'],self.d['task'])['state'],'PENDING')
 async def test_revoke_after_user_revocation_uses_maintenance(self):
  await self.exchange('pending');await self.consent.revoke(self.token,self.d['task']);await self.exchange('revoke')
  self.assertEqual(self.sa.history(self.d['owner'],self.d['task'])['state'],'REVOKED')
 async def test_revoke_waits_for_owned_receive_exit(self):
  await self.activate();entered=asyncio.Event();release=asyncio.Event();cancelled=asyncio.Event()
  async def owned():
   entered.set()
   try:await asyncio.Future()
   except asyncio.CancelledError:cancelled.set();await release.wait();raise
  job=asyncio.create_task(owned());self.role.active_task=job;await entered.wait()
  jobs=[asyncio.create_task(self.controller.revoke()),asyncio.create_task(self.role.serve_once())]
  try:
   await asyncio.wait_for(cancelled.wait(),3)
   self.assertEqual(self.sa.history(self.d['owner'],self.d['task'])['state'],'REVOKE_PENDING')
   self.assertFalse(jobs[0].done());release.set();await asyncio.gather(*jobs)
   self.assertEqual(self.sc.history(self.d['owner'],self.d['task'])['state'],'REVOKED')
  finally:
   release.set();await asyncio.gather(job,*jobs,return_exceptions=True);self.role.active_task=None
 async def test_reopened_store_has_history_but_no_mutation(self):
  await self.exchange('pending');other=ReceivingStore(self.root/'c');self.addCleanup(other.close)
  self.assertEqual(other.history(self.d['owner'],self.d['task'])['state'],'PENDING')
  with self.assertRaises(Exception):other.unknown(self.d)
 async def test_lost_stop_ack_keeps_central_unknown_and_charge(self):
  from unittest.mock import patch
  await self.exchange('pending');old=self.role.send
  async def lost(message,action):
   if action=='stop-ack':raise OSError('lost ACK')
   return await old(message,action)
  with patch.object(self.role,'send',lost):results=await asyncio.gather(self.controller.revoke(),self.role.serve_once(),return_exceptions=True)
  self.assertTrue(all(isinstance(v,BaseException) for v in results))
  self.assertEqual(self.sc.history(self.d['owner'],self.d['task'])['state'],'REVOKED')
  self.assertEqual(self.sa.history(self.d['owner'],self.d['task'])['state'],'UNKNOWN')
  self.assertEqual(self.sa.history(self.d['owner'],self.d['task'])['charge'],33168)
 async def test_generic_B_prepare_cannot_supply_input_ready(self):
  await self.exchange('pending');b,control,source=await self.b_control()
  async def serve_b():
   try:
    await b.serve_once();await b.serve_once()
   finally:await control._client.channel.close()
  results=await asyncio.wait_for(asyncio.gather(self.controller.activate(source),self.role.serve_once(),serve_b(),return_exceptions=True),3)
  self.assertTrue(any(isinstance(x,BaseException) for x in results));self.assertEqual(self.bcalls,[])
  self.assertNotEqual(self.sa.history(self.d['owner'],self.d['task'])['state'],'ACTIVE')
 async def test_activation_after_revoke_does_not_query_B(self):
  from unittest.mock import patch
  from cluster_p1.c_receiving_input import CommittedBInput
  await self.exchange('pending');await self.exchange('revoke');b,control,source=await self.b_control();called=[]
  async def forbidden(*args):called.append(1);raise AssertionError('B query after revoked C')
  with patch.object(CommittedBInput,'read_committed',forbidden):
   with self.assertRaises(Exception):await self.controller.activate(source)
  self.assertEqual(called,[])
 async def test_repeated_pending_never_regrants(self):
  await self.exchange('pending')
  results=await asyncio.gather(self.controller.pending(),self.role.serve_once(),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in results));self.assertEqual(self.sa.history(self.d['owner'],self.d['task'])['state'],'UNKNOWN')
 async def test_capacity_exit_failure_retains_charge(self):
  from unittest.mock import patch
  @contextmanager
  def uncertain(*args):yield None;raise OSError('capacity unknown')
  with patch.object(self.controller.resources,'hold',uncertain):
   with self.assertRaises(Exception):await self.controller.pending()
  row=self.sa.history(self.d['owner'],self.d['task']);self.assertEqual((row['state'],row['charge']),('UNKNOWN',33168))
 async def test_wrong_issuer_is_rejected(self):
  a,aw,b,bw=streams('b','c');d=self.d;g=Grant(d['task'],d['session'],1,1,(0,1),d['model'],2)
  ch=Channel(b,bw,config('b'),{'b':{g}},capacity_bytes=32768,capacity_tiles=1);self.addAsyncCleanup(ch.close);self.addCleanup(aw.close)
  with self.assertRaises(Exception):ReceivingRole(ch,self.sc,d,issuer_node='authority',issuer_certificate=hashlib.sha256(b'authority').hexdigest(),identity=Identity(),authorize=Policy(),maintenance=Policy(),resources=Policy())
 async def test_stop_ack_unknown_commit_does_not_report_success(self):
  from unittest.mock import patch
  await self.exchange('pending');old=self.sc.move
  def fail(d,c,previous,state,proof=None):
   old(d,c,previous,state,proof)
   if state=='REVOKED':raise OSError('commit response lost')
  with patch.object(self.sc,'move',fail):results=await asyncio.gather(self.controller.revoke(),self.role.serve_once(),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in results));self.assertEqual(self.sa.history(self.d['owner'],self.d['task'])['state'],'UNKNOWN')
 async def test_control_disconnect_retires_active_receive_task(self):
  await self.activate();entered=asyncio.Event();exited=asyncio.Event()
  async def receiving():
   entered.set()
   try:await asyncio.Future()
   finally:exited.set()
  task=asyncio.create_task(receiving());self.role.active_task=task;await entered.wait()
  await self.role.close();self.assertTrue(exited.is_set());self.assertTrue(task.done());self.role.active_task=None
  self.assertEqual(self.sc.history(self.d['owner'],self.d['task'])['state'],'UNKNOWN')
 async def result_args(self):
  from cluster_p1.projection_driver import Spec
  from isolated_tls_delivery import ReceiveBinding
  from local_delivery_gate import Scope
  from validation.split_roles.test_transport import Identity as PeerIdentity,Authority as PeerPermission
  d=self.d;s=Spec(d['owner'],d['task'],d['session'],d['epoch'],d['model'],d['layout'],d['input_snapshot'],d['preprocessing'],d['plan_digest'],d['token_digest'],d['hidden'],d['vocab'],('a'*64,))
  ar,aw,br,bw=streams('b','c');mr,mw,rr,rw=streams('b','c');g=Grant(s.request,s.session,1,1,(0,1),s.model,2)
  meta=Channel(rr,rw,config('b'),{'b':{g}},capacity_bytes=32768,capacity_tiles=1)
  self.addAsyncCleanup(meta.close);self.addCleanup(aw.close);self.addCleanup(mw.close)
  binding=ReceiveBinding(Scope(s.request,1,s.plan_digest,s.layout,'out','logits',s.session,'single-token','b',hashlib.sha256(b'b').hexdigest(),'placeholder'),1,s.model,2,'float32',((0,1,(1,1,s.vocab)),))
  return (s,br,bw,config('b'),meta,binding),dict(identity=PeerIdentity(),authorize=PeerPermission())
 async def test_actual_ingress_factory_consumed_persistently(self):
  await self.activate();args,kw=await self.result_args();self.role.attach_result(*args,**kw)
  self.assertEqual(self.sc.history(self.d['owner'],self.d['task'])['state'],'RECEIVING')
  with self.assertRaises(Exception):self.role.attach_result(*args,**kw)
 async def test_actual_ingress_failure_marks_unknown_and_denies(self):
  await self.activate();args,kw=await self.result_args();self.role.attach_result(*args,**kw)
  with self.assertRaises(Exception):await self.role.receive_result(None,lambda:None,lambda:None)
  self.assertFalse(self.role.receiving);self.assertEqual(self.sc.history(self.d['owner'],self.d['task'])['state'],'UNKNOWN')
if __name__=='__main__':
 r=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_')));raise SystemExit(not r.wasSuccessful())
