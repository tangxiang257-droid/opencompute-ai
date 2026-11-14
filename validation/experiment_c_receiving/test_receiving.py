"""Actual confirmation/import/C protocol/Ingress; LAB providers and B-proof double."""
import asyncio,hashlib,json,struct,unittest
from contextlib import contextmanager
from unittest.mock import patch
from validation.c_input_import.test_import import Tests as Base,LabProvider
from validation.experiment_admission import test_admission as fixture
from validation.c_receiving.test_protocol import Tests as Protocol,Identity,Policy
from validation.role_bridge.test_transport import streams,config
from cluster_p1.channel import Channel,Grant
from cluster_p1.c_receiving import ReceivingStore,ReceivingController,ReceivingRole
from private_c_receiving_authority import CReceivingPolicy
from cluster_p1.online_issuance import IssuanceAuthority
from cluster_p1.projection_driver import Spec
from cluster_p1.split_role_transport import ResultSender
from validation.split_roles.test_transport import Authority
from cluster_p1.byte_spool import Book,Binding,Plan,Spool
from isolated_tls_delivery import ReceiveBinding
from local_delivery_gate import Scope
class Tests(Base):
 b_control=Protocol.b_control
 async def asyncSetUp(self):
  def receiving(*a,**kw):return IssuanceAuthority(*a,**dict(kw,enable_receiving=True))
  with patch.object(fixture,'IssuanceAuthority',receiving):await super().asyncSetUp()
  await self.registered();await self.run_import()
  self.receive_parent=LabProvider();self.maintenance=LabProvider()
  self.cp=await self.make_policy();self.d=self.cp.descriptor
  a,aw,c,cw=streams('authority','c');g=Grant(self.d['task'],self.d['session'],1,1,(0,1),self.d['model'],2)
  ac=Channel(a,aw,config('c'),{'c':{g}},capacity_bytes=32768,capacity_tiles=1);cc=Channel(c,cw,config('authority'),{'authority':{g}},capacity_bytes=32768,capacity_tiles=1)
  self.sa=ReceivingStore.for_authority(self.strict);self.sc=ReceivingStore(self.root/'receiving');self.addCleanup(self.sc.close)
  self.controller=ReceivingController(ac,self.sa,self.d,identity=Identity(),authorize=self.cp,maintenance=self.cp,resources=Policy())
  # Co-located independent C consent check; remote identity/capacity are explicit LAB.
  self.role=ReceivingRole(cc,self.sc,self.d,issuer_node='authority',issuer_certificate=hashlib.sha256(b'authority').hexdigest(),identity=Identity(),authorize=self.cp,maintenance=self.cp,resources=Policy())
 async def make_policy(self):return await CReceivingPolicy.create_experiment(self.importer,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,parent=self.receive_parent,maintenance=self.maintenance)
 async def asyncTearDown(self):
  if hasattr(self,'role'):await asyncio.gather(self.role.close(),self.controller.close(),return_exceptions=True)
  await super().asyncTearDown()
 async def exchange(self,op,*args):return await asyncio.gather(getattr(self.controller,op)(*args),self.role.serve_once())
 async def activate_lab_proof(self):
  from cluster_p1.c_receiving_input import CommittedBInput
  await self.exchange('pending');_,_,source=await self.b_control()
  async def lab(inner,d):return dict(input_digest='a'*64,receipt_digest='b'*64,session=d['session'],epoch=d['epoch'],plan_digest=d['plan_digest'],tile=0)
  with patch.object(CommittedBInput,'read_committed',lab):await self.exchange('activate',source)
 async def result_pair(self):
  await self.activate_lab_proof();d=self.d
  s=Spec(d['owner'],d['task'],d['session'],1,d['model'],d['layout'],d['input_snapshot'],d['preprocessing'],d['plan_digest'],d['token_digest'],d['hidden'],d['vocab'],('a'*64,));self.spec=s
  grant=Grant(s.request,s.session,1,1,(0,1),s.model,2)
  sr,sw,rr,rw=streams('b','c');mr,mw,cr,cw=streams('b','c')
  data=Channel(sr,sw,config('c'),{'c':{grant}},capacity_bytes=32816,capacity_tiles=1)
  meta=Channel(mr,mw,config('c'),{'c':{grant}},capacity_bytes=32768,capacity_tiles=1);cm=Channel(cr,cw,config('b'),{'b':{grant}},capacity_bytes=32768,capacity_tiles=1)
  binding=ReceiveBinding(Scope(s.request,1,s.plan_digest,s.layout,'out','logits',s.session,'single-token','b',hashlib.sha256(b'b').hexdigest(),'placeholder'),1,s.model,2,'float32',((0,1,(1,1,s.vocab)),))
  self.transport_authority=Authority();kw=dict(identity=Identity(),authorize=self.transport_authority)
  self.sender=ResultSender(data,meta,binding,**kw);self.addAsyncCleanup(self.sender.close)
  self.role.attach_result(s,rr,rw,config('b'),cm,binding,**kw)
  source=Book(self.root/'spool-source',131072);destination=Book(self.root/'spool-destination',131072);self.addCleanup(source.close);self.addCleanup(destination.close)
  plan=Plan(Binding(s.owner,s.request,s.session,'single-token',1,self.sender.generation(),s.model,s.layout,s.input_snapshot,s.token_digest,s.preprocessing,s.plan_digest),(1,),s.vocab)
  source.reserve('result',plan,'source');destination.reserve('result',plan,'destination');spool=Spool(self.root/'spool',source,destination,'result',plan)
  raw=struct.pack('<f',1.0)*100;spool.produce(lambda write:write(0,0,raw));return spool,hashlib.sha256(raw).hexdigest()
 def publication(self):return self.cp.hold(self.d,'receive')
 async def transfer(self):
  spool,digest=await self.result_pair()
  return await asyncio.gather(self.sender.send(self.spec,spool,digest,lambda:None),self.role.receive_result(self.artifacts,lambda:None,self.publication),return_exceptions=True)
 async def test_real_ingress_synthetic_bytes_and_result_ack(self):
  ack,result=await self.transfer();self.assertIsInstance(ack,dict);self.assertEqual(ack['manifest_digest'],result);self.assertFalse(ack['execution_allowed'])
  self.assertEqual(self.sc.history(self.d['owner'],self.d['task'])['state'],'RECEIVED')
  chunks=[];self.artifacts.fetch(self.owner,self.d['task'],lambda tile,b:chunks.append(b));self.assertEqual(b''.join(chunks),struct.pack('<f',1.)*100)
 async def test_pending_no_receive_permission(self):
  await self.exchange('pending');self.assertEqual(self.sc.history(self.owner,self.d['task'])['state'],'PENDING');self.assertFalse(self.role.receiving)
 async def test_no_original_live_handle_refuses(self):
  self.adm._registered.clear()
  with self.assertRaises(Exception):await self.make_policy()
 async def test_missing_parent_refuses(self):
  with self.assertRaises(Exception):await CReceivingPolicy.create_experiment(self.importer,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,maintenance=self.maintenance)
 async def test_source_revocation_blocks_pending(self):
  await self.consent.revoke(self.token,self.task.task_id)
  with self.assertRaises(Exception):await self.controller.pending()
 async def test_multiple_views_cannot_reclaim(self):
  other=await self.make_policy();self.cp.claim()
  with self.assertRaises(Exception):other.claim()
 async def test_import_unknown_cannot_create(self):
  self.adm.db.execute("UPDATE c_import_slots SET state='UNKNOWN'")
  with self.assertRaises(Exception):await self.make_policy()
 async def test_context_mutation_refuses(self):
  self.cp.experiment.body['receiver']['resource_id']='wrong'
  with self.assertRaises(Exception):
   with self.cp.hold(self.d,'pending'):pass
 async def test_capacity_loss_refuses(self):
  self.capacity.fail=True
  with self.assertRaises(Exception):await self.controller.pending()
 async def test_untrusted_digest_provider_cannot_activate(self):
  await self.exchange('pending')
  class Fake:
   async def read_committed(inner,d):raise AssertionError('must not run')
  with self.assertRaises(Exception):await self.controller.activate(Fake())
  self.assertFalse(self.role.receiving)
 async def test_execution_stays_denied(self):
  with self.assertRaises(Exception):
   with self.adm.hold({},'claim'):pass
 async def test_revoke_pending_after_user_revocation(self):
  await self.exchange('pending');await self.consent.revoke(self.token,self.task.task_id);await self.exchange('revoke')
  self.assertEqual(self.sc.history(self.owner,self.d['task'])['state'],'REVOKED')
 async def test_revoked_during_metadata_wait_no_publication(self):
  spool,digest=await self.result_pair();ingress=self.role.ingress
  ingress._meta.w.block_kind=b'C'
  sender=asyncio.create_task(self.sender.send(self.spec,spool,digest,lambda:None))
  receiver=asyncio.create_task(self.role.receive_result(self.artifacts,lambda:None,self.publication))
  await asyncio.wait_for(ingress._meta.w.entered.wait(),3)
  await self.consent.revoke(self.token,self.task.task_id);ingress._meta.w.resume.set()
  result=await asyncio.gather(sender,receiver,return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in result));self.assertEqual(self.sc.history(self.owner,self.d['task'])['state'],'UNKNOWN')
  self.assertEqual(self.artifacts._metadata(self.owner,self.d['task'])[0].state,'STORED')
 async def test_published_result_lost_ack_unknown_no_retry(self):
  spool,digest=await self.result_pair();ingress=self.role.ingress
  async def lost(*a,**kw):raise OSError('LAB ACK lost after durable result')
  with patch.object(ingress,'send_json',lost):
   result=await asyncio.gather(self.sender.send(self.spec,spool,digest,lambda:None),self.role.receive_result(self.artifacts,lambda:None,self.publication),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in result));self.assertEqual(self.sc.history(self.owner,self.d['task'])['state'],'UNKNOWN')
  self.assertEqual(self.artifacts._metadata(self.owner,self.d['task'])[0].state,'RESULT_STORED')
  with self.assertRaises(Exception):await self.role.receive_result(self.artifacts,lambda:None,self.publication)
 async def test_no_local_fences_across_network_waits(self):
  from validation.role_bridge.test_transport import Writer
  original=Writer.drain
  async def drain(writer):
   self.assertFalse(self.consent.authority._lock.locked());self.assertFalse(self.consent.fence.lock._is_owned());return await original(writer)
  with patch.object(Writer,'drain',drain):
   ack,result=await self.transfer();self.assertIsInstance(ack,dict)
 async def test_copied_registration_record_not_live_authority(self):
  from dataclasses import replace
  self.adm._registered[self.receipt.proposal_id]=replace(self.cp.registration)
  with self.assertRaises(Exception):await self.make_policy()
 async def test_import_context_mutation_refuses_and_stays_dead(self):
  request=self.cp.experiment.request;old=request['context_digest'];request['context_digest']='f'*64
  with self.assertRaises(Exception):self.cp._check()
  request['context_digest']=old
  with self.assertRaises(Exception):self.cp._check()
 async def test_source_cancel_after_import_refuses(self):
  await self.a.cancel(self.owner,self.task.task_id)
  with self.assertRaises(Exception):await self.controller.pending()
 async def test_provider_exit_failure_after_pending_retains_charge(self):
  self.receive_parent.fail_exit=True
  with self.assertRaises(Exception):await self.controller.pending()
  row=self.sa.history(self.owner,self.d['task'])
  # Nested claim checks fail before pending write; no reset of one-shot claim.
  self.assertTrue(row is None or row['state']=='UNKNOWN')
  self.assertGreater(self.slots()[0][1],32768)
 async def test_observed_wrapper_drift_is_permanent(self):
  original=self.cp.parent;self.cp.parent=LabProvider()
  with self.assertRaises(Exception):
   with self.cp.hold(self.d,'receive'):pass
  self.cp.parent=original
  with self.assertRaises(Exception):
   with self.cp.hold(self.d,'receive'):pass
 async def test_runtime_reuses_complete_source_validator(self):
  with patch.object(self.reader,'_read_current',side_effect=PermissionError('source validation failed')) as check:
   with self.assertRaises(Exception):
    with self.cp.hold(self.d,'receive'):pass
   check.assert_called_once()
 async def test_cancelled_target_cannot_receive(self):
  self.artifacts.cancel(self.owner,self.d['task'])
  with self.assertRaises(Exception):
   with self.cp.hold(self.d,'receive'):pass
 async def test_isolated_target_cannot_receive(self):
  self.artifacts.quarantine(self.owner,self.d['task'])
  with self.assertRaises(Exception):
   with self.cp.hold(self.d,'receive'):pass
 async def test_outer_drift_inside_provider_never_enters_body(self):
  original=self.receive_parent.hold;entered=[]
  @contextmanager
  def drift(*args):
   with original(*args):
    self.cp.parent=LabProvider();yield None
  self.receive_parent.hold=drift;self.cp.experiment.pins=(self.receive_parent,self.maintenance,drift,self.maintenance.hold)
  with self.assertRaises(Exception):
   with self.cp.hold(self.d,'receive'):entered.append(True)
  self.assertEqual(entered,[]);self.assertTrue(self.cp.experiment.dead)
def load_tests(loader,tests,pattern):return unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
