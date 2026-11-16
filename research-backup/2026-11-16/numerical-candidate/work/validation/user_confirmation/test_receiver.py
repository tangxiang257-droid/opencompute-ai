"""Real consent and C view; synthetic C authority and no device/network."""
import asyncio,json,unittest,hashlib
from contextlib import contextmanager
from dataclasses import asdict,replace
from urllib.parse import urlencode
from validation.user_confirmation.test_entry import Tests as Base
from cluster_p1.projection_driver import Spec
from cluster_p1.split_role_transport import transport_scope
from isolated_tls_delivery import ReceiveBinding
from local_delivery_gate import Scope
try:
 from private_receiver_consent import ReceiverConsent
except ImportError:ReceiverConsent=None
class Parent:
 @contextmanager
 def hold_receiver(self,context,action):yield None
class Tests(Base):
 async def asyncSetUp(self):
  await super().asyncSetUp();self.assertIsNotNone(ReceiverConsent,'C confirmed receiving view absent')
 async def view(self):
  r,page,form=await self.review();self.assertEqual((await self.request('POST',body=urlencode(form).encode()))[0],200)
  b=json.loads(self.ledger._db.execute('SELECT body FROM task_consents').fetchone()[0]);d=b['declaration'];s=b['source']
  spec=Spec(self.owner,self.task.task_id,self.session.session_id,1,self.ptask.model_hash,self.session.layout_hash,s['input_snapshot'],s['preprocessing'],d['plan_digest'],s['token_digest'],24,100,('a'*64,))
  binding=ReceiveBinding(Scope(spec.request,1,spec.plan_digest,spec.layout,'out','logits',spec.session,'single-token','b',hashlib.sha256(b'b').hexdigest(),'original'),1,spec.model,2,'float32',((0,1,(1,1,100)),))
  v=await ReceiverConsent.create(self.consent,self.token,r.proposal_id,r.proposal_digest,spec,binding,parent=Parent())
  from validation.role_bridge.test_transport import streams,config
  from validation.split_roles.test_transport import Identity,Authority
  from cluster_p1.channel import Channel,Grant
  ar,aw,br,bw=streams('b','c');mr,mw,rr,rw=streams('b','c')
  grant=Grant(spec.request,spec.session,1,1,(0,1),spec.model,2)
  self.ms=Channel(mr,mw,config('c'),{'c':{grant}},capacity_bytes=32768,capacity_tiles=1)
  self.mc=Channel(rr,rw,config('b'),{'b':{grant}},capacity_bytes=32768,capacity_tiles=1)
  self.ri=v.ingress(br,bw,config('b'),self.mc,binding,identity=Identity(),authorize=Authority())
  self.addAsyncCleanup(self.ri.close);self.addAsyncCleanup(self.ms.close);self.addCleanup(aw.close)
  return v,spec,binding
 def context(self,s,b):return dict(recipient_node='c',purpose='result-store',receive_binding=json.loads(json.dumps(asdict(b))),binding=transport_scope(s))
 async def test_confirmed_receiver_checks_same_task_and_retirement(self):
  v,s,b=await self.view()
  with v.hold_receiver(self.context(s,b),'result-reserve'):pass
  await self.consent.revoke(self.token,s.request)
  with self.assertRaises(Exception):
   with v.hold_receiver(self.context(s,b),'result-store'):pass
 async def test_wrong_scope_and_wrong_connection_denied(self):
  v,s,b=await self.view();c=self.context(s,b);c['receive_binding']['scope']['incarnation']='reconnect'
  with self.assertRaises(Exception):
   with v.hold_receiver(c,'result-store'):pass
 async def test_missing_parent_rejected(self):
  v,s,b=await self.view()
  with self.assertRaises(Exception):await ReceiverConsent.create(self.consent,self.token,v.proposal_id,v.sha,s,b)
 async def test_body_error_not_suppressed(self):
  v,s,b=await self.view()
  @contextmanager
  def swallow(*a):
   try:yield None
   except OSError:pass
  v.parent.hold_receiver=swallow
  with self.assertRaises(OSError):
   with v.hold_receiver(self.context(s,b),'result-store'):raise OSError('disk')
  self.assertTrue(v.retired)
 async def test_wrong_actual_input_rejected(self):
  v,s,b=await self.view();c=self.context(s,b);c['binding']={'scope':transport_scope(s),'actual_inputs':['f'*64]}
  with self.assertRaises(Exception):
   with v.hold_receiver(c,'result-store'):pass
 async def test_actual_memory_metadata_enters_C_permission(self):
  from cluster_p1.envelope import Envelope
  v,s,b=await self.view();scope=transport_scope(s);raw=json.dumps({'message':'ordinary'},separators=(',',':')).encode()
  await asyncio.gather(self.ms.start(),self.mc.start())
  result=await asyncio.gather(self.ms.send(Envelope(s.request,s.session,s.epoch,1,0,(0,1),s.model,2,(len(raw),),'uint8',raw)),self.ri.read_json(self.mc,scope,1))
  self.assertEqual(result[1],{'message':'ordinary'})
 async def test_actual_metadata_generation_replacement_denied(self):
  from cluster_p1.split_role_transport import _result_binding
  v,s,b=await self.view();c=self.context(s,b)
  c['binding']={'scope':transport_scope(s),'actual_inputs':list(s.inputs),'generation':'f'*64,'binding':json.loads(json.dumps(asdict(_result_binding(b,s,'f'*64))))}
  with self.assertRaises(Exception):
   with v.hold_receiver(c,'result-store'):pass
 async def test_duplicate_receiving_view_denied(self):
  v,s,b=await self.view()
  with self.assertRaises(Exception):await ReceiverConsent.create(self.consent,self.token,v.proposal_id,v.sha,s,b,parent=Parent())
 async def test_confirmed_policy_and_real_generation_reach_parent(self):
  v,s,b=await self.view();seen=[]
  @contextmanager
  def parent(context,action):seen.append(context);yield None
  v.parent.hold_receiver=parent;c=self.context(s,b)
  c['binding']={'scope':transport_scope(s),'actual_inputs':list(s.inputs),'generation':v.generation,'binding':v.wire_binding}
  with v.hold_receiver(c,'result-commit'):pass
  self.assertEqual(seen[0]['receiver'],self.receiver)
  self.assertEqual(seen[0]['consent_digest'],v.sha)
 async def test_second_ingress_denied_before_taking_connection(self):
  v,s,b=await self.view()
  with self.assertRaises(Exception):v.ingress(None,None,None,None,b)
 async def test_confirmed_C_terms_flow_into_actual_issuer_registration(self):
  from private_consent_runtime import IssuanceConsent
  from cluster_p1.registered_input_source import RegisteredInputSource
  from cluster_p1.online_issuance import IssuanceAuthority
  from validation.consent_runtime.test_wiring import Parent as ExistingParent
  from validation.authority_resources.test_source import Permission
  v,s,b=await self.view()
  adapter=IssuanceConsent(self.consent,self.token,v.proposal_id,v.sha,parent=ExistingParent())
  source=RegisteredInputSource(self.reader,identity_fence=self.local,consent=adapter)
  issuer=IssuanceAuthority(self.root/'confirmed-issuer.db',namespace='e'*64,source_provider=source,authorize=adapter,resources=Permission(),require_consent=True)
  self.addCleanup(issuer.close)
  registration=await source.register(issuer,self.token,self.ptask,self.session,self.stages,self.tiles,hidden_size=24,vocab_size=100,config_sha256=self.declaration['config_sha256'])
  self.assertEqual(json.loads(registration.authorization)['body']['receiver'],self.receiver)
  self.assertEqual(issuer.db.execute('SELECT context FROM authorizations').fetchone(),(registration.authorization,))
if __name__=='__main__':
 r=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_')));raise SystemExit(not r.wasSuccessful())
