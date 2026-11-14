"""Synthetic confirmation/identity/resource policies, actual installed B and C IO.

This composes real memory Channels, committed input and receiving ingress. The
LAB control sender deliberately substitutes for a genuinely authorized issuer;
it does not test user confirmation, PKI or physical capacity.
"""
import asyncio,hashlib,inspect
from cluster_p1.c_receiving import ReceivingRole,ReceivingStore
from cluster_p1.c_receiving_input import CommittedBInput
from cluster_p1.c_dispatch_gate import BDispatchGate,CDispatchGate
from cluster_p1.channel import Channel,Grant
from cluster_p1.control_exchange import _json,_parse
from cluster_p1.envelope import Envelope
from validation.role_bridge.test_transport import streams,config
from validation.c_receiving.test_protocol import Identity,Policy
from validation.split_roles.test_pipeline import as_role

async def install(t,b,control,s,reader,writer,metadata,binding,identity,permission):
 d=dict(version=1,purpose='result-receive',owner=s.owner,task=s.request,session=s.session,epoch=s.epoch,
  model=s.model,layout=s.layout,plan_digest=s.plan_digest,input_snapshot=s.input_snapshot,token_digest=s.token_digest,
  preprocessing=s.preprocessing,hidden=s.hidden,vocab=s.vocab,consent_id='synthetic-confirmation',consent_digest='a'*64,
  revision=0,c_node='c',b_node='b',resource_id='synthetic-existing-C-capacity',charge=s.vocab*4+32768,
  receiver=dict(version=1,node='c',purpose='result-store',identity='synthetic-C-policy',storage_policy='private',retention_seconds=60,max_charge_bytes=s.vocab*4+32768),
  b_placement={k:b.startup.descriptor[k] for k in ('stage','node','device','instance','resource_id')},
  declaration_digest=control.descriptor.binding['declaration_digest'],authority_incarnation='f'*32)
 def pair(a,z):
  ar,aw,zr,zw=streams(a,z);g=Grant(s.request,s.session,1,1,(0,1),s.model,2)
  ca=Channel(ar,aw,config(z),{z:{g}},capacity_bytes=32768,capacity_tiles=1)
  cz=Channel(zr,zw,config(a),{a:{g}},capacity_bytes=32768,capacity_tiles=1)
  t.channels.extend((ca,cz));return ca,cz
 authority,c=pair('authority','c');store=ReceivingStore(t.root/'gate-c.db');t.addCleanup(store.close)
 role=getattr(t,'create_receiver',ReceivingRole)(c,store,d,issuer_node='authority',issuer_certificate=hashlib.sha256(b'authority').hexdigest(),identity=Identity(),authorize=Policy(),maintenance=Policy(),resources=Policy())
 if inspect.isawaitable(role):role=await role
 t.addAsyncCleanup(role.close);t.gate_c_role=role;t.gate_c_store=store
 async def issue(op,seq,proof=None):
  request=dict(version=1,purpose='result-receive',sequence=seq,op=op,descriptor=d,challenge='e'*64,actual=proof,recipient=None if seq==0 else store.incarnation)
  async def send():
   if seq==0:await authority.start()
   raw=_json(request);await authority.send(Envelope(s.request,s.session,1,1,seq,(0,1),s.model,2,(len(raw),),'uint8',raw),max_frame_bytes=65536)
   delivery=await authority.receive();reply=_parse(delivery.envelope.payload);await delivery.release();return reply
  return await asyncio.gather(send(),getattr(t,'receiving_launcher',role).serve_once())
 await issue('pending',0)
 async def serve_ready():
  # prepare() might already have run in an input-readiness test; still queries.
  for _ in range(2):await (t.launchers[1].serve_once() if hasattr(t,'launchers') else b.serve_once())
 _,proof=await asyncio.gather(as_role('b',serve_ready()),as_role('coordinator',CommittedBInput(control).read_committed(d)))
 await issue('activate',1,proof)
 getattr(t,'receiving_launcher',role).attach_result(s,reader,writer,config('b'),metadata,binding,identity=identity,authorize=permission)
 bg,cg=pair('b','c')
 gate=t.launchers[1].attach_gate(bg,d,identity=Identity(),authorize=Policy()) if hasattr(t,'launchers') else BDispatchGate(bg,b,d,identity=Identity(),authorize=Policy())
 receiver=t.receiving_launcher.attach_gate(cg,identity=Identity(),authorize=Policy()) if hasattr(t,'receiving_launcher') else CDispatchGate(cg,role,identity=Identity(),authorize=Policy())
 t.addAsyncCleanup(receiver.close);t.gate_b=gate;t.gate_c=receiver
 async def serve_gate():return await as_role('c',t.receiving_launcher.serve_gate() if hasattr(t,'receiving_launcher') else receiver.serve_once())
 t.gate_task=asyncio.create_task(serve_gate())
 async def finish():
  if not t.gate_task.done():t.gate_task.cancel()
  await asyncio.gather(t.gate_task,return_exceptions=True)
 t.addAsyncCleanup(finish)
 class Ingress:
  async def receive(self,spec,artifacts,check,fence):
   assert spec==role.spec
   return await getattr(t,'receiving_launcher',role).receive_result(artifacts,check,fence)
  async def close(self):await role.close()
 return Ingress()
