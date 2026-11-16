"""Original installed A -> bounded hidden-data session and committed send.

Supplied authenticated Channel and trusted local certificates/providers only.
No listener, identity issuer, execution permit, resource reservation or retry.
Before ownership claim the supplied Channel remains the caller's responsibility.
"""
import asyncio
from contextlib import contextmanager
from .channel import Channel
from .role_launcher import StageLauncher
from .projection_driver import Spec,nothing
from .local_stage_operations import StageAOperation
from .nonce_coordinator import _require_issued_journal
from .online_issuance import _DESCRIPTOR
from .online_issuance_transport import _json
from .control_exchange import _need
from .split_role_transport import HiddenSender,transport_scope,_ssl_snapshot
from .local_attempt import _non_suppressing
from .envelope import Envelope
from .data_session import DataSession,DataSessionPolicy

def hidden_binding(launcher,spec,local_certificate,peer_certificate):
 _need(type(launcher)is StageLauncher and type(spec)is Spec)
 for value in (local_certificate,peer_certificate):_need(type(value)is str and len(value)==64 and all(x in '0123456789abcdef' for x in value))
 s=launcher.startup;d=s.descriptor;stage=d['stage'];_need(stage in (0,1))
 s.runtime['resources']._validate(d,spec)
 nodes=[x['assigned_node'] for x in d['declaration']['stages']];_need(len(nodes)==2 and nodes[0]!=nodes[1] and nodes[stage]==d['node'])
 certificates=[peer_certificate,peer_certificate];certificates[stage]=local_certificate
 installation={k:d[k] for k in ('registration_id','issuer_incarnation','source_namespace','source_id','declaration_digest','source_binding')}
 c=launcher.config.data;installation['issuer']=dict(node=c['issuer_node'],certificate=c['issuer_certificate'])
 return dict(transport_scope=transport_scope(spec),a=dict(node=nodes[0],certificate=certificates[0]),b=dict(node=nodes[1],certificate=certificates[1]),installation=installation)

class _ADataOwner:
 def __init__(self,launcher,channel,spec,policy,local_certificate,peer_certificate,identity,authorize):
  _need(type(launcher)is StageLauncher and launcher.config.data['role']=='A' and type(channel)is Channel and type(policy)is DataSessionPolicy)
  _need(type(spec)is Spec and not channel._start_attempted and not hasattr(launcher,'_data_transport_owner'))
  for p in (identity,authorize):_need(callable(getattr(p,'hold',None)))
  launcher.check();s=launcher.startup;_need(launcher._installed_ready and s.journal is not None)
  self.launcher=launcher;self.startup=s;self.channel=channel;self.spec=spec;self.policy=policy
  self.identity=identity;self.authorize=authorize;self.resources=s.runtime['resources'];self.ticket=s.runtime['ticket'];self.journal=s.journal
  self.binding=hidden_binding(launcher,spec,local_certificate,peer_certificate)
  self.origin=_require_issued_journal(self.journal,0);_need(self.origin[0] is s.store)
  self.record=s.store.issued[id(self.journal)];self.pending=self.origin[1].pending;self.intent=self.pending.raw
  self.peer=channel.authenticated_identity;self.ssl=_ssl_snapshot(channel.w)
  _need((self.peer.node_id,self.peer.certificate_sha256)==(self.binding['b']['node'],peer_certificate))
  self.claimed=self.retired=self.sent=False;self.session=self.sender=self.operation=None;self.transport_cleanup=None
  self.sender_pin=None
  self.quarantine=self.resources.quarantine;self.pin=self._pins()
 def _pins(self):
  s=self.startup;r=self.resources
  providers=(self.identity,self.authorize,r,r.authority,r.capacity,s.store,s.store.identity,s.store.authorize,s.store.device,s.client.identity,s.client.authorize,s.client.resources,s.client._completion,*s.runtime.values())
  return (self.launcher,self.launcher.config,s,s.client,s.store,s.channel,self.channel,self.channel.r,self.channel.w,self.journal,self.spec,self.spec.digest,self.ticket,self.policy,_json(self.binding),_json(s.descriptor),tuple((id(p),n,getattr(p,n,None)) for p in providers for n in ('hold','check','quarantine','verify_stage','hold_load','hold_stage0','load','open_scope','require_open','materialize','finish_scope','require_finished')))
 def _leaf(self):
  _need(self.pin==self._pins() and not self.retired);l=self.launcher;s=self.startup;r=self.resources;t=self.ticket
  l._live();s.client.current();_need(l.startup is s and s.journal is self.journal and l._installed_ready)
  _need(r.tickets.get(id(t)) is t and t.reservation_id==s.descriptor['resource_id'] and t.reservation_id not in r.poisoned and r._row(t.reservation_id)[5]=='HELD')
  _need(s.store.issued.get(id(self.journal)) is self.record and self.record[0] is self.journal and self.record[1] is self.origin[1])
  _need(self.origin[1].pending is self.pending and self.pending.raw==self.intent and {k:self.pending.request[k] for k in _DESCRIPTOR}==s.descriptor)
  row=s.store._row(self.pending);_need(row[2]=='INSTALLED' and not row[3])
  _need(self.channel.authenticated_identity==self.peer and _ssl_snapshot(self.channel.w)==self.ssl)
  with self.channel.identity_fence(self.peer):pass
  if self.claimed:_need(l._data_transport_owner==(self,self.close_transports,self.retire))
  if self.session is not None:self.session.check()
  if self.sender_pin is not None:
   _need(type(self.sender)is HiddenSender and self.sender is self.sender_pin and self.sender._channel is self.channel)
   _need(self.sender._scope==transport_scope(self.spec) and self.sender._identity is self.identity and self.sender._authorize is self)
 def check(self):
  self._leaf();self.launcher.check();self._leaf();_need(_require_issued_journal(self.journal,0)==self.origin);self._leaf()
  with self.startup.client.admission('live-installation'):self._leaf()
  self._leaf()
 def session_check(self):
  # Transport lifetime checks current original task/resources, not a new
  # execution action or operation/device readiness inspection.
  self._leaf()
  with self.startup.client.admission('live-installation'):self._leaf()
  self._leaf()
 @contextmanager
 def hold(self,principal,scope,action):
  self._leaf();_need(principal==self.peer.node_id and scope==transport_scope(self.spec))
  with _non_suppressing(self.authorize.hold(principal,scope,action)) as result:
   nothing(result);self._leaf();yield None;self._leaf()
  self._leaf()
 def retire(self):
  if self.claimed and not self.retired:
   self.retired=True;self.quarantine(self.spec,self.ticket,None)
 async def open(self):
  self.check();l=self.launcher;l._take_channel(self.channel);self.channel._single_start=True
  l._data_transport_owner=(self,self.close_transports,self.retire);self.claimed=True;l._watch(self.channel)
  async def opening():
   self.session=DataSession.attach(self.channel,self.binding,self.policy,identity=self.identity,authorize=self.authorize,check=self.session_check,enabled=True)
   await self.session.ready();self.check()
   await self.channel.start();self.check()
   self.sender=HiddenSender(self.channel,scope=transport_scope(self.spec),identity=self.identity,authorize=self)
   self.sender_pin=self.sender;self.check()
   return self
  return await l._run(opening)
 async def send_committed(self,*,enabled=False):
  _need(enabled is True and self.sender is not None and not self.sent);self.sent=True
  async def send():
   self.check();_need(self.session.active)
   op=self.launcher.operation;_need(type(op)is StageAOperation and op.sender is self.journal and op.spec is self.spec)
   self.operation=op;raw=op.committed_bytes();self.check()
   s=self.spec;e=Envelope(s.request,s.session,s.epoch,0,0,(0,1),s.model,2,(1,1,s.hidden),'float32',raw)
   result=await self.sender.send(e.encode());self.check();return result
  return await self.launcher._run(send)
 async def close_transports(self):
  if self.transport_cleanup is None:
   async def cleanup():
    errors=[]
    try:self.retire()
    except BaseException as e:errors.append(e)
    try:await self.channel.close()
    except BaseException as e:errors.append(e)
    try:await asyncio.wait_for(self.channel.w.wait_closed(),self.channel.timeout)
    except BaseException as e:errors.append(e)
    if errors:raise BaseExceptionGroup('A hidden transport cleanup unknown',errors)
   self.transport_cleanup=asyncio.create_task(cleanup())
  from private_experiment_assembly import _settle
  await _settle(self.transport_cleanup)
 async def close(self):await self.launcher.close()

async def open_hidden_session(launcher,channel,spec,*,local_certificate=None,peer_certificate=None,policy=None,identity=None,authorize=None,enabled=False):
 _need(enabled is True)
 owner=_ADataOwner(launcher,channel,spec,policy,local_certificate,peer_certificate,identity,authorize)
 return await owner.open()
