"""Explicit one-shot B hidden-data listener, original installation and receipt.

Only trusted bootstrap may enable it with real providers. No provider discovery,
execution permission, input prediction, retry or quota release. Existing receipt
Channel deadlines still bound installation and computation idle time.
"""
import asyncio,hashlib,ipaddress,socket,ssl,json
from contextlib import contextmanager
from dataclasses import asdict
from private_control_action_host import _Endpoint
from private_installation_host import _accepted_tls_streams
from cluster_p1.role_launcher import StageLauncher,_cleanup_failure
from cluster_p1.channel import TLSConfig,Grant
from cluster_p1.control_exchange import _need,_json,_parse
from cluster_p1.online_issuance_transport import _json as _intent_json
from cluster_p1.online_issuance import _DESCRIPTOR
from cluster_p1.projection_driver import Spec,nothing
from cluster_p1.prepared_receipt import PreparedReceiptConnection
from cluster_p1.split_role_transport import HiddenReceiver,transport_scope,_ssl_snapshot
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.nonce_coordinator import _require_issued_journal
from cluster_p1.tls_material import load_context
from cluster_p1.delivery_receipt import _with_cleanup
from cluster_p1.data_session import DataSession,DataSessionPolicy
from cluster_p1.hidden_session import hidden_binding

_FIELDS=set('version purpose launcher_sha256 spec_sha256 peer peer_certificate certificate material_sha256 bind port hostname accept_seconds handshake_seconds close_seconds session'.split())
class BHiddenHost:
 def __init__(self,raw,pin,launcher,spec,tls,material,*,identity=None,authorize=None,resources=None):
  _need(type(raw)is bytes and len(raw)<=16384 and hashlib.sha256(raw).hexdigest()==pin)
  c=_parse(raw);_need(set(c)==_FIELDS and type(c['version'])is int and c['version']==1 and c['purpose']=='B-hidden-host')
  _need(type(launcher)is StageLauncher and launcher.config.data['role']=='B' and type(spec)is Spec and type(tls)is TLSConfig)
  _need(c['launcher_sha256']==launcher.config._sha and c['spec_sha256']==hashlib.sha256(_json(json.loads(json.dumps(asdict(spec))))).hexdigest())
  _need(type(material)is bytes and hashlib.sha256(material).hexdigest()==c['material_sha256'] and _parse(material)['leaf_sha256']==c['certificate'])
  _need(type(c['peer'])is str and c['peer']!=launcher.config.data['node'] and tls.peers=={c['peer']:c['peer_certificate']})
  for k in ('certificate','peer_certificate'):_need(type(c[k])is str and len(c[k])==64 and all(x in '0123456789abcdef' for x in c[k]))
  for k,maximum in (('accept_seconds',30),('handshake_seconds',10),('close_seconds',10)):_need(type(c[k])in(int,float) and 0<c[k]<=maximum)
  _need(type(c['port'])is int and 1024<=c['port']<=65535)
  ip=ipaddress.ip_address(c['bind']);_need(str(ip)==c['bind'] and any(ip in ipaddress.ip_network(n) for n in ('127.0.0.0/8','10.0.0.0/8','172.16.0.0/12','192.168.0.0/16','::1/128','fc00::/7')))
  _need(type(c['hostname'])is str and 0<len(c['hostname'])<=253 and not any(x.isspace() or ord(x)<32 for x in c['hostname']))
  for p in (identity,authorize,resources):_need(callable(getattr(p,'hold',None)))
  self.raw=raw;self.c=c;self.launcher=launcher;self.startup=launcher.startup;self.spec=spec
  _need(type(c['session'])is dict and set(c['session'])=={'interval','silence','lifetime'})
  self.session_policy=DataSessionPolicy(**c['session']);self.data_session=None
  self.identity=identity;self.authorize=authorize;self.resources=resources;self.ticket=self.startup.runtime['ticket']
  _need(c['peer']==self.startup.descriptor['declaration']['stages'][0]['assigned_node'])
  _need(resources is self.startup.store.resources and resources is self.startup.runtime['resources'])
  resources._validate(self.startup.descriptor,spec)
  self.tls=TLSConfig(tls.certfile,tls.keyfile,tls.cafile,dict(tls.peers));self.material=material
  self.loop=asyncio.get_running_loop();self.slot=_Endpoint(self.loop,c);self.pin=self._pins()
  self.prepared=self.receiver=self.peer=self.ssl_pin=self.origin=None
  self.receiver_pin=None
  self.origin_record=self.pending_pin=self.intent_pin=None
  self.started=self.closed=self.claimed=self.installing=self.receiving=False;self.state='NEW'
  self.work=self.active=self.cleanup=self.transport_cleanup=self.failure=None
  self._quarantine=resources.quarantine;self.retired=False
 def _pins(self):
  s=self.startup;r=self.resources
  providers=(self.identity,self.authorize,r,r.authority,r.capacity,s.store,s.store.identity,s.store.authorize,s.store.device,s.client.identity,s.client.authorize,s.client.resources,s.client._completion,*s.runtime.values())
  return (self.launcher,self.launcher.config,s,s.client,s.store,s.channel,_intent_json(s.descriptor),self.spec,self.spec.digest,self.ticket,self.session_policy,tuple((id(p),n,getattr(p,n,None)) for p in providers for n in ('hold','quarantine','verify_stage','check','hold_load','hold_call','finalize','load','open_scope','require_open','materialize','finish_scope','require_finished')),self.tls.certfile,self.tls.keyfile,self.tls.cafile,tuple(sorted(self.tls.peers.items())),self.material)
 def _leaf(self):
  _need(not self.closed and self.loop is asyncio.get_running_loop() and _json(self.c)==self.raw and self.pin==self._pins())
  l=self.launcher;s=self.startup;r=self.resources;t=self.ticket
  _need(l.startup is s and r is s.runtime['resources'] and r is s.store.resources and t is s.runtime['ticket'])
  _need(r.tickets.get(id(t)) is t and t.reservation_id==s.descriptor['resource_id'] and t.reservation_id not in r.poisoned)
  _need(r._row(t.reservation_id)[5]=='HELD');r._validate(s.descriptor,self.spec);l._live();s.client.current()
  if self.claimed:_need(l._data_transport_owner==(self,self.close_transports,self.retire))
  if self.prepared is not None:
   ch=self.prepared.channel;_need(ch.w is self.slot.writer and self.slot.channel is ch and ch.authenticated_identity==self.peer and _ssl_snapshot(ch.w)==self.ssl_pin)
   with ch.identity_fence(self.peer):pass
  if self.data_session is not None:self.data_session.check()
  if l._installed_ready:
   _need(s.prepared is self.prepared and s.journal is self.prepared.journal)
   row=s.store.issued.get(id(s.journal))
   _need(self.origin_record is not None and row is self.origin_record and row[0] is s.journal and row[1] is self.origin[1])
   h=row[1];p=h.pending;_need(p is self.pending_pin and p.raw==self.intent_pin and {k:p.request[k] for k in _DESCRIPTOR}==s.descriptor)
   _need(s.store.pending_handles.get(id(p)) is p)
   stored=s.store._row(p);_need(stored[2]=='INSTALLED' and not stored[3])
  if self.receiver_pin is not None:
   receiver=self.receiver
   _need(type(receiver)is HiddenReceiver and receiver is self.receiver_pin)
   _need(receiver._bound is self.prepared.bound and receiver._journal is self.prepared.journal and receiver._scope==transport_scope(self.spec))
   _need(receiver._identity is self.identity and receiver._authorize is self and receiver._channels==(self.prepared.channel,))
 def _check(self):
  self._leaf();s=self.startup
  self.launcher.check();self._leaf()
  if self.launcher._installed_ready:
   _need(_require_issued_journal(s.journal,1)==self.origin);self._leaf()
  with s.client.admission('live-installation' if self.launcher._installed_ready else 'begin-install'):
   self._leaf()
  self._leaf()
  if self.prepared is not None:
   with self.hold_identity():
    with self.hold(self.c['peer'],transport_scope(self.spec),'hidden-receive'):pass
  self._leaf()
 @contextmanager
 def hold_identity(self):
  self._leaf();ch=self.prepared.channel
  with ch.identity_fence(self.peer),_non_suppressing(self.identity.hold(self.peer)) as principal:
   _need(principal==self.c['peer']);yield;self._leaf()
  self._leaf()
 def session_check(self):
  self._leaf()
  with self.startup.client.admission('live-installation' if self.launcher._installed_ready else 'begin-install'):self._leaf()
  self._leaf()
 @contextmanager
 def hold(self,principal,scope,action):
  self._leaf();_need(principal==self.c['peer'] and scope==transport_scope(self.spec))
  with _non_suppressing(self.authorize.hold(principal,scope,action)) as allowed:
   nothing(allowed);self._leaf();yield None;self._leaf()
  self._leaf()
 def retire(self):
  if self.claimed and not self.retired:
   self.retired=True;self.state='UNKNOWN';self._quarantine(self.spec,self.ticket,None)
 async def _run(self):
  self._check();l=self.launcher;_need(not l._install_attempted and not hasattr(l,'_data_transport_owner'))
  l._data_transport_owner=(self,self.close_transports,self.retire);self.claimed=True;l._jobs.add(asyncio.current_task())
  try:
   ctx=load_context(self.tls,self.material,self.c['material_sha256'],server=True,hostname=self.c['hostname'])
   _need(type(ctx)is ssl.SSLContext and ctx.verify_mode==ssl.CERT_REQUIRED and not ctx.check_hostname and ctx.minimum_version>=ssl.TLSVersion.TLSv1_2)
   self._check();slot=self.slot;c=self.c
   slot.listener=socket.socket(socket.AF_INET6 if ':' in c['bind'] else socket.AF_INET,socket.SOCK_STREAM)
   slot.listener.setblocking(False);slot.listener.bind((c['bind'],c['port']));slot.listener.listen(1);self.state='LISTENING'
   task=asyncio.create_task(self.loop.sock_accept(slot.listener));slot.pending['accept']=task
   slot.accepted,_=await slot._wait(task,c['accept_seconds']);slot.listener.close();slot.listener=None;self._check()
   task=asyncio.create_task(_accepted_tls_streams(slot,slot.accepted,ctx,c['handshake_seconds'],c['close_seconds']));slot.pending['handshake']=task
   reader,slot.writer=await slot._wait(task,c['handshake_seconds']);self._check();l._available_writer(slot.writer)
   snap=_ssl_snapshot(slot.writer);_need(snap[0]==c['peer_certificate'] and snap[2]in('TLSv1.2','TLSv1.3') and snap[3]==ssl.CERT_REQUIRED and snap[5]is True)
   s=self.spec;g=Grant(s.request,s.session,s.epoch,0,(0,1),s.model,2)
   self.prepared=await PreparedReceiptConnection.create(reader,slot.writer,self.tls,{c['peer']:{g}})
   slot.channel=self.prepared.channel;self.peer=slot.channel.authenticated_identity;self.ssl_pin=snap
   self._check();l._watch(slot.channel)
   binding=hidden_binding(l,self.spec,c['certificate'],c['peer_certificate'])
   self.data_session=DataSession.attach(slot.channel,binding,self.session_policy,identity=self.identity,authorize=self.authorize,check=self.session_check,enabled=True)
   await self.data_session.ready();self._check();self.state='PREPARED';return self
  except BaseException as error:
   try:await self.close_transports()
   except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
  finally:l._jobs.discard(asyncio.current_task())
 async def start(self,*,enabled=False):
  _need(enabled is True and not self.started and not self.closed);self.started=True;self.work=asyncio.create_task(self._run())
  try:return await asyncio.shield(self.work)
  except BaseException as error:await self._failed(error)
 async def _failed(self,error):
  self.failure=error;self.state='UNKNOWN';self.active=None
  if self.cleanup is None:
   try:await self.close()
   except BaseException as cleanup:raise _with_cleanup(error,cleanup)
  raise error
 async def install(self,*,enabled=False,prepare_fence=None,session_policy=None):
  _need(enabled is True and self.state=='PREPARED' and not self.installing);self.installing=True;self.active=asyncio.current_task()
  try:
   self._check();_need(self.data_session is not None and self.data_session.active)
   await self.launcher.install(prepared=self.prepared,prepare_fence=prepare_fence,session_policy=session_policy)
   record=self.startup.store.issued.get(id(self.prepared.journal));_need(record is not None and record[0] is self.prepared.journal)
   self.origin_record=record;self.origin=(self.startup.store,record[1]);self.pending_pin=record[1].pending;self.intent_pin=self.pending_pin.raw
   self._check()
   self.receiver=HiddenReceiver(self.prepared.bound,self.prepared.journal,scope=transport_scope(self.spec),identity=self.identity,authorize=self)
   self.receiver_pin=self.receiver;self._check()
   self.state='READY';return self.prepared.journal
  except BaseException as error:await self._failed(error)
  finally:self.active=None
 async def receive_once(self,*,enabled=False):
  _need(enabled is True and self.state=='READY' and not self.receiving);self.receiving=True;self.active=asyncio.current_task()
  async def receive():
   self._check();result=await self.receiver.receive();self._check();self.state='RECEIVED';return result
  try:return await self.launcher._run(receive)
  except BaseException as error:await self._failed(error)
  finally:self.active=None
 async def close_transports(self):
  if self.transport_cleanup is None:
   async def cleanup():
    errors=[]
    try:self.retire()
    except BaseException as error:errors.append(error)
    try:await self.slot.close()
    except BaseException as error:errors.append(error)
    if errors:raise BaseExceptionGroup('hidden data transport unknown',errors)
   self.transport_cleanup=asyncio.create_task(cleanup())
  from private_experiment_assembly import _settle
  await _settle(self.transport_cleanup)
 async def _cleanup(self):
  errors=[]
  try:self.retire()
  except BaseException as error:errors.append(error)
  tasks=[t for t in (self.work,self.active) if t is not None]
  for t in tasks:
   if not t.done():t.cancel()
  for t in tasks:
   try:await t
   except BaseException as error:
    if error is not self.failure and _cleanup_failure(error):errors.append(error)
  if self.claimed:
   try:await self.launcher.close()
   except BaseException as error:errors.append(error)
  try:await self.close_transports()
  except BaseException as error:errors.append(error)
  if errors:raise BaseExceptionGroup('B hidden host cleanup unknown',errors)
  if self.state!='UNKNOWN':self.state='CLOSED'
 async def close(self):
  self.closed=True
  if self.cleanup is None:
   self.cleanup=asyncio.create_task(self._cleanup());self.cleanup.add_done_callback(lambda t:None if t.cancelled() else t.exception())
  try:done,_=await asyncio.wait((self.cleanup,),timeout=self.c['close_seconds'])
  except BaseException:self.state='UNKNOWN';raise
  if self.cleanup not in done:self.state='UNKNOWN';raise TimeoutError('hidden host cleanup pending; retain resource responsibility')
  self.cleanup.result()
