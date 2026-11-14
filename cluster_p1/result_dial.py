"""Private B result transport preparation, then original sender/gate attachment.

No authority is created here. Real installed B, actual committed input, same
resources and explicit trusted providers are required. CONNECTED is raw TLS,
not C readiness or an execution grant. No retries or quota release.
"""
import asyncio,hashlib,json,math,ssl
from dataclasses import asdict
from .channel import TLSConfig,Grant,_accept_unstarted
from .role_launcher import StageLauncher,_cleanup_failure
from .local_stage_operations import committed_b_input,StageBOperation
from .nonce_coordinator import _require_issued_journal
from .online_issuance import _DESCRIPTOR
from .control_exchange import _need,_json,_parse
from .projection_driver import Spec,nothing
from .split_role_transport import ResultSender,_generation,_ssl_snapshot,_result_binding
from .c_receiving import descriptor
from .tls_material import load_context
from .role_startup import _close_channel
from .delivery_receipt import _with_cleanup
from private_experiment_assembly import _settle

_ORDER=('data','metadata','gate')
class BResultDial:
 def __init__(self,raw,pin,launcher,spec,binding,receiving,tls,material,*,identity=None,authorize=None,gate_identity=None,gate_authorize=None):
  _need(type(raw)is bytes and len(raw)<=16384 and hashlib.sha256(raw).hexdigest()==pin)
  c=_parse(raw);_need(set(c)=={'version','purpose','launcher_sha256','spec_sha256','binding_sha256','receiving_sha256','peer_certificate','material_sha256','timeout','prepare_seconds',*_ORDER} and type(c['version'])is int and c['version']==1 and c['purpose']=='B-result-dial')
  _need(type(launcher)is StageLauncher and launcher.config.data['role']=='B' and type(spec)is Spec and type(tls)is TLSConfig)
  _result_binding(binding,spec,binding.scope.incarnation);d=descriptor(receiving)
  _need(c['launcher_sha256']==launcher.config._sha and c['spec_sha256']==hashlib.sha256(_json(json.loads(json.dumps(asdict(spec))))).hexdigest() and c['binding_sha256']==hashlib.sha256(_json(json.loads(json.dumps(asdict(binding))))).hexdigest() and c['receiving_sha256']==hashlib.sha256(_json(d)).hexdigest())
  _need(tls.peers=={d['c_node']:c['peer_certificate']} and type(c['peer_certificate'])is str and len(c['peer_certificate'])==64 and all(x in '0123456789abcdef' for x in c['peer_certificate']))
  _need(type(material)is bytes and hashlib.sha256(material).hexdigest()==c['material_sha256'] and _parse(material)['leaf_sha256']==binding.scope.peer_certificate_der_sha256)
  for k,cap in (('timeout',10),('prepare_seconds',30)):_need(type(c[k])in(int,float) and math.isfinite(c[k]) and 0<c[k]<=cap)
  for k in _ORDER:
   e=c[k];_need(type(e)is dict and set(e)=={'host','port','hostname'} and type(e['port'])is int and 1<=e['port']<=65535)
   for name in ('host','hostname'):_need(type(e[name])is str and 0<len(e[name])<=253 and not any(x.isspace() or ord(x)<32 for x in e[name]))
  _need(len({(c[k]['host'],c[k]['port']) for k in _ORDER})==3)
  for p in (identity,authorize,gate_identity,gate_authorize):_need(callable(getattr(p,'hold',None)))
  self.raw=raw;self.c=c;self.launcher=launcher;self.startup=launcher.startup;self.spec=spec;self.binding=binding;self.d=d
  self.identity=identity;self.authorize=authorize;self.gate_identity=gate_identity;self.gate_authorize=gate_authorize
  self.tls=TLSConfig(tls.certfile,tls.keyfile,tls.cafile,dict(tls.peers));self.material=material
  self.store=self.startup.store;self.issued_raw=None;self.issued_pending=None;self.issued_handle=None;self.runtime=dict(self.startup.runtime);self.resources=self.runtime['resources'];self.ticket=self.runtime['ticket'];self.journal=self.startup.journal;self.session=self.startup.client._session
  self.retirement=(self.resources,self.resources.quarantine,self.spec,self.ticket);self.loop=asyncio.get_running_loop();self.pin=self._pins();self.streams={};self.raw_pins={};self.tasks=[];self.channels={}
  self.sender_pin=None;self.sender=self.operation=self.gate=None;self.work=self.cleanup=self.transport_cleanup=self.failure=self.active=None
  self.claimed=self.started=self.closed=self.sent=False;self.state='NEW';self.deadline=self.timer=None
 def _pins(self):
  s=self.startup;r=self.runtime
  providers=(self.identity,self.authorize,self.gate_identity,self.gate_authorize,s.client.identity,s.client.authorize,s.client.resources,s.client._completion,*r.values(),self.resources.authority,self.resources.capacity)
  methods=tuple((id(p),n,getattr(p,n,None)) for p in providers for n in ('hold','check','verify_stage','quarantine','hold_load','hold_call','finalize','load','open_scope','require_open','materialize','finish_scope','require_finished'))
  return (self.launcher,self.startup,self.startup.client,self.store,self.journal,self.session,self.ticket,self.resources,self.spec,self.spec.digest,_json(self.d),self.binding,self.tls.certfile,self.tls.keyfile,self.tls.cafile,tuple(sorted(self.tls.peers.items())),self.material,methods)
 def _peer(self,writer):
  v=_ssl_snapshot(writer);_need(v[0]==self.c['peer_certificate'] and v[2] in ('TLSv1.2','TLSv1.3') and v[3]==ssl.CERT_REQUIRED and v[4] is True and v[5] is False);return v
 def _leaf(self):
  _need(not self.closed and asyncio.get_running_loop() is self.loop and _json(self.c)==self.raw and self._pins()==self.pin)
  l=self.launcher;s=self.startup;_need(l.startup is s and s.store is self.store and s.journal is self.journal and s.client._session is self.session and self.session is not None)
  _need(set(s.runtime)==set(self.runtime) and all(s.runtime[k] is v for k,v in self.runtime.items()));self.session.check();l.check()
  _need(l._installed_ready and l.config._sha==self.c['launcher_sha256'])
  if self.issued_handle is not None:
   row=self.store.issued.get(id(self.journal));_need(row is not None and row[0] is self.journal and row[1] is self.issued_handle and self.issued_handle.pending is self.issued_pending)
   _need(self.issued_pending.raw==self.issued_raw and {k:self.issued_pending.request[k] for k in _DESCRIPTOR}==s.descriptor)
   stored=self.store._row(self.issued_pending);_need(stored[2]=='INSTALLED' and not stored[3])
  if self.claimed:_need(l._result_transport_owner==(self,self.close_transports))
  if self.deadline is not None:_need(self.loop.time()<self.deadline)
  for k,(r,w) in self.streams.items():_need(not r.at_eof() and r.exception() is None and self._peer(w)==self.raw_pins[k])
  if self.operation is not None:
   _need(l.operation is self.operation and self.operation.spec is self.spec and self.operation.spool.plan.binding.connection==_generation(self.streams['data'][1]))
   _need(self.gate is l.gate and self.gate.role is l.installed and self.gate.channel is self.channels['gate'] and self.gate.identity is self.gate_identity and self.gate.authorize is self.gate_authorize)
   _need(self.sender is self.sender_pin and type(self.sender)is ResultSender and self.sender._data is self.channels['data'] and self.sender._meta is self.channels['metadata'] and self.sender._identity is self.identity and self.sender._authorize is self.authorize and self.sender._binding is self.binding)
 def _check(self):
  self._leaf();s=self.startup;d=s.descriptor
  _need((d['node'],d['device'],d['instance'],d['resource_id'])==tuple(self.d['b_placement'][k] for k in ('node','device','instance','resource_id')))
  _need(self.binding.scope.peer_node==d['node'] and self.d['owner']==self.spec.owner and self.d['task']==self.spec.request)
  _need(all(getattr(self.spec,k)==self.d[k] for k in ('session','epoch','model','layout','plan_digest','input_snapshot','token_digest','preprocessing','hidden','vocab')))
  _need(getattr(self.journal,'_online_issuer',None) is self.store)
  store,handle=_require_issued_journal(self.journal,1)
  _need(store is self.store and {k:handle.pending.request[k] for k in _DESCRIPTOR}==d)
  if self.issued_handle is None:self.issued_handle=handle;self.issued_pending=handle.pending;self.issued_raw=handle.pending.raw
  _need(handle is self.issued_handle)
  with s.client.admission('live-installation'):
   nothing(self.resources.verify_stage(self.spec,self.ticket,1))
   committed_b_input(self.journal,self.spec,self.runtime['layer_range'])
  self._leaf()
 def generation(self):
  _need(self.state in ('CONNECTED','READY','SENT'));self._check();return _generation(self.streams['data'][1])
 async def _prepare(self):
  self._check();l=self.launcher;_need(not hasattr(l,'_result_transport_owner') and l.operation is None and not l._operation_attempted)
  l._result_transport_owner=(self,self.close_transports);self.claimed=True;l._jobs.add(asyncio.current_task())
  try:
   ctx=load_context(self.tls,self.material,self.c['material_sha256']);_need(type(ctx)is ssl.SSLContext and ctx.verify_mode==ssl.CERT_REQUIRED and ctx.check_hostname and ctx.minimum_version>=ssl.TLSVersion.TLSv1_2)
   for k in _ORDER:
    self._check();e=self.c[k];_need(ctx.verify_mode==ssl.CERT_REQUIRED and ctx.check_hostname and ctx.minimum_version>=ssl.TLSVersion.TLSv1_2)
    task=asyncio.create_task(asyncio.open_connection(e['host'],e['port'],ssl=ctx,server_hostname=e['hostname']));self.tasks.append(task)
    done,_=await asyncio.wait((task,),timeout=self.c['timeout'])
    if task not in done:raise TimeoutError('B result TLS dial incomplete')
    stream=task.result();r,w=stream
    _need(all(w is not v[1] and r is not v[0] for v in self.streams.values()));l._available_writer(w)
    self.streams[k]=stream;self.raw_pins[k]=self._peer(w);self._check()
   self.state='CONNECTED';self.deadline=self.loop.time()+self.c['prepare_seconds']
   def expire():
    self.state='UNKNOWN';task=asyncio.create_task(self.close());task.add_done_callback(lambda t:None if t.cancelled() else t.exception())
   self.timer=self.loop.call_later(self.c['prepare_seconds'],expire);return self
  except BaseException as error:
   self.failure=error
   try:await self.close_transports()
   except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
  finally:l._jobs.discard(asyncio.current_task())
 async def prepare(self,*,enabled=False):
  _need(enabled is True and not self.started and not self.closed);self.started=True;self.work=asyncio.create_task(self._prepare())
  try:return await asyncio.shield(self.work)
  except BaseException as error:
   self.failure=error
   if self.claimed:self.state='UNKNOWN'
   try:await self.close()
   except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
 async def attach_ready(self,*,enabled=False):
  _need(enabled is True and self.state=='CONNECTED' and not self.closed);self.state='ATTACHING'
  async def attach():
   self._check();l=self.launcher;op=l.operation
   _need(type(op)is StageBOperation and op.spec is self.spec and l.installed is not None and l.installed.operation is op and op.receiver is self.journal and all(getattr(op,k) is self.runtime[k] for k in ('authority','resources','ticket','device','loader')) and l._attached_ready and l._actions_ready and l.gate is None)
   _need(op.spool.plan.binding.connection==_generation(self.streams['data'][1]));op.received_bytes()
   grant=Grant(self.spec.request,self.spec.session,self.spec.epoch,1,(0,1),self.spec.model,2)
   for k in _ORDER:
    self.channels[k]=await _accept_unstarted(*self.streams[k],self.tls,{self.d['c_node']:{grant}},capacity_bytes=32816 if k=='data' else 32768,capacity_tiles=1);self._check()
   sender=ResultSender(self.channels['data'],self.channels['metadata'],self.binding,identity=self.identity,authorize=self.authorize)
   l._take_channel(self.channels['data']);l._take_channel(self.channels['metadata']);l.attach_gate(self.channels['gate'],self.d,identity=self.gate_identity,authorize=self.gate_authorize)
   self.sender=self.sender_pin=sender;self.operation=op;self.gate=l.gate;self._check();self.deadline=None
   if self.timer is not None:self.timer.cancel()
   self.state='READY';return self
  try:return await self.launcher._run(attach)
  except BaseException as error:
   self.failure=error;self.state='UNKNOWN'
   if self.cleanup is None:
    try:await self.close()
    except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
 async def send(self,spec,spool,digest,check):
  _need(self.state=='READY' and not self.sent and spec is self.spec and spool is self.operation.spool and callable(check));self.sent=True;self.active=asyncio.current_task()
  def checked():self._check();nothing(check());self._leaf()
  async def send():
   checked();return await self.sender.send(spec,spool,digest,checked)
  try:
   result=await self.launcher._run(send);self._leaf();self.state='SENT';return result
  except BaseException as error:
   self.failure=error;self.state='UNKNOWN';self.active=None
   if self.cleanup is None:
    try:await self.close()
    except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
  finally:self.active=None
 async def close_transports(self):
  if self.timer is not None:self.timer.cancel()
  if self.transport_cleanup is None:
   async def cleanup():
    errors=[];streams=list(self.streams.values())
    if self.claimed:
     # Terminal retirement of the same retained promise, not new authority or
     # device completion. Original callback survives loss of online eligibility.
     service,quarantine,spec,ticket=self.retirement
     try:nothing(quarantine(spec,ticket,None))
     except BaseException as error:errors.append(error)
    for task in self.tasks:
     if not task.done():task.cancel()
     try:
      value=await task
      if not any(value[1] is w for _,w in streams):streams.append(value)
     except BaseException as error:
      if _cleanup_failure(error) and error is not self.failure:errors.append(error)
    if self.sender_pin is not None:
     try:await self.sender_pin.close()
     except BaseException as error:errors.append(error)
    for c in self.channels.values():
     try:await _close_channel(c)
     except BaseException as error:errors.append(error)
    for _,w in streams:
     try:w.close()
     except BaseException as error:errors.append(error)
     try:await w.wait_closed()
     except BaseException as error:errors.append(error)
    if errors:raise BaseExceptionGroup('B result transports unknown',errors)
   self.transport_cleanup=asyncio.create_task(cleanup())
  await _settle(self.transport_cleanup)
 async def _cleanup(self):
  errors=[]
  for t in (self.work,self.active):
   if t is None:continue
   if not t.done():t.cancel()
   try:await t
   except BaseException as error:
    if _cleanup_failure(error) and error is not self.failure:errors.append(error)
  if self.claimed:
   try:await self.launcher.close()
   except BaseException as error:errors.append(error)
  try:await self.close_transports()
  except BaseException as error:errors.append(error)
  if errors:self.state='UNKNOWN';raise BaseExceptionGroup('B result dial cleanup unknown',errors)
  if self.state!='UNKNOWN':self.state='CLOSED'
 async def close(self):
  self.closed=True
  if self.cleanup is None:
   self.cleanup=asyncio.create_task(self._cleanup());self.cleanup.add_done_callback(lambda t:None if t.cancelled() else t.exception())
  try:done,_=await asyncio.wait((self.cleanup,),timeout=self.c['timeout'])
  except BaseException:self.state='UNKNOWN';raise
  if self.cleanup not in done:self.state='UNKNOWN';raise TimeoutError('B result cleanup pending; retain resources')
  self.cleanup.result()
