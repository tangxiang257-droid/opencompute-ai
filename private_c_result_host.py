"""Explicit B-to-C accepted endpoints using the existing C authority/result core.

Trusted bootstrap supplies pinned public configuration and real providers. No
provider discovery, new permission, dispatch, quota release or retry. Before
claim C is borrowed; a claimed failure retires that original receiving launcher.
"""
import json,asyncio,hashlib,ipaddress,socket,ssl
from dataclasses import asdict
from private_control_action_host import _Endpoint
from private_installation_host import _accepted_tls_streams
from cluster_p1.role_launcher import ReceivingLauncher,_cleanup_failure
from cluster_p1.channel import TLSConfig,Grant,_accept_unstarted
from cluster_p1.control_exchange import _need,_json,_parse
from cluster_p1.projection_driver import Spec,nothing
from cluster_p1.split_role_transport import _ssl_snapshot,transport_scope,_result_binding,ResultIngress,_generation
from cluster_p1.tls_material import load_context
from cluster_p1.c_dispatch_gate import CDispatchGate
from cluster_p1.delivery_receipt import _with_cleanup
from isolated_tls_delivery import ReceiveBinding,_binding
from framed_result_transport import MAX_OBJECT

_ORDER=('data','metadata','gate')
_FIELDS=set('version purpose launcher_sha256 spec_sha256 binding_sha256 certificate material_sha256 accept_seconds handshake_seconds close_seconds data metadata gate'.split())
class CResultHost:
 def __init__(self,raw,pin,launcher,spec,binding,tls,material,*,identity=None,authorize=None,gate_identity=None,gate_authorize=None,resources=None):
  _need(type(raw)is bytes and len(raw)<=16384 and hashlib.sha256(raw).hexdigest()==pin)
  c=_parse(raw);_need(set(c) in (_FIELDS,_FIELDS|{'prepare_seconds'}) and type(c['version'])is int and c['version']==1 and c['purpose']=='C-result-host')
  _need(type(launcher)is ReceivingLauncher and type(spec)is Spec and type(binding)is ReceiveBinding and type(tls)is TLSConfig)
  _binding(binding,MAX_OBJECT);_result_binding(binding,spec,binding.scope.incarnation)
  _need(c['launcher_sha256']==launcher.config._sha and c['spec_sha256']==hashlib.sha256(_json(json.loads(json.dumps(asdict(spec))))).hexdigest() and c['binding_sha256']==hashlib.sha256(_json(json.loads(json.dumps(asdict(binding))))).hexdigest())
  _need(type(material)is bytes and hashlib.sha256(material).hexdigest()==c['material_sha256'] and _parse(material)['leaf_sha256']==c['certificate'])
  _need(tls.peers=={binding.scope.peer_node:binding.scope.peer_certificate_der_sha256})
  for k,maximum in (('accept_seconds',30),('handshake_seconds',10),('close_seconds',10)):_need(type(c[k])in(int,float) and 0<c[k]<=maximum)
  _need(type(c.get('prepare_seconds',10))in(int,float) and 0<c.get('prepare_seconds',10)<=30)
  for k in _ORDER:
   e=c[k];_need(type(e)is dict and set(e)=={'bind','port','hostname'} and type(e['port'])is int and 1024<=e['port']<=65535)
   ip=ipaddress.ip_address(e['bind']);_need(str(ip)==e['bind'] and any(ip in ipaddress.ip_network(n) for n in ('127.0.0.0/8','10.0.0.0/8','172.16.0.0/12','192.168.0.0/16','::1/128','fc00::/7')))
  _need(len({(c[k]['bind'],c[k]['port']) for k in _ORDER})==3)
  for p in (identity,authorize,gate_identity,gate_authorize,resources):_need(callable(getattr(p,'hold',None)))
  self.raw=raw;self.c=c;self.launcher=launcher;self.role=launcher.role;self.spec=spec;self.binding=binding
  self.identity=identity;self.authorize=authorize;self.gate_identity=gate_identity;self.gate_authorize=gate_authorize;self.resources=resources
  self.tls=TLSConfig(tls.certfile,tls.keyfile,tls.cafile,dict(tls.peers));self.material=material
  self.loop=asyncio.get_running_loop();self.endpoints={k:_Endpoint(self.loop,c) for k in _ORDER};self.endpoint_pin=tuple(self.endpoints.items())
  self.pin=self._pins();self.started=self.closed=self.claimed=self.attached=self.served=False;self.state='NEW'
  self.data_reader=None;self.ingress_pin=self.gate_pin=None;self.deadline=None;self.timer=None;self.raw_pins={};self.preparing=False;self.streams={};self.transport_cleanup=None;self.actual_pin=None;self.receipt_pin=None;self.work=self.active=self.cleanup=self.failure=None
 def _pins(self):
  r=self.role
  return (self.launcher,self.launcher.config,self.role,r.store,r.channel,r.original,_json(r.d),self.spec,self.binding,self.identity,self.identity.hold,self.authorize,self.authorize.hold,self.gate_identity,self.gate_identity.hold,self.gate_authorize,self.gate_authorize.hold,self.resources,self.resources.hold,r.identity,r.identity.hold,r.authorize,r.authorize.hold,r.maintenance,r.maintenance.hold,r.resources,r.resources.hold,self.tls.certfile,self.tls.keyfile,self.tls.cafile,tuple(sorted(self.tls.peers.items())),self.material)
 def _leaf(self):
  _need(not self.closed and asyncio.get_running_loop() is self.loop and _json(self.c)==self.raw and self._pins()==self.pin)
  _need(self.launcher.role is self.role and tuple(self.endpoints.items())==self.endpoint_pin)
  _need(self.resources is self.role.resources and self.c['launcher_sha256']==self.launcher.config._sha)
  _need(self.c['spec_sha256']==hashlib.sha256(_json(json.loads(json.dumps(asdict(self.spec))))).hexdigest() and self.c['binding_sha256']==hashlib.sha256(_json(json.loads(json.dumps(asdict(self.binding))))).hexdigest())
  if self.deadline is not None:_need(self.loop.time()<self.deadline)
  for k,(reader,writer) in self.streams.items():
   _need(not reader.at_eof() and reader.exception() is None and self._raw_peer(writer)==self.raw_pins[k])
  if self.claimed:_need(self.launcher._result_host is self and self.launcher._result_transport_owner==(self,self.close_transports))
  if self.attached:
   r=self.role;i=r.ingress
   _need(r.spec is self.spec and r.binding==json.loads(json.dumps(asdict(self.binding))))
   _need(type(i)is ResultIngress and self.ingress_pin is i)
   _need(i._reader is self.data_reader and i._writer is self.endpoints['data'].writer and i._meta is self.endpoints['metadata'].channel and i._config is self.tls and i._binding is self.binding)
   _need(i._identity is self.identity and i._authorize is self.authorize and i._receive_authorize is r and i._recipient_node==r.d['c_node'])
   _need(i._ssl==self._raw_peer(i._writer))
   generation=_generation(i._writer);_need(r.result_generation==generation and r.wire_binding==json.loads(json.dumps(asdict(_result_binding(self.binding,self.spec,generation)))))
   if self.gate_pin is not None:
    g=self.gate_pin
    _need(type(g)is CDispatchGate and self.launcher.gate is g and r._dispatch_gate is g and g.role is r and g.channel is self.endpoints['gate'].channel)
    _need(g.identity is self.gate_identity and g.authorize is self.gate_authorize and _json(g.d)==_json(r.d) and g.original==_generation(g.channel.w))

  self.launcher.check();self.role.ready()
 def _receive_leaf(self):
  self._leaf();r=self.role;s=self.spec;row=r.store.history(r.d['owner'],r.d['task'])
  stored=r.store.row(r.d);receipt=(stored[0],stored[1],stored[2],stored[5])
  if self.receipt_pin is None:self.receipt_pin=receipt
  _need(receipt==self.receipt_pin and stored[1]==r.original)
  _need(row is not None and s.request==r.d['task'])
  if self.preparing:
   _need(not self.attached and not r.claimed and row['state']=='PENDING' and not r.receiving and row['actual'] is None)
  else:
   _need(r.receiving and row['state']==('RECEIVING' if self.attached else 'ACTIVE'))
   _need(s.inputs==(row['actual']['input_digest'],))
   value=_json(row['actual'])
   if self.actual_pin is None:self.actual_pin=value
   _need(self.actual_pin==value)
  _need(all(getattr(s,k)==r.d[k] for k in transport_scope(s) if k!='request'))
  _need(self.binding.scope.peer_node==r.d['b_node'] and self.launcher.config.data['node']==r.d['c_node'])
  if not self.attached:_need(not r.claimed and r.ingress is None and self.launcher.gate is None)
 def _preflight(self):
  self._receive_leaf()
  with self.role.fence('pending' if self.preparing else 'receive'),self.role.gate:self._receive_leaf()
  self._receive_leaf()
 def _raw_peer(self,writer):
  snap=_ssl_snapshot(writer)
  _need(snap[0]==self.binding.scope.peer_certificate_der_sha256 and snap[2] in ('TLSv1.2','TLSv1.3') and snap[3]==ssl.CERT_REQUIRED and snap[5] is True)
  return snap
 async def _run(self):
  self._preflight();l=self.launcher;_need(not hasattr(l,'_result_host') and not hasattr(l,'_result_transport_owner'))
  l._result_host=self;l._result_transport_owner=(self,self.close_transports);self.claimed=True;l._jobs.add(asyncio.current_task())
  try:
   contexts={k:load_context(self.tls,self.material,self.c['material_sha256'],server=True,hostname=self.c[k]['hostname']) for k in _ORDER}
   for ctx in contexts.values():_need(type(ctx)is ssl.SSLContext and ctx.verify_mode==ssl.CERT_REQUIRED and not ctx.check_hostname and ctx.minimum_version>=ssl.TLSVersion.TLSv1_2)
   self._preflight()
   for k,slot in self.endpoints.items():
    e=self.c[k];slot.listener=socket.socket(socket.AF_INET6 if ':' in e['bind'] else socket.AF_INET,socket.SOCK_STREAM)
    slot.listener.setblocking(False);slot.listener.bind((e['bind'],e['port']));slot.listener.listen(1)
   self.state='LISTENING';streams={};s=self.spec;grant=Grant(s.request,s.session,s.epoch,1,(0,1),s.model,2)
   for k,slot in self.endpoints.items():
    task=asyncio.create_task(self.loop.sock_accept(slot.listener));slot.pending['accept']=task
    slot.accepted,_=await slot._wait(task,self.c['accept_seconds']);slot.listener.close();slot.listener=None;self._preflight()
    task=asyncio.create_task(_accepted_tls_streams(slot,slot.accepted,contexts[k],self.c['handshake_seconds'],self.c['close_seconds']));slot.pending['handshake']=task
    reader,slot.writer=await slot._wait(task,self.c['handshake_seconds']);self._preflight();self._raw_peer(slot.writer);l._available_writer(slot.writer);_need(all(slot.writer is not w for _,w in streams.values()));streams[k]=(reader,slot.writer)
    self._preflight()
   _need(len({id(w) for _,w in streams.values()})==3);self.streams=streams;self.raw_pins={k:self._raw_peer(w) for k,(_,w) in streams.items()}
   if self.preparing:
    self.state='CONNECTED';self.deadline=self.loop.time()+self.c.get('prepare_seconds',10)
    def expire():
     self.state='UNKNOWN';task=asyncio.create_task(self.close());task.add_done_callback(lambda t:None if t.cancelled() else t.exception())
    self.timer=self.loop.call_later(self.c.get('prepare_seconds',10),expire);return self
   return await self._attach()
  except BaseException as error:
   # Keep the preparing task owned by the launcher until late accept/TLS
   # outcomes are settled or explicitly UNKNOWN. Do not close borrowed C here:
   # an external launcher cleanup may itself be waiting for this task.
   try:await self.close_transports()
   except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
  finally:l._jobs.discard(asyncio.current_task())
 async def _attach(self):
  self._preflight();s=self.spec;l=self.launcher;grant=Grant(s.request,s.session,s.epoch,1,(0,1),s.model,2)
  for k in ('metadata','gate'):
   slot=self.endpoints[k];reader,writer=self.streams[k]
   slot.channel=await _accept_unstarted(reader,writer,self.tls,{self.binding.scope.peer_node:{grant}},capacity_bytes=32768,capacity_tiles=1);self._preflight()
  self.data_reader=self.streams['data'][0]
  l.attach_result(s,*self.streams['data'],self.tls,self.endpoints['metadata'].channel,self.binding,identity=self.identity,authorize=self.authorize)
  self.ingress_pin=l.role.ingress;self.attached=True;self._preflight()
  l.attach_gate(self.endpoints['gate'].channel,identity=self.gate_identity,authorize=self.gate_authorize)
  self.gate_pin=l.gate;self._preflight();self.deadline=None
  if self.timer is not None:self.timer.cancel()
  self.state='READY';return self
 async def prepare(self,*,enabled=False):
  _need(enabled is True and not self.started and not self.closed);self.preparing=True
  return await self.start(enabled=True)
 async def attach_ready(self,*,enabled=False):
  _need(enabled is True and self.state=='CONNECTED' and self.preparing and not self.closed)
  self.preparing=False;self.state='ATTACHING'
  try:return await self.launcher._run(self._attach)
  except BaseException as error:
   self.failure=error;self.state='UNKNOWN'
   if self.cleanup is None:
    try:await self.close()
    except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
 async def close_transports(self):
  if self.timer is not None:self.timer.cancel()
  if self.transport_cleanup is None:
   async def cleanup():
    errors=[]
    for slot in self.endpoints.values():
     try:await slot.close()
     except BaseException as error:errors.append(error)
    if errors:raise BaseExceptionGroup('C result transports unknown',errors)
   self.transport_cleanup=asyncio.create_task(cleanup())
  from private_experiment_assembly import _settle
  await _settle(self.transport_cleanup)
 async def start(self,*,enabled=False):
  _need(enabled is True and not self.started and not self.closed);self.started=True;self.work=asyncio.create_task(self._run())
  try:return await asyncio.shield(self.work)
  except BaseException as error:
   self.failure=error
   if self.claimed:self.state='UNKNOWN'
   try:await self.close()
   except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
 async def serve_once(self,artifacts,check,publication_fence,*,enabled=False):
  _need(enabled is True and self.state=='READY' and not self.served);self._preflight();self.served=True;self.active=asyncio.current_task()
  def checked():
   self._leaf();nothing(check());self._leaf()
  try:
   result=await self.launcher.serve_result_once(artifacts,checked,publication_fence,enabled=True)
   self._leaf();self.state='RECEIVED';return result
  except BaseException as error:
   self.failure=error;self.state='UNKNOWN';self.active=None
   if self.cleanup is None:
    try:await self.close()
    except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
  finally:self.active=None
 async def _cleanup(self):
  errors=[];tasks=[t for t in (self.work,self.active) if t is not None]
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
  if errors:self.state='UNKNOWN';raise BaseExceptionGroup('C result host cleanup unknown',errors)
  if self.state!='UNKNOWN':self.state='CLOSED'
 async def close(self):
  self.closed=True
  if self.cleanup is None:
   self.cleanup=asyncio.create_task(self._cleanup());self.cleanup.add_done_callback(lambda t:None if t.cancelled() else t.exception())
  try:done,_=await asyncio.wait((self.cleanup,),timeout=self.c['close_seconds'])
  except BaseException:self.state='UNKNOWN';raise
  if self.cleanup not in done:self.state='UNKNOWN';raise TimeoutError('C result cleanup pending; retain resource responsibility')
  self.cleanup.result()
