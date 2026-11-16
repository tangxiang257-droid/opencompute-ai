"""Private one-attempt installation listener. Disabled until explicit start.

One planned stage, one TCP accept, one TLS handshake, one existing installation.
No provider discovery, execution dispatch, retries or quota release. The host
retains session and cleanup ownership; close timeout means UNKNOWN, not stopped.
"""
import asyncio,hashlib,ipaddress,socket,ssl
from private_experiment_admission import ExperimentAdmission,need
from private_experiment_installation import _parent,_current
from cluster_p1.channel import TLSConfig,Grant,_accept_unstarted
from cluster_p1.control_exchange import _json,_parse
from cluster_p1.installation_session import SessionPolicy
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing
from cluster_p1.role_startup import _close_channel
from cluster_p1.tls_material import load_context
from cluster_p1.delivery_receipt import _with_cleanup

_FIELDS=set('version purpose experiment context stage bind port issuer_node issuer_certificate server_hostname role_node role_certificate material_sha256 accept_seconds handshake_seconds close_seconds'.split())
class InstallationHost:
 def __init__(self,raw,pin,admission,credential,tls,grants,material,*,identity=None,authorize=None,resources=None):
  need(type(raw)is bytes and len(raw)<=16384 and hashlib.sha256(raw).hexdigest()==pin)
  c=_parse(raw);need(set(c)==_FIELDS and c['version']==1 and type(c['version'])is int and c['purpose']=='one-installation-host')
  need(type(c['stage'])is int and c['stage'] in (0,1) and type(c['port'])is int and 1024<=c['port']<=65535)
  ip=ipaddress.ip_address(c['bind']);need(str(ip)==c['bind'] and any(ip in ipaddress.ip_network(n) for n in ('127.0.0.0/8','10.0.0.0/8','172.16.0.0/12','192.168.0.0/16','::1/128','fc00::/7')))
  for k,maximum in (('accept_seconds',30),('handshake_seconds',10),('close_seconds',10)):need(type(c[k])in(int,float) and 0<c[k]<=maximum)
  need(type(admission)is ExperimentAdmission and type(tls)is TLSConfig)
  need(type(material)is bytes and hashlib.sha256(material).hexdigest()==c['material_sha256'])
  need(_parse(material)['leaf_sha256']==c['issuer_certificate'])
  need(tls.peers=={c['role_node']:c['role_certificate']})
  need(type(grants)is dict and set(grants)=={c['role_node']} and bool(grants[c['role_node']]) and all(type(g)is Grant for g in grants[c['role_node']]))
  for p in (identity,authorize,resources):need(callable(getattr(p,'hold',None)))
  self.raw=raw;self.pin=pin;self.c=c;self.a=admission;self.credential=credential
  self.tls=TLSConfig(tls.certfile,tls.keyfile,tls.cafile,dict(tls.peers));self.grants={k:set(v) for k,v in grants.items()};self.material=material
  self.transport_pin=(self.tls.certfile,self.tls.keyfile,self.tls.cafile,dict(self.tls.peers),{k:set(v) for k,v in self.grants.items()},material,credential)
  self.identity=identity;self.authorize=authorize;self.resources=resources;self.loop=asyncio.get_running_loop()
  self.fixed=(admission,identity,identity.hold,authorize,authorize.hold,resources,resources.hold,admission.installation_service)
  self.listener=None;self.accepted=None;self.writer=None;self.channel=None;self.service=None;self.handed=False
  self.work=None;self.pending={};self.cleanup=None;self.failure=None;self.started=False;self.closed=False;self.state='NEW'
 def check(self):
  need(not self.closed and asyncio.get_running_loop() is self.loop and _json(self.c)==self.raw)
  need(self.transport_pin==(self.tls.certfile,self.tls.keyfile,self.tls.cafile,self.tls.peers,self.grants,self.material,self.credential))
  need(self.fixed==(self.a,self.identity,self.identity.hold,self.authorize,self.authorize.hold,self.resources,self.resources.hold,self.a.installation_service))
 async def _preflight(self):
  self.check();a=self.a;c=self.c;consent=a.consent
  async with consent.authority._lock:
   a._ready();owner=consent.service._download_owner(self.credential)
   with consent._user(self.credential,owner):
    b,row=a._valid_current(self.credential,owner,c['experiment'],{'REGISTERED'});need(row[1]==c['context'])
    reg=a._registered.get(c['experiment']);need(reg is not None and self.resources is a._issuer_resources and self.resources is a._issuer.resources)
    place=b['placements'][c['stage']];need(place['node']==c['role_node'])
    d=a._issuer.describe(reg,c['stage'],**{k:place[k] for k in ('node','device','instance','resource_id')})
    expected=Grant(d['task'],d['nonce'],d['epoch'],c['stage'],(0,1),d['declaration']['task']['model_hash'],2)
    need(self.grants=={c['role_node']:{expected}})
    v=dict(credential=self.credential,owner=owner,pid=c['experiment'],sha=c['context'],body=b,stage=c['stage'],descriptor=d,registration=reg,issuer=a._issuer,service=None,attempted=False)
    with _parent(a,v,'prepare-installation'),_non_suppressing(self.resources.hold(d['resource_id'],d)) as held:nothing(held);self.check()
   _current(a,v);self.check()
  self.check();return d
 async def _wait(self,task,seconds):
  done,_=await asyncio.wait((task,),timeout=seconds)
  if task not in done:raise TimeoutError('installation host phase incomplete')
  return task.result()
 async def _handshake(self,sock,ctx):
  return await _accepted_tls_streams(self,sock,ctx,self.c['handshake_seconds'],self.c['close_seconds'])
 async def _run(self):
  d=await self._preflight();c=self.c;key=(c['experiment'],c['stage'])
  need(key not in self.a._installations)
  if not hasattr(self.a,'_installation_hosts'):self.a._installation_hosts={}
  need(key not in self.a._installation_hosts);self.a._installation_hosts[key]=self
  ctx=load_context(self.tls,self.material,c['material_sha256'],server=True,hostname=c['server_hostname'])
  need(type(ctx)is ssl.SSLContext and ctx.verify_mode==ssl.CERT_REQUIRED and not ctx.check_hostname and ctx.minimum_version>=ssl.TLSVersion.TLSv1_2)
  await self._preflight();need(self.a._installation_hosts[key] is self)
  self.listener=socket.socket(socket.AF_INET6 if ':' in c['bind'] else socket.AF_INET,socket.SOCK_STREAM)
  self.listener.setblocking(False);self.listener.bind((c['bind'],c['port']));self.listener.listen(1);self.state='LISTENING'
  accept=asyncio.create_task(self.loop.sock_accept(self.listener));self.pending['accept']=accept
  self.accepted,_=await self._wait(accept,c['accept_seconds'])
  self.listener.close();self.listener=None;self.check()
  await self._preflight();self.state='HANDSHAKE'
  handshake=asyncio.create_task(self._handshake(self.accepted,ctx));self.pending['handshake']=handshake
  reader,self.writer=await self._wait(handshake,c['handshake_seconds']);self.check();await self._preflight()
  self.channel=await _accept_unstarted(reader,self.writer,self.tls,self.grants,capacity_bytes=32768,capacity_tiles=1)
  self.service=await self.a.installation_service(self.credential,c['experiment'],c['context'],c['stage'],self.channel,issuer_node=c['issuer_node'],issuer_certificate=c['issuer_certificate'],identity=self.identity,authorize=self.authorize,resources=self.resources)
  self.check();self.state='INSTALLING';await self.service.serve_once(session_policy=SessionPolicy());self.check()
  self.state='ACTIVE';return self
 async def start(self,*,enabled=False):
  need(enabled is True and not self.started and not self.closed);self.started=True
  self.work=asyncio.create_task(self._run())
  try:return await asyncio.shield(self.work)
  except BaseException as error:
   self.failure=error
   try:await self.close()
   except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
 async def _cleanup(self):
  errors=[]
  if self.listener is not None:
   try:self.listener.close()
   except BaseException as e:errors.append(e)
   else:self.listener=None
  if self.work is not None and not self.work.done():self.work.cancel()
  for name,task in list(self.pending.items()):
   if not task.done():task.cancel()
   try:
    result=await task
    if name=='accept':self.accepted=result[0]
    else:_,self.writer=result
   except asyncio.CancelledError as e:
    if e.__cause__ is not None or e.__context__ is not None:errors.append(e)
   except BaseException as e:
    if e is not self.failure:errors.append(e)
  if self.work is not None:
   try:await self.work
   except asyncio.CancelledError as e:
    if e.__cause__ is not None or e.__context__ is not None:errors.append(e)
   except BaseException as e:
    if e is not self.failure:errors.append(e)
  try:
   if self.service is not None:await self.service.close()
   elif self.channel is not None:await _close_channel(self.channel)
   elif self.writer is not None:
    try:self.writer.close()
    except BaseException as e:errors.append(e)
    try:await self.writer.wait_closed()
    except BaseException as e:errors.append(e)
   elif self.handed:raise RuntimeError('TLS transport cleanup unobserved; isolate host')
  except BaseException as e:errors.append(e)
  if self.accepted is not None and not self.handed:
   try:self.accepted.close()
   except BaseException as e:errors.append(e)
  if errors:self.state='UNKNOWN';raise BaseExceptionGroup('installation host cleanup unknown',errors)
  if self.state!='UNKNOWN':self.state='CLOSED'
 async def close(self):
  self.closed=True
  if self.cleanup is None:
   self.cleanup=asyncio.create_task(self._cleanup())
   self.cleanup.add_done_callback(lambda t:None if t.cancelled() else t.exception())
  try:done,_=await asyncio.wait((self.cleanup,),timeout=self.c['close_seconds'])
  except BaseException:self.state='UNKNOWN';raise
  if self.cleanup not in done:self.state='UNKNOWN';raise TimeoutError('installation host cleanup pending; isolate host')
  self.cleanup.result()

async def _accepted_tls_streams(owner,sock,ctx,handshake_seconds,close_seconds):
 """Capture TLS ownership even if handshake fails after transport creation."""
 reader=asyncio.StreamReader(limit=32768)
 class Protocol(asyncio.StreamReaderProtocol):
  def connection_made(self,transport):
   super().connection_made(transport)
   owner.writer=asyncio.StreamWriter(transport,self,reader,owner.loop)
 protocol=Protocol(reader);owner.handed=True
 transport,_=await owner.loop.connect_accepted_socket(lambda:protocol,sock,ssl=ctx,ssl_handshake_timeout=handshake_seconds,ssl_shutdown_timeout=close_seconds)
 if owner.writer is None:owner.writer=asyncio.StreamWriter(transport,protocol,reader,owner.loop)
 return reader,owner.writer
