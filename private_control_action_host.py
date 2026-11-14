"""Explicit one-attempt central control/action host; no execution dispatch.

Providers and local endpoint identities must come from trusted bootstrap. This
entry neither discovers providers nor establishes their trust. Existing online
installation and action authority remain the only permission path. Borrowed
installation/resource services are never closed or refunded here.
"""
import asyncio,hashlib,ipaddress,socket,ssl
from dataclasses import asdict
from private_experiment_admission import ExperimentAdmission,need,digest
from private_experiment_installation import _parent,_current
from private_experiment_topology import receive_control
from private_experiment_assembly import assemble_actions
from private_experiment_actions import sample
from private_installation_host import InstallationHost,_accepted_tls_streams
from cluster_p1.channel import TLSConfig,Grant,_accept_unstarted
from cluster_p1.control_exchange import _json,_parse
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing
from cluster_p1.tls_material import load_context
from cluster_p1.delivery_receipt import _with_cleanup

_FIELDS=set('version purpose experiment context stage role_node role_certificate charge accept_seconds handshake_seconds close_seconds control actions'.split())
_ENDPOINT=set('bind port hostname node certificate material_sha256'.split())
class _Endpoint(InstallationHost):
 """Reuse installation host's retained accepted-socket cleanup, not its start."""
 def __init__(self,loop,c):
  self.loop=loop;self.c=c;self.listener=self.accepted=self.writer=self.channel=self.service=None
  self.handed=False;self.work=None;self.pending={};self.cleanup=None;self.failure=None;self.closed=False;self.state='NEW'

 async def _cleanup(self):
  errors=[]
  try:await super()._cleanup()
  except BaseException as error:errors.append(error)
  # Channel.close intentionally suppresses some writer exceptions. This host
  # still owns the original stream and must observe its final close outcome.
  if self.writer is not None and self.channel is not None:
   try:await self.writer.wait_closed()
   except BaseException as error:errors.append(error)
  if errors:self.state='UNKNOWN';raise BaseExceptionGroup('endpoint cleanup unknown',errors)

class ControlActionHost:
 def __init__(self,raw,pin,admission,credential,tls,materials,*,control_identity=None,action_identity=None,clock=None,resources=None):
  need(type(raw)is bytes and len(raw)<=16384 and hashlib.sha256(raw).hexdigest()==pin)
  c=_parse(raw);need(set(c)==_FIELDS and type(c['version'])is int and c['version']==1 and c['purpose']=='central-control-action-host')
  need(type(c['stage'])is int and c['stage'] in (0,1) and type(c['charge'])is int and 0<c['charge']<2**63)
  need(type(admission)is ExperimentAdmission and type(tls)is dict and type(materials)is dict and set(tls)==set(materials)=={'control','actions'})
  for k,maximum in (('accept_seconds',30),('handshake_seconds',10),('close_seconds',10)):need(type(c[k])in(int,float) and 0<c[k]<=maximum)
  for purpose in ('control','actions'):
   e=c[purpose];need(type(e)is dict and set(e)==_ENDPOINT and type(e['port'])is int and 1024<=e['port']<=65535)
   ip=ipaddress.ip_address(e['bind']);need(str(ip)==e['bind'] and any(ip in ipaddress.ip_network(n) for n in ('127.0.0.0/8','10.0.0.0/8','172.16.0.0/12','192.168.0.0/16','::1/128','fc00::/7')))
   t=tls[purpose];m=materials[purpose]
   need(type(t)is TLSConfig and t.peers=={c['role_node']:c['role_certificate']})
   need(type(m)is bytes and hashlib.sha256(m).hexdigest()==e['material_sha256'] and _parse(m)['leaf_sha256']==e['certificate'])
  need((c['control']['bind'],c['control']['port'])!=(c['actions']['bind'],c['actions']['port']))
  for p in (control_identity,action_identity,resources):need(callable(getattr(p,'hold',None)))
  need(callable(getattr(clock,'sample',None)))
  self.raw=raw;self.c=c;self.a=admission;self.credential=credential;self.resources=resources;self.clock=clock
  self.control_identity=control_identity;self.action_identity=action_identity
  self.tls={k:TLSConfig(t.certfile,t.keyfile,t.cafile,dict(t.peers)) for k,t in tls.items()};self.materials=dict(materials)
  self.pin=self._pins();self.loop=asyncio.get_running_loop();self.endpoints={k:_Endpoint(self.loop,c) for k in ('control','actions')}
  self.control=self.assembly=None;self.work=self.cleanup=self.active=None;self.failure=None
  self.installation_pin=None;self.started=self.closed=self.claimed=False;self.action_count=0;self.state='NEW'
 def _pins(self):
  return (self.a,self.credential,self.resources,self.resources.hold,self.clock,self.clock.sample,self.control_identity,self.control_identity.hold,self.action_identity,self.action_identity.hold,tuple((k,t.certfile,t.keyfile,t.cafile,tuple(sorted(t.peers.items())),self.materials[k]) for k,t in sorted(self.tls.items())))
 def check(self):
  need(not self.closed and asyncio.get_running_loop() is self.loop and _json(self.c)==self.raw and self._pins()==self.pin)
 def _installation_leaf(self,v,expected):
  c=self.c;s=v['service']
  need(self.a._installations.get((c['experiment'],c['stage'])) is v)
  need(all(x is y for x,y in zip((v,s,s._session,v['descriptor']),expected)))
  if self.installation_pin is not None:need(all(x is y for x,y in zip(expected,self.installation_pin)))
  if self.claimed:need(self.a._control_hosts.get((c['experiment'],c['stage'])) is self)
  self.check()
 async def _preflight(self):
  self.check();sample(self.clock);self.check();a=self.a;c=self.c;consent=a.consent
  async with consent.authority._lock:
   v=a._installations.get((c['experiment'],c['stage']));need(v is not None and v['attempted'] and v['credential']==self.credential and v['sha']==c['context'])
   with consent._user(self.credential,v['owner']):
    _current(a,v);s=v['service'];need(s._session is not None);s.ready()
    current=(v,s,s._session,v['descriptor'])
    if self.installation_pin is not None:need(all(x is y for x,y in zip(current,self.installation_pin)))
    if self.claimed:need(a._control_hosts.get((c['experiment'],c['stage'])) is self)
    need(self.resources is a._issuer_resources and self.resources is s.resources)
    need((s._identity.node_id,s._identity.certificate_sha256)==(c['role_node'],c['role_certificate']))
    need(s._issuer_identity==dict(node_id=c['actions']['node'],certificate_sha256=c['actions']['certificate']))
    with _parent(a,v,'prepare-control-host',intent=c),_non_suppressing(self.resources.hold(v['descriptor']['resource_id'],v['descriptor'])) as held:nothing(held);self.check()
   _current(a,v);self._installation_leaf(v,current)
  self._installation_leaf(v,current);self.installation_pin=current;return v
 async def _run(self):
  v=await self._preflight();c=self.c;key=(c['experiment'],c['stage'])
  if not hasattr(self.a,'_control_hosts'):self.a._control_hosts={}
  need(key not in self.a._control_hosts);self.a._control_hosts[key]=self;self.claimed=True
  contexts={}
  for k in self.endpoints:
   e=c[k];ctx=load_context(self.tls[k],self.materials[k],e['material_sha256'],server=True,hostname=e['hostname'])
   need(type(ctx)is ssl.SSLContext and ctx.verify_mode==ssl.CERT_REQUIRED and not ctx.check_hostname and ctx.minimum_version>=ssl.TLSVersion.TLSv1_2);contexts[k]=ctx
  await self._preflight()
  # Both ports exist before the sequential role dial can complete its first one.
  for k,slot in self.endpoints.items():
   e=c[k];slot.listener=socket.socket(socket.AF_INET6 if ':' in e['bind'] else socket.AF_INET,socket.SOCK_STREAM)
   slot.listener.setblocking(False);slot.listener.bind((e['bind'],e['port']));slot.listener.listen(1)
  self.state='LISTENING';d=v['descriptor'];grant=Grant(d['task'],d['nonce'],d['epoch'],c['stage'],(0,1),d['declaration']['task']['model_hash'],2)
  for k,slot in self.endpoints.items():
   task=asyncio.create_task(self.loop.sock_accept(slot.listener));slot.pending['accept']=task
   slot.accepted,_=await slot._wait(task,c['accept_seconds']);slot.listener.close();slot.listener=None
   await self._preflight();self.state='HANDSHAKE'
   task=asyncio.create_task(_accepted_tls_streams(slot,slot.accepted,contexts[k],c['handshake_seconds'],c['close_seconds']));slot.pending['handshake']=task
   reader,slot.writer=await slot._wait(task,c['handshake_seconds']);await self._preflight()
   slot.channel=await _accept_unstarted(reader,slot.writer,self.tls[k],{c['role_node']:{grant}},capacity_bytes=32768,capacity_tiles=1)
   await slot.channel.start();await self._preflight()
  self.state='BOOTSTRAP'
  self.control=await receive_control(self.a,self.credential,c['experiment'],c['context'],c['stage'],self.endpoints['control'].channel,identity=self.control_identity,resources=self.resources,expected_charge=c['charge'])
  await self._preflight();d=self.control.descriptor;channel=self.endpoints['actions'].channel
  config=dict(version=1,purpose='central-action-assembly',experiment=v['pid'],context=v['sha'],stage=v['stage'],placement={k:v['descriptor'][k] for k in ('node','device','instance','resource_id')},charge=d.charge,control_sha256=digest(dict(command=d.command_id,reservation=d.reservation_id,binding=d.binding,charge=d.charge)),control_peer=asdict(self.control._client._identity),action_peer=asdict(channel.authenticated_identity),issuer=dict(v['service']._issuer_identity))
  raw=_json(config)
  self.assembly=await assemble_actions(raw,hashlib.sha256(raw).hexdigest(),self.a,self.credential,self.control,channel,enabled=True,identity=self.action_identity,clock=self.clock,resources=self.resources)
  await self._preflight();self.state='READY';return self
 async def start(self,*,enabled=False):
  need(enabled is True and not self.started and not self.closed);self.started=True;self.work=asyncio.create_task(self._run())
  try:return await asyncio.shield(self.work)
  except BaseException as error:
   self.failure=error
   if self.claimed:self.state='UNKNOWN';self.a._unknown(self.c['experiment'])
   try:await self.close()
   except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
 async def serve_action_once(self):
  need(self.active is None and self.state=='READY' and self.action_count<2);self.check()
  self.active=asyncio.current_task();self.action_count+=1
  try:await self._preflight();await self.assembly.serve_once();await self._preflight()
  except BaseException as error:
   self.failure=error;self.state='UNKNOWN';self.a._unknown(self.c['experiment'])
   # Self-owned failure must not join itself via cleanup. An external closer
   # already captured this task and will retain its original exception.
   self.active=None
   if self.cleanup is None:
    try:await self.close()
    except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
  finally:self.active=None
 async def _cleanup(self):
  errors=[]
  tasks=[t for t in (self.work,self.active) if t is not None]
  for task in tasks:
   if not task.done():task.cancel()
  for task in tasks:
   try:await task
   except asyncio.CancelledError as e:
    if e.__cause__ is not None or e.__context__ is not None:errors.append(e)
   except BaseException as e:
    if e is not self.failure:errors.append(e)
  for obj in (self.assembly,self.control,*self.endpoints.values()):
   if obj is not None:
    try:await obj.close()
    except BaseException as e:errors.append(e)
  if errors:self.state='UNKNOWN';raise BaseExceptionGroup('control/action host cleanup unknown',errors)
  if self.state!='UNKNOWN':self.state='CLOSED'
 async def close(self):
  self.closed=True
  if self.cleanup is None:
   self.cleanup=asyncio.create_task(self._cleanup());self.cleanup.add_done_callback(lambda t:None if t.cancelled() else t.exception())
  try:done,_=await asyncio.wait((self.cleanup,),timeout=self.c['close_seconds'])
  except BaseException:self.state='UNKNOWN';raise
  if self.cleanup not in done:self.state='UNKNOWN';raise TimeoutError('control/action cleanup pending; retain resource responsibility')
  self.cleanup.result()
