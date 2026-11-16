"""Configured central action assembly, not a production host or permission issuer.

Only trusted local bootstrap supplies objects and an independently pinned public
configuration. Method shapes/enable flags cannot establish provider trust. This
entry does not discover providers, create credentials, connect/listen, install a
role, dispatch computation, or release quotas. B still requires committed input.
"""
import asyncio,hashlib
from dataclasses import asdict
from private_experiment_admission import ExperimentAdmission,need,digest
from private_experiment_actions import sample
from private_experiment_control import create_action_server
from cluster_p1.channel import Channel
from cluster_p1.role_control import CoordinatorControl
from cluster_p1.control_exchange import _json,_parse
from cluster_p1.role_startup import _close_channel
from cluster_p1.delivery_receipt import _with_cleanup

_FIELDS={'version','purpose','experiment','context','stage','placement','charge','control_sha256','control_peer','action_peer','issuer'}
_FLAGS=('_descriptor_bootstrap_attempted','_experiment_action_owned','_launcher_owned','_online_issuance_claimed','_receipt_state','_installed_role_control_claimed','_role_control_claimed','_dispatch_gate_claimed','_c_receiving_claimed')

def _configuration(raw,pin):
 need(type(raw)is bytes and 0<len(raw)<=32768 and type(pin)is str and len(pin)==64)
 need(hashlib.sha256(raw).hexdigest()==pin)
 d=_parse(raw);need(set(d)==_FIELDS and type(d['version'])is int and d['version']==1 and d['purpose']=='central-action-assembly')
 need(type(d['stage'])is int and d['stage'] in (0,1))
 need(type(d['experiment'])is str and 0<len(d['experiment'])<=128 and type(d['context'])is str and len(d['context'])==64)
 return d

def _binding(a,credential,control,channel,d,resources):
 need(type(a)is ExperimentAdmission and type(control)is CoordinatorControl and type(channel)is Channel)
 v=a._installations.get((d['experiment'],d['stage']));need(v is not None and v['attempted'])
 need(v['credential']==credential and v['sha']==d['context'] and resources is not None and resources is a._issuer_resources and resources is v['service'].resources)
 desc=control.descriptor;scope=dict(command=desc.command_id,reservation=desc.reservation_id,binding=desc.binding,charge=desc.charge)
 expected=dict(version=1,purpose='central-action-assembly',experiment=v['pid'],context=v['sha'],stage=v['stage'],
  placement={k:v['descriptor'][k] for k in ('node','device','instance','resource_id')},charge=desc.charge,
  control_sha256=digest(scope),control_peer=asdict(control._client._identity),action_peer=asdict(channel.authenticated_identity),issuer=dict(v['service']._issuer_identity))
 need(_json(d)==_json(expected))
 need(desc.role_node==d['placement']['node'])
 need((channel.authenticated_identity.node_id,channel.authenticated_identity.certificate_sha256)==(v['service']._identity.node_id,v['service']._identity.certificate_sha256))
 # These are central endpoints. issuer is the original configured LOCAL issuer,
 # not the role peer; its trust is supplied by bootstrap, not proved by JSON.
 borrowed=(control._client.channel,v['service'].channel)
 need(all(channel is not c and channel.w is not c.w for c in borrowed))
 return v

class ActionAssembly:
 def __init__(self):raise TypeError('use assemble_actions')
 @property
 def authority(self):return self.server.authority
 @property
 def channel(self):return self._channel
 def _leaf(self):
  need(not self.closed and asyncio.get_running_loop() is self.loop)
  need(self.pin==(self.raw,self.sha,self.a,self.credential,self.control,self._channel,self.identity,self.identity.hold,self.clock,self.clock.sample,self.resources,self.resources.hold,self.installation,self.experiment))
  d=_configuration(self.raw,self.sha);need(_binding(self.a,self.credential,self.control,self._channel,d,self.resources) is self.installation)
  need(self.server_pin==(self.server,self.server.serve_once,self.server.authority,self.server.authority.authorize))
 def check(self):
  self._leaf();self.server_pin[0].check();self.server_pin[3]._current();self._leaf()
 async def serve_once(self):
  # A concurrent caller is rejected without canceling the existing owner.
  need(self.active_task is None and not self.closed)
  self.active_task=asyncio.current_task()
  try:
   self.check();await self.server_pin[1]();self.check()
  except BaseException as error:
   self.pin[2]._unknown(self.pin[-1])
   try:await self.close()
   except BaseException as cleanup:raise _with_cleanup(error,cleanup)
   raise
  finally:self.active_task=None
 async def close(self):
  self.closed=True
  server=self.server_pin[0];server.dead=True;self.server_pin[2].dead=True;self.server_pin[3].dead=True
  if self.close_task is None:
   caller=asyncio.current_task();active=self.active_task
   async def cleanup():
    if active is not None and active is not caller and not active.done():active.cancel()
    errors=[]
    try:await _close_channel(self.pin[5])
    except BaseException as error:errors.append(error)
    if active is not None and active is not caller:
     results=await asyncio.gather(active,return_exceptions=True)
     errors.extend(x for x in results if isinstance(x,BaseException) and not isinstance(x,asyncio.CancelledError))
    if errors:raise BaseExceptionGroup('action assembly cleanup unknown',errors)
   self.close_caller=caller;self.close_task=asyncio.create_task(cleanup())
  # A serve cancellation/error must not recursively join its own cleanup.
  if asyncio.current_task() is self.active_task and asyncio.current_task() is not self.close_caller:return
  await _settle(self.close_task)

async def _settle(task):
 """Retain and join cleanup despite repeated caller cancellation; no refund."""
 cancelled=None
 while not task.done():
  try:await asyncio.shield(task)
  except asyncio.CancelledError as e:cancelled=e
  except BaseException:break
 try:task.result()
 except BaseException as e:
  if cancelled is not None:raise _with_cleanup(cancelled,e)
  raise
 if cancelled is not None:raise cancelled

async def assemble_actions(raw,pin,a,credential,control,channel,*,enabled=False,identity=None,identity_mode=None,clock=None,resources=None):
 """Take only a validated dedicated action Channel; never automatically serve.

 Before take, caller retains its channel and borrowed services on rejection.
 After take, any failure permanently seals this attempt and closes only the
 owned channel. Existing installation/control/resource services stay borrowed.
 No local fence is held across asynchronous control proof or cleanup.
 """
 need(enabled is True);d=_configuration(raw,pin)
 need(identity_mode is None or type(identity_mode)is str and identity_mode=='channel')
 need(callable(getattr(resources,'hold',None)) and callable(getattr(clock,'sample',None)))
 if identity_mode is None:need(callable(getattr(identity,'hold',None)))
 else:need(identity is None)
 callbacks=(clock.sample,resources.hold);original_identity=getattr(identity,'hold',None)
 sample(clock)
 installation=_binding(a,credential,control,channel,d,resources)
 need(callbacks==(clock.sample,resources.hold) and original_identity==getattr(identity,'hold',None))
 if identity_mode=='channel':
  from cluster_p1.scoped_identity import ScopedChannelIdentity
  descriptor=control.descriptor
  scope=dict(command=descriptor.command_id,reservation=descriptor.reservation_id,binding=descriptor.binding,charge=descriptor.charge)
  identity=ScopedChannelIdentity(channel,scope,expected_peer=d['action_peer'])
 need(callbacks==(clock.sample,resources.hold))
 need(channel._start_attempted and not channel.w.is_closing() and (channel.capacity_bytes,channel.capacity_tiles)==(32768,1))
 with channel.identity_fence(channel.authenticated_identity):pass
 need(not any(getattr(channel,k,False) or getattr(channel.w,k,False) for k in _FLAGS))
 pins=(raw,pin,a,credential,control,channel,identity,identity.hold,clock,clock.sample,resources,resources.hold,installation,d['experiment'])
 # Permanent connection occupancy, not another permission or quota record.
 channel._experiment_action_owned=True;channel.w._experiment_action_owned=True
 try:
  server=await create_action_server(a,credential,d['experiment'],d['context'],d['stage'],control,channel,identity=identity,clock=clock)
  self=object.__new__(ActionAssembly);self.raw=raw;self.sha=pin;self.a=a;self.credential=credential;self.control=control;self._channel=channel
  self.identity=identity;self.clock=clock;self.resources=resources;self.installation=installation;self.experiment=d['experiment']
  self.pin=pins
  self.server=server;self.server_pin=(server,server.serve_once,server.authority,server.authority.authorize)
  self.loop=asyncio.get_running_loop();self.closed=False;self.active_task=None;self.close_task=None
  with server.fence():self.check()
  self.check();return self
 except BaseException as error:
  a._unknown(d['experiment'])
  cleanup=asyncio.create_task(_close_channel(channel))
  try:await _settle(cleanup)
  except BaseException as cleanup:raise _with_cleanup(error,cleanup)
  raise
