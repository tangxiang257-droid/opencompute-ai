"""Private B dispatch prerequisite over a separate authenticated B/C Channel.

ACK is not a task permission or device/resource grant. C must already own its
actual ingress under its independent receiving authorization. Unknown exchanges
burn their local slots; no reconnect/restart restores a gate. A later remote
revocation not yet observed by B cannot atomically prevent B computation. C's
own receiving fences still reject revoked output. No network lock is held.
"""
import secrets
from contextlib import contextmanager
from .c_receiving import ReceivingRole,descriptor
from .history_query import _HistoryEndpoint,_context
from .control_exchange import _need,_nothing,_json,_parse
from .control_journal import _binding
from .online_issuance_transport import _hash,_copy
from .split_role_transport import _generation


def _matches(b,d,proof):
 _binding(b)
 expected=dict(owner=d['owner'],task=d['task'],session=d['session'],epoch=d['epoch'],stage=1,
  model=d['model'],layout=d['layout'],plan=d['plan_digest'],input_snapshot=d['input_snapshot'],
  token_digest=d['token_digest'],preprocessing=d['preprocessing'],declaration_digest=d['declaration_digest'],
  device=d['b_placement']['device'],instance=d['b_placement']['instance'],input_digest=proof['input_digest'])
 _need(all(b.get(k)==v for k,v in expected.items()))

class _Gate(_HistoryEndpoint):
 def __init__(self,channel,d,identity,authorize):
  super().__init__(channel);self.d=descriptor(d);self.identity=identity;self.authorize=authorize
  _need(not channel._start_attempted and not getattr(channel,'_dispatch_gate_claimed',False))
  channel._dispatch_gate_claimed=True;channel._single_start=True
  self.original=_generation(channel.w);self.used=False;self.started=False
  self.scope=dict(task=d['task'],session=d['session'],epoch=d['epoch'],stage=1,model=d['model'])
 @contextmanager
 def fence(self):
  _need(not self._closed);self.current();_need(_generation(self.channel.w)==self.original)
  _need(callable(getattr(self.identity,'hold',None)) and callable(getattr(self.authorize,'hold',None)))
  with self.channel.identity_fence(self._identity),_context(self.identity.hold(self._identity)) as who:
   _need(who==self._identity.node_id)
   with _context(self.authorize.hold(_copy(self.d),'dispatch-ack')) as allowed:_nothing(allowed);yield
 async def start(self):
  with self.fence():pass
  if not self.started:self.started=True;await self.channel.start()
 async def send(self,value,fence):
  await self.channel.send(self.envelope(self.scope,0,_json(value)),max_frame_bytes=65536,write_fence=fence)
 async def read(self):
  delivery=await self.channel.receive()
  try:
   self.current();self.validate(delivery.envelope,self.scope,0);v=_parse(delivery.envelope.payload)
   await delivery.release();return v
  finally:delivery.envelope=None

class BDispatchGate(_Gate):
 """An exact installed B owns this handle; an ACK map cannot install it."""
 def __init__(self,channel,role,d,*,identity=None,authorize=None):
  from .role_assembly import InstalledStageRole
  _need(type(role)is InstalledStageRole and role.startup.stage==1)
  role._check();service=role.role._service
  _need(getattr(service,'_receiver_gate',None) is None and role.journal.require_receiving_gate)
  super().__init__(channel,d,identity,authorize)
  _need(self._identity.node_id==self.d['c_node'] and service.node==self.d['b_node'])
  _matches(service._call[2],self.d,dict(input_digest=role.operation.spec.inputs[0]))
  _need(role.startup.descriptor['resource_id']==self.d['b_placement']['resource_id'])
  self.role=role;self.service=service;self.binding=_copy(service._call[2]);self.acked=False
  self.nonce=secrets.token_hex(32);self.recipient=None;service._receiver_gate=self
 def check(self,service,binding):
  _need(service is self.service and service._receipt_owner is self.role and service._receiver_gate is self)
  _need(self.acked and _json(binding)==_json(self.binding));self.role._check()
  with self.fence():
   with service.journal._lock:
    row=service.journal._db.execute('SELECT binding,nonce,recipient,state FROM control_receiving_gate WHERE command_id=?',(service._call[0],)).fetchone()
    _need(row is not None and row[:3]==(_json(binding).decode(),self.nonce,self.recipient) and row[3] in ('ACKED','CONSUMED'))
  # Provider exit callbacks may have closed the transport. No external callback
  # or await follows this final local connection check.
  self.current();_need(not self._closed and _generation(self.channel.w)==self.original)
 async def acquire(self,service,binding):
  _need(service is self.service and not self.used and _json(binding)==_json(self.binding));self.used=True
  try:
   self.role._check()
   command,reservation,_,_=service._call
   request=dict(version=1,purpose='C-dispatch-ack',descriptor=self.d,binding=self.binding,command=command,reservation=reservation,nonce=self.nonce)
   await self.start();await self.send(request,self.fence);reply=await self.read()
   _need(type(reply)is dict and set(reply)=={'request','recipient','state','execution_allowed'} and reply['request']==_hash(request) and reply['state']=='ACKED' and reply['execution_allowed'] is False)
   with self.fence(),service.fence(binding,'execute'):
    self.recipient=reply['recipient'];service.journal.acknowledge_receiver(command,reservation,binding,self.nonce,self.recipient)
   self.acked=True;self.check(service,binding)
  except BaseException as error:await self._failed(error)

class CDispatchGate(_Gate):
 """C's original receiving role, not a copied controller receipt, supplies ACK."""
 def __init__(self,channel,role,*,identity=None,authorize=None):
  _need(type(role)is ReceivingRole);super().__init__(channel,role.d,identity,authorize)
  _need(self._identity.node_id==self.d['b_node'] and not getattr(role,'_dispatch_gate_claimed',False))
  role._dispatch_gate_claimed=True;self.role=role;role._dispatch_gate=self
  self._control_reader=role.channel._task
  def disconnected(_):
   role.receiving=False;self.channel._abort()
  self._control_reader.add_done_callback(disconnected)
  self._disconnect_callback=disconnected
  if self._control_reader.done():disconnected(self._control_reader)
 @contextmanager
 def receiving(self):
  r=self.role
  with self.fence(),r.fence('receive'),r.gate:
   row=r.store.history(r.d['owner'],r.d['task'])
   _need(r.receiving and r.claimed and r.ingress is not None and row is not None and row['state']=='RECEIVING')
   yield row
 async def close(self):
  self._control_reader.remove_done_callback(self._disconnect_callback)
  await super().close()
 async def serve_once(self):
  _need(not self.used);self.used=True
  try:
   await self.start();request=await self.read()
   _need(type(request)is dict and set(request)=={'version','purpose','descriptor','binding','command','reservation','nonce'})
   _need(type(request['version'])is int and request['version']==1 and request['purpose']=='C-dispatch-ack' and request['descriptor']==self.d)
   from .role_control import _ids
   b=request['binding'];_need(_ids(b)==(request['command'],request['reservation']))
   nonce=request['nonce'];_need(type(nonce)is str and len(nonce)==64 and all(c in '0123456789abcdef' for c in nonce))
   with self.receiving() as row:
    _matches(b,self.d,row['actual'])
    r=self.role
    with r.store._tx() as c:
     r.store._check(True)
     c.execute("INSERT INTO c_dispatch_gate VALUES(?,?,?,?,?,'ACKED')",(self.d['owner'],self.d['task'],_json(b).decode(),nonce,self.original))
   @contextmanager
   def write_fence():
    with self.receiving():yield None
   await self.send(dict(request=_hash(request),recipient=r.store.incarnation,state='ACKED',execution_allowed=False),write_fence)
  except BaseException as error:
   try:await self.role.close()
   except BaseException as cleanup:error=BaseExceptionGroup('C gate unknown',[error,cleanup])
   await self._failed(error)
