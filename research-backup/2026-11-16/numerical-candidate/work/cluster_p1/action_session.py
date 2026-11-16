"""Explicit original action-connection liveness; never a short execution lease.

OPEN/OPENED owns only the initial D sequence. Native bounded P then leaves the
action RPC consumer untouched. Missing original authority/providers reject.
"""
import asyncio,json,secrets
from contextlib import contextmanager
from .data_session import DataSession,DataSessionPolicy
from .control_exchange import _json,_parse,_need
from .online_issuance_transport import _hash
from .local_attempt import _non_suppressing
from .projection_driver import nothing

class _Transport:
 def __init__(self,e,server):
  self.e=e;self.server=server;self.identity=e.identity;self.method=e.identity.hold
  self.scope=json.loads(_json(e.authority.scope if server else e.scope()))
  if server:
   a=e.authority;self.original=(a,a.authorize,a.v,a.v['service'])
   descriptor=a.v['descriptor'];issuer=a.v['service']._issuer_identity
  else:
   i=e._installed;s=i.startup;self.original=(i,s,s.store,s.runtime['resources'],s.runtime['ticket'])
   descriptor=s.descriptor;issuer=s.client._issuer_identity
  self.descriptor=descriptor;self.descriptor_hash=_hash(descriptor)
  self.context=dict(scope=self.scope,installation=self.descriptor_hash,issuer=issuer,role=descriptor['node'])
  self.raw=_json(self.context);self.session=None;self.check()
 def check(self):
  e=self.e;_need(not e.dead and e.identity is self.identity and e.identity.hold==self.method)
  e.current();_need(_json(self.context)==self.raw and _hash(self.descriptor)==self.descriptor_hash)
  _need(_json(e.authority.scope if self.server else e.scope())==_json(self.scope))
  if self.server:
   a,control,v,service=self.original
   _need(e.authority is a and a.authorize is control and a.v is v and v['service'] is service)
   a.current();control._current()
  else:
   i,s,store,r,ticket=self.original
   _need(e._installed is i and i.startup is s and s.store is store and not i._closed)
   _need(s.runtime['resources'] is r and s.runtime['ticket'] is ticket)
   _need(e._service_pin==(e.service,e.service.channel,e.service._identity,e.service._call,e.service._operation))
   _need(e.service._action_gate is e and e.service._action_gate_pin is e)
   s.client.current();e.service.current();store.require_issued(s.journal,s.stage)
   _need(r.tickets.get(id(ticket)) is ticket and ticket.reservation_id not in r.poisoned)
   _need(r._row(ticket.reservation_id)[5]=='HELD')
 @contextmanager
 def permission(self):
  self.check();e=self.e
  if self.server:
   a,control,v,service=self.original;c=a.a.consent
   with c._user(a.credential,a.owner),c.fence.lock:
    b,_=a.current()
    with a.a._policy_scope(b,'action-session'),_non_suppressing(a.a._issuer_resources.hold(v['descriptor']['resource_id'],json.loads(json.dumps(v['descriptor'])))) as held:
     nothing(held);self.check();yield None;self.check()
    self.check()
  else:
   i,s,store,r,ticket=self.original;d=self.scope
   with s.client.admission('action-session'),_non_suppressing(r.hold_control(ticket.reservation_id,d['reservation'],d['binding'],d['charge'])) as held:
    nothing(held);self.check();yield None;self.check()
  self.check()

class _Identity:
 def __init__(self,t):self.t=t
 @contextmanager
 def hold(self,peer):
  t=self.t;t.check();_need(peer==t.e._identity)
  with _non_suppressing(t.method(t.scope,peer)) as held:
   nothing(held);t.check();yield peer.node_id;t.check()
  t.check()

class _Permission:
 def __init__(self,t):self.t=t
 @contextmanager
 def hold(self,principal,binding,action):
  t=self.t;_need(principal==t.e._identity.node_id and action=='data-session')
  _need(t.session is not None and binding==t.session.binding)
  with t.permission():yield None

def attach_action_session(endpoint,policy,*,enabled=False):
 from private_experiment_actions import ActionGate,ActionServer
 _need(enabled is True and type(endpoint) in (ActionGate,ActionServer) and type(policy)is DataSessionPolicy)
 _need(policy.lifetime<=120)
 e=endpoint;_need(e._sequence==0 and not getattr(e,'_action_session_attempted',False) and e.channel._start_attempted)
 _need(not getattr(e,'used',set()) and getattr(e,'permit',None) is None)
 t=_Transport(e,type(e)is ActionServer);e._action_session_attempted=True
 s=DataSession._attach(e.channel,t.context,policy,identity=_Identity(t),authorize=_Permission(t),check=t.check,purpose='action-session',dormant=True)
 t.session=s;e._action_session=s
 def failed(error):
  e.dead=True
  if t.server:
   e.authority.dead=True;e.authority.a._unknown(e.authority.pid)
  s._fail(error)
 async def exchange():
  try:
   with s.fence():pass
   def body(kind,nonce):return dict(version=1,purpose='action-session',kind=kind,context=s.digest.hex(),nonce=nonce)
   async def send(kind,nonce):
    await e.channel.send(e.envelope(t.scope['binding'],0,_json(body(kind,nonce))),max_frame_bytes=65536,write_fence=s.fence)
   async def receive(kind,nonce=None):
    delivery=await e.channel.receive()
    try:
     with s.fence():
      e.validate(delivery.envelope,t.scope['binding'],0);raw=_parse(delivery.envelope.payload)
      _need(type(raw)is dict and set(raw)=={'version','purpose','kind','context','nonce'} and type(raw['version'])is int)
      n=raw.get('nonce');_need(type(n)is str and len(n)==64 and all(x in '0123456789abcdef' for x in n))
      _need(raw==body(kind,n) and (nonce is None or nonce==n))
     await delivery.release()
     with s.fence():pass
     return n
    finally:delivery.envelope=None
   async with asyncio.timeout_at(s.opening_deadline):
    if t.server:
     n=secrets.token_hex(32);await send('OPEN',n);await receive('OPENED',n)
    else:
     n=await receive('OPEN');await send('OPENED',n)
    with s.fence():_need(e._sequence==0);e._sequence=1
    s._start_pulses();await s.ready()
  except BaseException as error:
   failed(error);raise
 task=asyncio.create_task(exchange());s.tasks+=(task,);e._action_session_open=task
 def ended(done):
  error=asyncio.CancelledError() if done.cancelled() else done.exception()
  if error is not None and s.phase!='CLOSED':failed(error)
 task.add_done_callback(ended)
 return task
