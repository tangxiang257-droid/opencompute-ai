"""Opt-in original installation connection liveness; never execution authority.

Terminal transport close is the CLOSE protocol. No close ACK permits quota
release, retry, reinstall or inference that device work stopped.
"""
import asyncio,contextvars,secrets
from contextlib import contextmanager
from dataclasses import dataclass
from .control_exchange import _need
from .online_issuance_transport import _hash,_json,_parse

_internal=contextvars.ContextVar('installation_session_transport',default=None)
@dataclass(frozen=True)
class SessionPolicy:
 interval:float=2.
 silence:float=10.
 def __post_init__(self):
  _need(type(self.interval) in (int,float) and type(self.silence) in (int,float))
  _need(.01<=self.interval<=3 and 3*self.interval<=self.silence<=10)
 def wire(self):return dict(interval=float(self.interval),silence=float(self.silence))

class _Session:
 def __init__(self,e,p):
  self.endpoint=e;self.policy=p;self.channel=e.channel;self.loop=asyncio.get_running_loop()
  self.phase='OPENING';self.last=self.loop.time();self.deadline=self.last+p.silence;self.task=None;self.error=None;self.sequence=2;self.cleanup_errors=[]
  self.descriptor=_hash(e._descriptor);self.context=None
  self.pins=(e.channel,e._identity,e.identity,e.identity.hold,e.authorize,e.authorize.hold,e.resources,e.resources.hold,e._descriptor)
 @property
 def active(self):return self.phase=='ACTIVE'
 def _fail(self,error):
  if self.phase!='CLOSED':self.error=error;self.phase='CLOSED'
  try:self.channel._abort()
  except BaseException as cleanup:self.cleanup_errors.append(cleanup)
  finally:
   if not self.channel._task.done():self.channel._task.cancel()
   task=self.task
   if task is not None and task is not asyncio.current_task() and not task.done():task.cancel()
 def check(self):
  e=self.endpoint
  internal=_internal.get()==(self,asyncio.current_task())
  try:
   _need(asyncio.get_running_loop() is self.loop and e._session is self and self.phase!='CLOSED')
   _need(self.active or internal)
   now=self.loop.time();_need(self.last<=now<self.deadline);self.last=now
   _need(self.pins==(e.channel,e._identity,e.identity,e.identity.hold,e.authorize,e.authorize.hold,e.resources,e.resources.hold,e._descriptor))
   _need(self.descriptor==_hash(e._descriptor))
   if self.active:_need(self.task is not None and not self.task.done())
  except BaseException as error:self._fail(error);raise
 @contextmanager
 def fence(self):
  token=_internal.set((self,asyncio.current_task()))
  try:
   self.check()
   with self.endpoint.admission('installation-session'):
    self.check();yield None;self.check()
   self.check()
  finally:_internal.reset(token)
 def raw(self,kind,nonce):return dict(version=1,purpose='installation-session',kind=kind,descriptor=self.descriptor,policy=self.policy.wire(),nonce=nonce)
 async def send(self,body):
  with self.fence():pass
  e=self.endpoint;raw=_json(body);_need(len(raw)<=1024)
  await self.channel.send(e.envelope(e._scope,self.sequence,raw),max_frame_bytes=65536,write_fence=self.fence)
  with self.fence():pass
 async def receive(self,kind,nonce=None):
  delivery=await self.channel.receive(_session_owner=self if self.active else None)
  try:
   with self.fence():
    e=self.endpoint;e.validate(delivery.envelope,e._scope,self.sequence)
    raw=delivery.envelope.payload;_need(len(raw)<=1024);body=_parse(raw)
    _need(type(body)is dict and set(body)==set(self.raw(kind,'')))
    n=body['nonce'];_need(type(n)is str and len(n)==64 and all(c in '0123456789abcdef' for c in n))
    _need(_json(body)==_json(self.raw(kind,n)) and (nonce is None or nonce==n))
   await delivery.release()
   with self.fence():pass
   return body
  finally:delivery.envelope=None
 def refresh(self):
  with self.fence():pass
  self.deadline=self.last+self.policy.silence
 async def run(self,client):
  try:
   if client:
    while True:
     async with asyncio.timeout_at(self.deadline):
      n=secrets.token_hex(32);await self.send(self.raw('PING',n));await self.receive('PONG',n);self.refresh();self.sequence+=1
     try:await asyncio.wait_for(asyncio.Event().wait(),self.policy.interval)
     except TimeoutError:pass
   else:
    while True:
     async with asyncio.timeout_at(self.deadline):
      body=await self.receive('PING');await self.send(self.raw('PONG',body['nonce']));self.refresh();self.sequence+=1
  except BaseException as error:self._fail(error)
 def ended(self,task):
  if self.phase!='CLOSED':self._fail(RuntimeError('installation session monitor exited'))
 async def stop(self):
  self._fail(RuntimeError('installation session closed'))
  task=self.task
  if task is not None and task is not asyncio.current_task():
   cancelled=None
   while not task.done():
    try:await asyncio.shield(task)
    except asyncio.CancelledError as error:cancelled=error
   try:task.result()
   except asyncio.CancelledError:pass
   if cancelled is not None:
    if self.cleanup_errors:raise BaseExceptionGroup("session cancellation and cleanup unknown",[cancelled,*self.cleanup_errors])
    raise cancelled
  if self.cleanup_errors:raise BaseExceptionGroup("installation session cleanup unknown",self.cleanup_errors)

async def open_session(endpoint,policy):
 from .online_issuance_transport import IssuanceClient,IssuanceService
 _need(type(endpoint) in (IssuanceClient,IssuanceService) and type(policy)is SessionPolicy)
 _need(endpoint._installed and endpoint._session is None and not getattr(endpoint.channel,'_session_taken',False))
 endpoint.current();s=_Session(endpoint,policy);endpoint._session=s;endpoint.channel._session_taken=True
 client=type(endpoint)is IssuanceClient
 try:
  async with asyncio.timeout_at(s.deadline):
   if client:
    n=secrets.token_hex(32);await s.send(s.raw('OPEN',n));await s.receive('OPENED',n)
   else:
    body=await s.receive('OPEN');await s.send(s.raw('OPENED',body['nonce']))
   s.refresh();s.sequence=3
   # No await between live owner publication, reader-mode transition and return.
   s.phase='ACTIVE';s.task=asyncio.create_task(s.run(client));s.task.add_done_callback(s.ended)
   s.channel._session_idle_owner=s;s.channel._idle_changed.set()
   s.check();return s
 except BaseException as error:s._fail(error);await s.stop();raise
