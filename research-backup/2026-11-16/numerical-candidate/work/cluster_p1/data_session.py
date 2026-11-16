"""Opt-in bounded transport liveness. Never an execution or receipt grant."""
import asyncio,hashlib,inspect,json,math,secrets,struct
from dataclasses import dataclass
from contextlib import contextmanager
from .channel import Channel,ChannelError
from .split_role_transport import _generation,_ssl_snapshot
from .local_attempt import _non_suppressing
FRAME_SIZE=74
_WIRE=struct.Struct('!BBQ32s32s')
def _need(ok):
 if not ok:raise ChannelError('data session unavailable')
def _raw(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def _none(v):
 if inspect.iscoroutine(v):v.close()
 _need(v is None)
@dataclass(frozen=True)
class DataSessionPolicy:
 interval:float=2
 silence:float=10
 lifetime:float=120
 def __post_init__(self):
  _need(all(type(x) in (int,float) and math.isfinite(x) for x in (self.interval,self.silence,self.lifetime)))
  _need(.01<=self.interval<=3 and 3*self.interval<=self.silence<=10 and self.silence<=self.lifetime<=300)
 def wire(self):return dict(interval=float(self.interval),silence=float(self.silence),lifetime=float(self.lifetime))
class DataSession:
 def __init__(self):raise ChannelError("use explicitly enabled attach")
 @classmethod
 def attach(cls,channel,binding,policy,*,identity=None,authorize=None,check=None,enabled=False):
  _need(enabled is True and type(channel)is Channel and type(policy)is DataSessionPolicy)
  _need(channel._data_session_owner is None and channel._session_idle_owner is None and not getattr(channel,'_online_issuance_claimed',False))
  _need(not channel._closed and not channel.w.is_closing() and not channel._task.done())
  _need(callable(getattr(identity,'hold',None)) and callable(getattr(authorize,'hold',None)) and callable(check))
  _need(type(binding)is dict and set(binding)=={'transport_scope','a','b','installation'})
  _need(type(binding['transport_scope'])is dict and set(binding['transport_scope'])=={'owner','request','session','epoch','model','layout','plan_digest','input_snapshot','token_digest','preprocessing','hidden','vocab'})
  ins=binding['installation'];_need(type(ins)is dict and set(ins)=={'registration_id','issuer_incarnation','source_namespace','source_id','declaration_digest','source_binding','issuer'})
  for n in ('a','b'):
   v=binding[n];_need(type(v)is dict and set(v)=={'node','certificate'} and type(v['node'])is str and bool(v['node']))
   _need(type(v['certificate'])is str and len(v['certificate'])==64 and all(x in '0123456789abcdef' for x in v['certificate']))
  _need(binding['a']['node']!=binding['b']['node'] and binding['a']['certificate']!=binding['b']['certificate'])
  peer=channel.authenticated_identity;_need(sum((v['node'],v['certificate'])==(peer.node_id,peer.certificate_sha256) for v in (binding['a'],binding['b']))==1)
  return cls._attach(channel,binding,policy,identity=identity,authorize=authorize,check=check)
 @classmethod
 def _attach(cls,channel,binding,policy,*,identity,authorize,check,purpose='data-session',dormant=False):
  # Private common engine. Public data attach keeps its exact validation above.
  _need(type(channel)is Channel and type(policy)is DataSessionPolicy and purpose in ('data-session','action-session','control-session'))
  _need(channel._data_session_owner is None and channel._session_idle_owner is None and not channel._closed)
  _need(purpose!='control-session' or channel._idle_aware is True)
  old_deadline=getattr(channel,'_reader_idle_deadline',None)
  _need(old_deadline is None or asyncio.get_running_loop().time()<old_deadline)
  _need(callable(identity.hold) and callable(authorize.hold) and callable(check))
  peer=channel.authenticated_identity
  self=object.__new__(cls);self.channel=channel;self.policy=policy;self.identity=identity;self.authorize=authorize;self.callback=check
  self.binding=dict(version=1,purpose=purpose,scope=json.loads(_raw(binding)),generation=_generation(channel.w),policy=policy.wire())
  self.raw=_raw(self.binding);_need(len(self.raw)<=16384);self.digest=hashlib.sha256(self.raw).digest();self.peer=peer;self.ssl=_ssl_snapshot(channel.w)
  self.pins=(channel.r,channel.w,identity.hold,authorize.hold,check,policy,channel._idle_aware);self.loop=asyncio.get_running_loop();now=self.loop.time();self.last=now
  self.opening_deadline=now+policy.silence;self.expires=now+policy.lifetime;self.deadline=self.opening_deadline
  self.phase='OPENING';self.error=None;self.sequence=self.peer_sequence=0;self.pending=self.ping=self.pong=None;self.answered=self.responded=False
  self.ping_event=asyncio.Event();self.pong_event=asyncio.Event();self.ready_event=asyncio.Event();self.closed_event=asyncio.Event();self.tasks=();self.cleanup=None
  self.window=now;self.count=0;channel._data_session_owner=self;channel._idle_changed.set()
  self._pulses_started=False
  if not dormant:self._start_pulses()
  return self
 def _start_pulses(self):
  self.check();_need(not self._pulses_started);self._pulses_started=True
  self.tasks+= (asyncio.create_task(self._guard(self._request)),asyncio.create_task(self._guard(self._respond)))
 @property
 def active(self):return self.phase=='ACTIVE'
 def check(self):
  _need(asyncio.get_running_loop() is self.loop and self.phase in ('OPENING','ACTIVE'))
  now=self.loop.time();_need(self.last<=now<min(self.deadline,self.expires));self.last=now
  c=self.channel;_need(c._data_session_owner is self and c._session_idle_owner is None and not c._closed and not c.w.is_closing())
  _need(self.pins==(c.r,c.w,self.identity.hold,self.authorize.hold,self.callback,self.policy,c._idle_aware))
  _need(c.authenticated_identity==self.peer and _ssl_snapshot(c.w)==self.ssl and _raw(self.binding)==self.raw)
 @contextmanager
 def fence(self):
  self.check()
  with self.channel.identity_fence(self.peer):
   with _non_suppressing(self.identity.hold(self.peer)) as principal:
    _need(principal==self.peer.node_id)
    with _non_suppressing(self.authorize.hold(principal,json.loads(self.raw),'data-session')) as allowed:
     _none(allowed);_none(self.callback());self.check();yield None;self.check()
    self.check()
   self.check()
  self.check()
 def _accept(self,raw):
  self.check();_need(len(raw)==FRAME_SIZE);version,op,seq,digest,nonce=_WIRE.unpack(raw)
  _need(version==1 and op in (1,2) and digest==self.digest and seq>0)
  if op==1:
   _need(self.ping is None and seq==self.peer_sequence+1)
   now=self.loop.time()
   if now-self.window>=self.policy.silence:self.window=now;self.count=0
   self.count+=1;_need(self.count<=int(self.policy.silence/self.policy.interval)+4)
   self.ping=(seq,nonce);self.ping_event.set()
  else:
   _need(self.pending==(seq,nonce) and self.pong is None);self.pong=(seq,nonce);self.pong_event.set()
 def _activate(self):
  if self.answered and self.responded and self.phase=='OPENING':
   self.phase='ACTIVE';self.deadline=min(self.expires,self.loop.time()+self.policy.silence);self.channel._idle_changed.set();self.ready_event.set()
 async def _control_checkpoint(self):
  # Raw binding is checked before selecting purpose; no cached authorization.
  self.check()
  if self.binding['purpose']=='control-session':
   await asyncio.sleep(0)
   self.check()  # Synchronous expiry rejection even if timers have not run.
 async def _write(self,op,pair):
  with self.fence():pass
  async with asyncio.timeout_at(min(self.deadline,self.expires)):
   await self._control_checkpoint()
   await self.channel._frame(b'P',_WIRE.pack(1,op,pair[0],self.digest,pair[1]),write_fence=self.fence)
   await self._control_checkpoint()
  with self.fence():pass
 async def _request(self):
  while True:
   with self.fence():
    self.sequence+=1;_need(self.sequence<2**64);self.pending=(self.sequence,secrets.token_bytes(32))
   async with asyncio.timeout_at(min(self.deadline,self.expires)):
    await self._write(1,self.pending);await self.pong_event.wait()
    with self.fence():
     _need(self.pong==self.pending);self.pending=self.pong=None;self.pong_event.clear();self.answered=True
   self.check()
   if self.active:self.deadline=min(self.expires,self.loop.time()+self.policy.silence)
   self._activate()
   try:await asyncio.wait_for(self.closed_event.wait(),min(self.policy.interval,self.expires-self.loop.time()))
   except TimeoutError:pass
   self.check()
 async def _respond(self):
  while True:
   try:
    async with asyncio.timeout_at(min(self.deadline,self.expires)):await self.ping_event.wait()
   except TimeoutError:
    # A matched response may have advanced the shared deadline while waiting.
    # Recheck the current deadline; never refresh it merely for waking.
    self.check();continue
   with self.fence():pair=self.ping;_need(pair is not None)
   await self._control_checkpoint()
   await self._write(2,pair)
   with self.fence():_need(self.ping==pair);self.peer_sequence=pair[0];self.ping=None;self.ping_event.clear();self.responded=True
   self._activate()
 async def _guard(self,body):
  try:await body()
  except BaseException as error:self._fail(error)
 def _fail(self,error=None,*,abort=True):
  if self.phase=='CLOSED':return
  self.phase='CLOSED';self.error=error;self.ready_event.set();self.closed_event.set();self.channel._idle_changed.set()
  for t in self.tasks:
   if t is not asyncio.current_task():t.cancel()
  try:
   if abort:self.channel._abort()
  except BaseException as e:
   self.error=BaseExceptionGroup('data session shutdown failed',[error,e] if error is not None else [e])
  finally:
   if self.channel._task is not asyncio.current_task():self.channel._task.cancel()
 def _channel_closed(self,error):self._fail(error,abort=False)
 async def ready(self):
  try:
   async with asyncio.timeout_at(self.opening_deadline):await self.ready_event.wait()
   if not self.active:raise self.error or ChannelError('data session closed')
   self.check();return self
  except BaseException as e:self._fail(e);raise
 async def close(self):
  self._fail()
  if self.cleanup is None:
   async def cleanup():
    errors=[]
    try:await self.channel.close()
    except BaseException as e:errors.append(e)
    try:await asyncio.wait_for(self.channel.w.wait_closed(),self.channel.timeout)
    except BaseException as e:errors.append(e)
    if errors:
     if len(errors)==1:raise errors[0]
     raise BaseExceptionGroup('data session cleanup failed',errors)
   self.cleanup=asyncio.create_task(cleanup())
  cancelled=None
  while not self.cleanup.done():
   try:await asyncio.shield(self.cleanup)
   except asyncio.CancelledError as e:cancelled=e
   except BaseException:break
  try:self.cleanup.result()
  except BaseException as e:
   if cancelled is not None:raise BaseExceptionGroup('cancelled data session cleanup',[cancelled,e])
   raise
  if cancelled is not None:raise cancelled
 async def stop(self):await self.close()
