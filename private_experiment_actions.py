"""Additional experiment action admission; never replaces device/resource authority.

One central admission DB orders revocation and each stage's START/COMMIT admission.
Replies are history on their original connection, not reusable credentials.
No listener, provider discovery, model invocation, refund, or retry is supplied.
"""
import asyncio,json,math
from contextlib import contextmanager
from dataclasses import asdict
from private_experiment_admission import ExperimentAdmission,SHORT_ACTION_PROFILE,need,digest
from private_experiment_installation import _current
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing
from cluster_p1.control_exchange import _json,_parse,_Endpoint

def sample(clock):
 need(callable(getattr(clock,'sample',None)))
 value=clock.sample();need(type(value)is tuple and len(value)==3)
 need(all(type(x)in(int,float) and math.isfinite(x) for x in value))
 wall,mono,error=value;need(0<=error<=0.25)
 return wall,mono,error

class ActionAuthority:
 """Pinned original installation plus independently verified control/resource scope.

 authorize.hold(full_context, action) must verify the control-to-install mapping,
 original control peer, current task policy AND existing persistent resource lease.
 It has no default implementation. An installation receipt alone never suffices.
 """
 def __init__(self,a,credential,pid,sha,*,stage=None,scope=None,authorize=None,clock=None):
  need(type(a)is ExperimentAdmission and a.schema_version==3 and type(stage)is int and stage in(0,1))
  need(callable(getattr(authorize,'hold',None)));sample(clock)
  self.a=a;self.credential=credential;self.pid=pid;self.sha=sha;self.stage=stage
  self.authorize=authorize;self.clock=clock;self._pin=(authorize,authorize.hold,clock,clock.sample)
  self.owner=a.consent.service._download_owner(credential)
  self.scope=_parse(_json(scope));self._raw=_json(self.scope);self.dead=False
  self.v=a._installations.get((pid,stage));need(self.v is not None and self.v['attempted'])
  self._last=sample(clock);self.current()
 def current(self):
  need(not self.dead and self._pin==(self.authorize,self.authorize.hold,self.clock,self.clock.sample))
  now=sample(self.clock);old=self._last
  need(now[1]>=old[1] and now[0]>=old[0] and abs((now[0]-old[0])-(now[1]-old[1]))<=0.5)
  self._last=now
  a=self.a;b=_current(a,self.v)
  need(self.v is a._installations.get((self.pid,self.stage)) and self.v['credential']==self.credential and self.v['sha']==self.sha)
  need(b.get('runtime_profile')==dict(name=SHORT_ACTION_PROFILE,public_non_sensitive_only=True,online_only=True,billing=False,credits=False,authority_ms=5000,role_margin_ms=1000,rpc_ms=1000))
  need(b['policy']['privacy']=='L1' and b['policy']['offline'] is False)
  need(now[0]+now[2]<b['policy']['expires_at'])
  self.v['service'].current() # Original installation connection must still be live.
  s=self.scope;need(_json(s)==self._raw and set(s)=={'command','reservation','charge','binding'})
  need(type(s['command'])is str and 0<len(s['command'])<=128 and type(s['reservation'])is str and 0<len(s['reservation'])<=128 and type(s['charge'])is int and s['charge']>0)
  binding=s['binding'];d=b['declaration'];place=b['placements'][self.stage]
  expected=dict(owner=b['owner'],task=b['execution_task'],session=d['session']['session_id'],epoch=d['session']['epoch'],stage=self.stage,input_snapshot=b['source']['input_snapshot'],token_digest=b['source']['token_digest'],preprocessing=b['source']['preprocessing'],model=d['session']['model_hash'],layout=d['session']['layout_hash'],plan=d['plan_digest'],device=place['device'],instance=place['instance'],declaration_digest=digest(d))
  need(set(binding)==set(expected)|{'input_digest','peer'} and all(binding[k]==v for k,v in expected.items()))
  for k in('input_digest','peer'):need(type(binding[k])is str and len(binding[k])==64 and all(c in '0123456789abcdef' for c in binding[k]))
  if self.stage==0:need(binding['input_digest']==b['source']['token_digest'])
  # B input is deliberately not inferred from A's uncommitted declaration.
  # The mandatory parent checks actual committed input + exact control mapping.
  return b,now
 def context(self,b,action,output_digest):
  return dict(action=action,output_digest=output_digest,experiment_id=self.pid,context_digest=self.sha,body=b,scope=self.scope,installation=self.v['descriptor'],installation_peer=asdict(self.v['service']._identity))
 async def admit(self,scope,action,output_digest=None):
  need(action in('start','commit') and _json(scope)==self._raw)
  need(output_digest is None if action=='start' else type(output_digest)is str and len(output_digest)==64 and all(c in '0123456789abcdef' for c in output_digest))
  a=self.a;c=a.consent;attempted=False
  try:
   async with c.authority._lock:
    with c._user(self.credential,self.owner),c.fence.lock:
     b,now=self.current()
     with (_non_suppressing(self.authorize.hold(self.context(b,action,output_digest),action)) as allowed,
           _non_suppressing(a._issuer_resources.hold(self.v['descriptor']['resource_id'],json.loads(json.dumps(self.v['descriptor'])))) as resource):
      nothing(allowed);nothing(resource);b,now=self.current()
      # SAME actual DB/transaction domain as ExperimentAdmission.revoke.
      with a._tx() as db:
       b,now=self.current()
       if action=='commit':
        start=db.execute("SELECT scope FROM short_actions WHERE experiment=? AND stage=? AND action='start'",(self.pid,self.stage)).fetchone()
        need(start is not None and _json(json.loads(start[0])['scope'])==self._raw)
       duration=min(5,b['policy']['expires_at']-now[0]-now[2]);need(duration>1)
       self._action_deadline=(now[0]+duration,now[1]+duration)
       attempted=True;db.execute('INSERT INTO short_actions VALUES(?,?,?,?)',(self.pid,self.stage,action,_json(dict(scope=self.scope,output_digest=output_digest,admitted_wall=repr(now[0]),max_expires_wall=repr(self._action_deadline[0]))).decode()))
      self.current()
     self.current()
    b,now=self.current()
   return dict(action=action,output_digest=output_digest,scope_digest=digest(self.scope),authority_boot=a.boot,ttl_ms=self.action_remaining(),execution_allowed=False)
  except BaseException:
   self.dead=True
   if attempted:a._unknown(self.pid)
   raise

 def action_remaining(self):
  _,now=self.current();wall,mono=self._action_deadline
  remaining=int(min(wall-now[0],mono-now[1],5)*1000);need(remaining>1000)
  return remaining

class ActionServer(_Endpoint):
 def __init__(self,channel,authority,*,identity=None):
  super().__init__(channel);need(type(authority)is ActionAuthority and callable(getattr(identity,'hold',None)))
  self.authority=authority;self.identity=identity;self._pin=(authority,identity,identity.hold);self.dead=False
  # New purpose channel, same verified role certificate as original installation.
  old=authority.v['service']._identity
  need(channel is not authority.v['service'].channel)
  need(self._identity.node_id==old.node_id and self._identity.certificate_sha256==old.certificate_sha256)
 def check(self):
  need(not self.dead and self._pin==(self.authority,self.identity,self.identity.hold));self.current();self.authority.current()
 @contextmanager
 def fence(self):
  self.check()
  with self.channel.identity_fence(self._identity),_non_suppressing(self.identity.hold(self.authority.scope,self._identity)) as v:
   nothing(v);self.check();yield;self.check()
  self.check()
 @contextmanager
 def reply_fence(self):
  with self.fence():
   self.authority.action_remaining();yield;self.authority.action_remaining()
 async def serve_once(self):
  delivery=None
  try:
   self.check();sequence=self._sequence;self._sequence+=1
   delivery=await self.channel.receive();binding=self.authority.scope['binding']
   self.validate(delivery.envelope,binding,sequence);raw=delivery.envelope.payload;request=_parse(raw)
   need(set(request)=={'version','action','scope','output_digest'} and type(request['version'])is int and request['version']==1)
   with self.fence():need(_json(request['scope'])==self.authority._raw)
   receipt=await self.authority.admit(request['scope'],request['action'],request['output_digest'])
   await delivery.release();delivery.envelope=None;delivery=None
   body=_json(dict(reply_to=digest(request),receipt=receipt))
   await self.channel.send(self.envelope(binding,sequence,body),max_frame_bytes=65536,write_fence=self.reply_fence)
  except BaseException:
   self.dead=True;self.authority.dead=True
   raise
  finally:
   if delivery is not None:delivery.envelope=None

class ActionGate(_Endpoint):
 """Role-local, once-only online check; no transferable permission returned."""
 def __init__(self,channel,*,authority_node=None,authority_certificate=None,identity=None,clock=None):
  super().__init__(channel);need(callable(getattr(identity,'hold',None)));sample(clock)
  need(self._identity.node_id==authority_node and self._identity.certificate_sha256==authority_certificate)
  self.identity=identity;self.clock=clock;self._pin=(identity,identity.hold,clock,clock.sample)
  self.dead=False;self.used=set();self.permit=None;self.service=None;self._service_pin=None;self._active=False
  self._last=sample(clock)
 def _now(self):
  now=sample(self.clock);old=self._last
  need(now[1]>=old[1] and now[0]>=old[0] and abs((now[0]-old[0])-(now[1]-old[1]))<=0.5)
  self._last=now;return now
 def attach(self,service):
  from cluster_p1.control_exchange import ControlService
  need(type(service)is ControlService and self.service is None and service.channel is not self.channel and service._call is not None)
  need(getattr(service,'_action_gate',None)is None)
  self.service=service;self._service_pin=(service,service.channel,service._identity,service._call,service._operation)
  service._action_gate=self;service._action_gate_pin=self
 def check(self):
  need(not self.dead and self._pin==(self.identity,self.identity.hold,self.clock,self.clock.sample))
  self.current();s=self.service
  need(s is not None and self._service_pin==(s,s.channel,s._identity,s._call,s._operation) and s._action_gate is self and s._action_gate_pin is self)
  s.current()
 def scope(self):
  command,reservation,binding,charge=self.service._call
  return dict(command=command,reservation=reservation,binding=binding,charge=charge)
 async def prepare(self,action,output_digest=None):
  try:
   self.check();need(action in('start','commit') and action not in self.used);self.used.add(action)
   self.permit=None;sent=self._now();scope=self.scope();raw=_json(dict(version=1,action=action,scope=scope,output_digest=output_digest))
   sequence=self._sequence;self._sequence+=1;delivery=None
   # Includes send queue, writer drain, response and credit waits; no local lock.
   async with asyncio.timeout(1):
    await self.channel.send(self.envelope(scope['binding'],sequence,raw),max_frame_bytes=65536)
    delivery=await self.channel.receive()
    try:
     self.check();self.validate(delivery.envelope,scope['binding'],sequence);response=_parse(delivery.envelope.payload)
     need(set(response)=={'reply_to','receipt'} and response['reply_to']==digest(_parse(raw)))
     r=response['receipt'];need(set(r)=={'action','output_digest','scope_digest','authority_boot','ttl_ms','execution_allowed'} and r['action']==action and r['output_digest']==output_digest and r['scope_digest']==digest(scope) and r['execution_allowed'] is False)
     need(type(r['authority_boot'])is str and len(r['authority_boot'])==32 and type(r['ttl_ms'])is int and 1000<r['ttl_ms']<=5000)
     await delivery.release()
    finally:delivery.envelope=None
   self.permit=(action,sent,sent[1]+r['ttl_ms']/1000-1)
   self._valid_permit(action,self.permit)
  except BaseException:self.dead=True;self.permit=None;raise
 def _valid_permit(self,action,permit):
  self.check();need(permit is not None and permit[0]==action)
  _,sent,deadline=permit;now=self._now()
  need(now[1]>=sent[1] and now[0]>=sent[0] and now[1]<deadline and abs((now[0]-sent[0])-(now[1]-sent[1]))<=0.5)
 @contextmanager
 def fence(self,action):
  try:
   need(not self._active);permit=self.permit;self._valid_permit(action,permit)
   self.permit=None;self._active=True # Consume before any provider callback/body.
   with self.channel.identity_fence(self._identity),_non_suppressing(self.identity.hold(self.scope(),self._identity)) as value:
    nothing(value);self._valid_permit(action,permit);yield;self._valid_permit(action,permit)
   self._valid_permit(action,permit)
  except BaseException:self.dead=True;self.permit=None;raise
  finally:self._active=False
