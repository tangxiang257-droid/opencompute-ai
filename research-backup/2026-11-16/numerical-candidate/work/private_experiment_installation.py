"""Original confirmed experiment -> existing one-shot online installation.

No new state machine, runtime execution grant or provider factory. Current USER,
independent installation authority and authenticated endpoint/resource fences are
mandatory. The existing issuer stage slot remains the durable consumption record.
"""
import asyncio,json,threading
from contextlib import contextmanager
from dataclasses import asdict
from private_experiment_admission import need,digest
from cluster_p1.online_issuance_transport import IssuanceService
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing
from cluster_p1.store import canonical

def _pins(s):
 return (s.channel,s._identity,s.identity,s.identity.hold,s.authorize,s.authorize.hold,s.resources,s.resources.hold,s._descriptor,s._write_fence)

def _current(a,v):
 try:return _current_checked(a,v)
 except BaseException:a._unknown(v['pid']);raise

def _current_checked(a,v):
 a._ready();need(callable(a._installation_hold))
 b,r=a._valid_current(v['credential'],v['owner'],v['pid'],{'REGISTERED'})
 need(r[1]==v['sha'] and b==v['body'] and a._registered.get(v['pid']) is v['registration'])
 need(a._issuer is v['issuer']);a._issuer._registration(v['registration'])
 place=b['placements'][v['stage']];d=v['descriptor']
 need(all(d[k]==place[k] for k in ('node','device','instance','resource_id')) and d['declaration']==b['declaration'])
 need(v['registration'].authorization==canonical(dict(version=1,kind='isolated-experiment-registration',experiment_id=v['pid'],context_digest=v['sha'],authority_boot=a.boot,body=b)))
 service=v['service']
 if service is not None:
  need(_pins(service)==v['pins'] and service._descriptor==v['descriptor'] and service._authority is v['issuer'] and service._registration is v['registration'])
 return b

@contextmanager
def _parent(a,v,action,intent=None,peer=None):
 b=_current(a,v)
 request=dict(version=1,purpose='experiment-stage-installation',experiment_id=v['pid'],context_digest=v['sha'],body=b,descriptor=v['descriptor'],intent=intent,peer=asdict(peer) if peer is not None else None)
 with _non_suppressing(a._installation_hold(json.loads(canonical(request)),action)) as allowed:
  nothing(allowed);_current(a,v);yield;_current(a,v)
 _current(a,v)

async def create_service(a,credential,pid,sha,stage,channel,**providers):
 a._ready();need(type(stage)is int and stage in (0,1) and callable(a._installation_hold))
 c=a.consent;owner=c.service._download_owner(credential);key=(pid,stage);claimed=False
 try:
  async with c.authority._lock:
   with c._user(credential,owner):
    b,r=a._valid(credential,owner,pid,{'REGISTERED'});need(r[1]==sha and key not in a._installations)
    reg=a._registered.get(pid);need(reg is not None);issuer=a._issuer;issuer._registration(reg)
    need(providers.get('resources') is issuer.resources)
    place=b['placements'][stage]
    d=issuer.describe(reg,stage,**{k:place[k] for k in ('node','device','instance','resource_id')})
    v=dict(credential=credential,owner=owner,pid=pid,sha=sha,body=b,stage=stage,descriptor=d,registration=reg,issuer=issuer,service=None,attempted=False)
    # Permanent same-boot endpoint occupancy, no retry with another connection.
    a._installations[key]=v;claimed=True
    with _parent(a,v,'prepare-installation'):
     wire=_WireGuard(a,v,providers['authorize'])
     service=IssuanceService(channel,issuer,reg,d,**dict(providers,authorize=wire));v['service']=service
     service._write_fence=wire.write_fence;v['pins']=_pins(service)
    _current(a,v)
   _current(a,v)
  return service
 except BaseException:
  if claimed:a._unknown(pid)
  raise # Supplied channel remains caller-owned until successful return.

class _WireGuard:
 def __init__(self,a,v,parent):
  need(callable(getattr(parent,'hold',None)));self.a=a;self.v=v;self.parent=parent;self.pin=(parent,parent.hold);self.fixed=(a,v)
 def check(self):
  need(self.a is self.fixed[0] and self.v is self.fixed[1] and self.parent is self.pin[0] and self.parent.hold==self.pin[1]);_current(self.a,self.v)
 @contextmanager
 def hold(self,principal,peer,binding,action):
  a,v=self.fixed;c=a.consent
  try:
   with c._user(v['credential'],v['owner']),c.fence.lock:
    self.check()
    with _non_suppressing(self.parent.hold(principal,peer,binding,action)) as allowed:
     nothing(allowed);self.check();yield None;self.check()
    self.check()
   self.check()
  except BaseException:a._unknown(v['pid']);raise
 @contextmanager
 def write_fence(self):
  a,v=self.fixed
  try:
   self.check();s=v['service']
   with s.admission('installation-write'):self.check();yield;self.check()
   self.check()
  except BaseException:a._unknown(v['pid']);raise

@contextmanager
def claim_fence(a,request):
 v=a._claim_active;need(v is not None and v['thread']==threading.get_ident() and v['task'] is asyncio.current_task() and a.consent.authority._lock.locked())
 expected=dict(version=2,intent=v['intent'],authorization=json.loads(v['registration'].authorization))
 need(request==expected)
 with _parent(a,v,'claim-installation',v['intent'],v['peer']):yield None

async def claim_online(a,issuer,registration,intent,peer,admission):
 a._ready();need(callable(admission) and type(intent)is dict)
 values=[v for v in a._installations.values() if v['registration'] is registration and v['stage']==intent.get('stage')]
 need(len(values)==1);v=values[0];service=v['service'];attempted=False;c=a.consent
 try:
  async with c.authority._lock:
   with c._user(v['credential'],v['owner']):
    _current(a,v);need(issuer is v['issuer'] and service is not None and not v['attempted'] and peer is service._identity)
    need(service._authority is issuer and service._registration is registration and service._descriptor==v['descriptor'] and service.resources is a._issuer_resources)
    need(all(intent.get(k)==x for k,x in v['descriptor'].items()))
    # Bind the original service's actual connection fence. Do not trust a supplied
    # arbitrary context callback as a replacement identity/resource admission.
    with service.admission('claim'):
     _current(a,v);v['attempted']=True;attempted=True
     active=dict(v,intent=json.loads(canonical(intent)),peer=peer,task=asyncio.current_task(),thread=threading.get_ident())
     need(a._claim_active is None);a._claim_active=active
     try:response=issuer.claim_stage(registration,intent,peer)
     finally:a._claim_active=None
     _current(a,v)
    _current(a,v)
   _current(a,v)
  return response
 except BaseException:
  if attempted:a._unknown(v['pid']);issuer.retired=True
  raise
