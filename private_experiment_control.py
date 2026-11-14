"""Original coordinator control -> actual short-action server, no provider fallback.

The control-bound record is an authenticated role statement, not a portable
grant or independent physical capacity proof. Only the central admission DB is
accessed here; no role database, installed-role object or local ticket is read.
"""
import asyncio,json
from contextlib import contextmanager
from dataclasses import asdict
from private_experiment_admission import ExperimentAdmission,need,digest
from private_experiment_actions import ActionAuthority,ActionServer,sample
from private_experiment_installation import _current
from cluster_p1.role_control import CoordinatorControl
from cluster_p1.c_receiving_input import CommittedBInput
from cluster_p1.control_exchange import _json
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing

def _b_descriptor(b):
 d=b['declaration'];s=d['session']
 return dict(owner=b['owner'],task=b['execution_task'],session=s['session_id'],epoch=s['epoch'],
  model=s['model_hash'],layout=s['layout_hash'],plan_digest=d['plan_digest'],**{k:b['source'][k] for k in ('input_snapshot','token_digest','preprocessing')},
  b_node=b['placements'][1]['node'],b_placement=b['placements'][1],declaration_digest=digest(d))

class ControlAuthority:
 def __init__(self):raise TypeError('use create_action_server')
 def _current(self,*,published=True):
  need(self.semantic_pin==(self.a,self.credential,self.owner,self.pid,self.sha,self.stage,self.key,self.scope_raw,self.install_peer,self.authority,self.authority.current,self.authority.context))
  if published:need(self.record==self.record_pin)
  a=self.a;need(not self.dead and self.fixed==(a,self.control,self.control.descriptor,self.v,self.client,self.client.channel,self.client._identity))
  a._ready();self.client.current();self.v['service'].current()
  b=_current(a,self.v);need(digest(b)==self.sha and self.v is a._installations.get(self.key))
  need(self.control._client is self.client and not self.control._closed)
  need(self.client._identity==self.peer and self.v['service']._identity==self.install_peer)
  need((self.peer.node_id,self.peer.certificate_sha256)==(self.install_peer.node_id,self.install_peer.certificate_sha256))
  need(self.control.descriptor.role_node==b['placements'][self.stage]['node'])
  d=self.control.descriptor
  need(_json(dict(command=d.command_id,reservation=d.reservation_id,binding=d.binding,charge=d.charge))==self.scope_raw)
  self.authority.current()
  if published:
   need(a._control_bindings.get(self.key) is self)
   row=a.db.execute("SELECT scope FROM short_actions WHERE experiment=? AND stage=? AND action='control-bound'",self.key).fetchone()
   need(row==(self.record,))
  return b
 @contextmanager
 def hold(self,context,action):
  a=self.a;c=a.consent
  try:
   need(action in('start','commit'))
   with c._user(self.credential,self.owner),c.fence.lock:
    b=self._current()
    need(digest(context)==digest(self.authority.context(b,action,context.get('output_digest'))))
    with a._policy_scope(b,action),_non_suppressing(a._issuer_resources.hold(self.v['descriptor']['resource_id'],json.loads(json.dumps(self.v['descriptor'])))) as held:
     nothing(held);self._current();yield None;self._current()
    self._current()
   self._current()
  except BaseException:self.dead=True;a._unknown(self.pid);raise

async def create_action_server(a,credential,pid,sha,stage,control,channel,*,identity=None,clock=None):
 """Once-only authenticated mapping, then the existing ActionAuthority/Server.

 Identity/clock and central persistent resource providers remain mandatory.
 Await prepare/B proof without any local identity/resource/SQLite lock held.
 Caller owns the supplied channel on failure; no socket or listener is created.
 """
 need(type(a)is ExperimentAdmission and type(control)is CoordinatorControl and type(stage)is int and stage in(0,1))
 need(callable(getattr(identity,'hold',None)));sample(clock)
 a._ready();key=(pid,stage);v=a._installations.get(key);need(v is not None and v['attempted'])
 if not hasattr(a,'_control_bindings'):a._control_bindings={};a._control_attempts=set()
 need(key not in a._control_attempts);a._control_attempts.add(key)
 self=object.__new__(ControlAuthority);self.a=a;self.credential=credential;self.pid=pid;self.sha=sha;self.stage=stage;self.key=key
 self.control=control;self.v=v;self.client=control._client;self.peer=self.client._identity;self.install_peer=v['service']._identity
 self.owner=a.consent.service._download_owner(credential);self.dead=False
 d=control.descriptor;scope=dict(command=d.command_id,reservation=d.reservation_id,binding=d.binding,charge=d.charge)
 self.scope_raw=_json(scope);self.fixed=(a,control,d,v,self.client,self.client.channel,self.peer)
 try:
  self.authority=ActionAuthority(a,credential,pid,sha,stage=stage,scope=scope,authorize=self,clock=clock)
  self.semantic_pin=(a,credential,self.owner,pid,sha,stage,key,self.scope_raw,self.install_peer,self.authority,self.authority.current,self.authority.context)
  c=a.consent
  with c._user(credential,self.owner),c.fence.lock:b=self._current(published=False)
  async with asyncio.timeout(1):
   receipt=await control.prepare()
   need(receipt['state']=='RESERVED' and receipt['retired'] is False)
   proof=await CommittedBInput(control).read_committed(_b_descriptor(b)) if stage==1 else None
  # Echoed receipt is from the pinned authenticated role. The reverse peer hash
  # is the role's fixed statement, not this endpoint's role peer/generation.
  record=dict(version=1,kind='original-authenticated-control',authority_boot=a.boot,context_digest=sha,
   scope=scope,role_peer=asdict(self.peer),installation_peer=asdict(self.install_peer),
   role_observed_controller_peer=d.binding['peer'],reservation_state=receipt['state'],b_input=proof)
  self.record=_json(record).decode();self.record_pin=self.record
  async with c.authority._lock:
   with c._user(credential,self.owner),c.fence.lock:
    b=self._current(published=False)
    with a._policy_scope(b,'bind-control'),_non_suppressing(a._issuer_resources.hold(v['descriptor']['resource_id'],json.loads(json.dumps(v['descriptor'])))) as held:
     nothing(held);self._current(published=False)
     with a._tx() as db:
      self._current(published=False)
      db.execute('INSERT INTO short_actions VALUES(?,?,?,?)',(pid,stage,'control-bound',self.record))
     self._current(published=False)
    self._current(published=False)
   self._current(published=False)
  # Publish only after every admission context exited; query cannot reconstruct.
  server=ActionServer(channel,self.authority,identity=identity)
  self._current(published=False);a._control_bindings[key]=self
  self._current();return server
 except BaseException:self.dead=True;a._unknown(pid);raise
