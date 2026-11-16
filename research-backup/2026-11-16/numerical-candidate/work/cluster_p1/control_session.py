"""Opt-in original control-stream liveness, not RPC or execution authority.

The installation/resource chain is checked directly, before action authorities
exist. Native bounded pulses never dispatch operations or recover UNKNOWN.
"""
import json
from contextlib import contextmanager
from .data_session import DataSession,DataSessionPolicy
from .control_exchange import _json,_need
from .online_issuance_transport import _hash
from .local_attempt import _non_suppressing
from .projection_driver import nothing

class _Transport:
 def __init__(self,owner,central):
  self.owner=owner;self.central=central
  if central:
   h=owner;self.control=h.control;self.endpoint=h.control._client
   v=h.a._installations[(h.c['experiment'],h.c['stage'])]
   self.original=(h.a,h.credential,h.resources,h.control_identity,h.control_identity.hold,v,v['service'])
   d=h.control.descriptor;installation=v['descriptor'];issuer=v['service']._issuer_identity;self.identity=h.control_identity
  else:
   l=owner;self.endpoint=l.installed.role._service
   self.original=(l.startup,l.installed,l.operation,l.startup.store,l.startup.runtime['resources'],l.startup.runtime['ticket'])
   d=l.installed.descriptor;installation=l.startup.descriptor;issuer=l.startup.client._issuer_identity;self.identity=self.endpoint.identity
  self.descriptor=d;self.descriptor_raw=self._descriptor_raw();self.scope=json.loads(self.descriptor_raw)
  self.installation=installation;self.installation_raw=_hash(installation)
  self.context=dict(scope=self.scope,installation=self.installation_raw,issuer=issuer);self.raw=_json(self.context)
  self.channel=self.endpoint.channel;self.peer=self.endpoint._identity;self.identity_method=self.identity.hold;self.session=None;self.check()
 def _descriptor_raw(self):
  d=self.descriptor;return _json(dict(command=d.command_id,reservation=d.reservation_id,binding=d.binding,charge=d.charge))
 def check(self):
  e=self.endpoint;d=self.descriptor
  _need(e.channel is self.channel and e._identity==self.peer and self.identity.hold==self.identity_method)
  _need(_json(self.context)==self.raw and _hash(self.installation)==self.installation_raw and self._descriptor_raw()==self.descriptor_raw)
  e.current();_need(self.channel._descriptor_bootstrap_ready)
  if self.central:
   from private_experiment_installation import _current
   h=self.owner;a,credential,r,identity,method,v,service=self.original;h.check()
   _need((h.a,h.credential,h.resources,h.control_identity,h.control_identity.hold,h.a._installations.get((h.c['experiment'],h.c['stage'])),v['service'])==self.original)
   _need(h.control is self.control and self.control._client is e and self.control.descriptor is d and not self.control._closed)
   _current(a,v);service.current();_need(service._descriptor==self.installation)
  else:
   l=self.owner;s,i,operation,store,r,ticket=self.original;l._live()
   _need((l.startup,l.installed,l.operation,l.startup.store,l.startup.runtime['resources'],l.startup.runtime['ticket'])==self.original)
   _need(i.descriptor is d and i.role._service is e and e._call==(d.command_id,d.reservation_id,d.binding,d.charge))
   i._check();s.client.current();store.require_issued(s.journal,s.stage)
   _need(r.tickets.get(id(ticket)) is ticket and ticket.reservation_id not in r.poisoned and r._row(ticket.reservation_id)[5]=='HELD')
 @contextmanager
 def permission(self):
  self.check()
  if self.central:
   from private_experiment_installation import _parent
   a,credential,r,identity,method,v,service=self.original;c=a.consent
   with c._user(credential,v['owner']),c.fence.lock:
    with _parent(a,v,'control-session'),_non_suppressing(r.hold(v['descriptor']['resource_id'],json.loads(json.dumps(v['descriptor'])))) as held:
     nothing(held);self.check();yield None;self.check()
    self.check()
  else:
   s,i,operation,store,r,ticket=self.original;d=self.descriptor
   with self.endpoint.fence(d.binding,'control-session'),_non_suppressing(r.hold_control(ticket.reservation_id,d.reservation_id,d.binding,d.charge)) as held:
    nothing(held);self.check();yield None;self.check()
   self.check()
  self.check()

class _Identity:
 def __init__(self,t):self.t=t
 @contextmanager
 def hold(self,peer):
  t=self.t;t.check();_need(peer==t.peer);d=t.descriptor
  scope={k:d.binding[k] for k in ('task','session','epoch','stage','model')} if t.central else d.binding
  with _non_suppressing(t.identity_method(scope,peer)) as held:
   nothing(held);t.check();yield peer.node_id;t.check()
  t.check()

class _Permission:
 def __init__(self,t):self.t=t
 @contextmanager
 def hold(self,principal,binding,action):
  t=self.t;_need(principal==t.peer.node_id and action=='data-session' and t.session is not None and binding==t.session.binding)
  with t.permission():yield None

async def attach_control_session(owner,policy,*,enabled=False):
 from .role_launcher import StageLauncher
 from private_control_action_host import ControlActionHost
 _need(enabled is True and type(owner) in (StageLauncher,ControlActionHost) and type(policy)is DataSessionPolicy and policy.lifetime<=120)
 t=_Transport(owner,type(owner)is ControlActionHost);e=t.endpoint
 _need(t.channel._idle_aware is True and e._sequence==2 and not getattr(e,'_control_session_attempted',False) and not getattr(t.channel,'_control_requests_started',False))
 e._control_session_attempted=True
 s=DataSession._attach(t.channel,t.context,policy,identity=_Identity(t),authorize=_Permission(t),check=t.check,purpose='control-session',dormant=True)
 t.session=s;e._control_session=s
 try:
  s._start_pulses();await s.ready();t.check();return s
 except BaseException as error:s._fail(error);raise
