"""Borrowed original-Channel identity for one fixed action scope, not authority.

Only the owning loop and original connection qualify. No global node registry,
task permission, resource allocation, credential issuance or reconnection. A
trusted caller supplies expected peer from independently pinned configuration;
JSON alone cannot manufacture the actual Channel or its generation.
"""
from contextlib import contextmanager
from dataclasses import asdict
from .channel import Channel,ChannelIdentity
from .control_exchange import _json,_parse,_need

class ScopedChannelIdentity:
 def __init__(self,channel,scope,*,expected_peer):
  _need(type(channel)is Channel and type(expected_peer)is dict)
  _need(type(scope)is dict and set(scope)=={'command','reservation','charge','binding'})
  _need(type(scope['binding'])is dict and bool(scope['binding']))
  _need(type(scope['charge'])is int and scope['charge']>0)
  for k in ('command','reservation'):_need(type(scope[k])is str and 0<len(scope[k])<=128)
  raw=_json(scope);_need(len(raw)<=32768);_parse(raw)
  peer=channel.authenticated_identity;_need(_json(expected_peer)==_json(asdict(peer)))
  self.channel=channel;self.peer=peer;self.scope_raw=raw;self.retired=False
  self._pin=(channel,channel.r,channel.w,peer,raw,channel.identity_fence)
  with channel.identity_fence(peer):self._check()
 def _check(self):
  c=self.channel
  _need(not self.retired and self._pin==(c,c.r,c.w,self.peer,self.scope_raw,c.identity_fence))
  _need(c.authenticated_identity==self.peer)
 @contextmanager
 def hold(self,scope,actual):
  try:
   self._check();_need(type(actual)is ChannelIdentity and actual==self.peer and _json(scope)==self.scope_raw)
   with self._pin[-1](self.peer):
    self._check();yield None;self._check()
    # Channel's fence checks entry only. Same-loop reentrant revocation inside
    # the body must also be detected before reporting successful context exit.
    with self._pin[-1](self.peer):self._check()
  except BaseException:
   self.retired=True;raise
