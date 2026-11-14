"""Central C registration policy bound to existing confirmed task + issuer.

No remote view/receipt is accepted as authority. Dedicated receiving and stop
maintenance parents are mandatory. New DB paths cannot reset this live consent's
local one-time C claim; restart does not restore active consent eligibility.
"""
from contextlib import contextmanager
from private_consent_runtime import IssuanceConsent
from private_task_consent import need,digest
from cluster_p1.c_receiving import descriptor
from cluster_p1.history_query import _context
from cluster_p1.control_exchange import _nothing

class CReceivingPolicy:
 @classmethod
 async def create(cls,adapter,*,resource_id,charge,parent=None,maintenance=None):
  need(cls is CReceivingPolicy and type(adapter)is IssuanceConsent and callable(getattr(parent,'hold',None)) and callable(getattr(maintenance,'hold',None)))
  c=adapter.consent
  async with c.authority._lock:
   with c._user(adapter.credential,c.service._download_owner(adapter.credential)):
    body,revision=adapter._locked();need(adapter.registration is not None)
    issuer,registration=adapter.registration;issuer._registration(registration);need(issuer.enable_receiving)
    r=body.get('receiver');need(r is not None and body['declaration']['session']['expected_tokens']==1 and type(charge)is int and charge<=r['max_charge_bytes'])
    d=body['declaration'];s=d['session'];source=body['source']
    value=dict(version=1,purpose='result-receive',owner=body['owner'],task=body['task'],session=s['session_id'],epoch=s['epoch'],model=d['task']['model_hash'],layout=s['layout_hash'],plan_digest=d['plan_digest'],input_snapshot=source['input_snapshot'],token_digest=source['token_digest'],preprocessing=source['preprocessing'],hidden=d['plan']['hidden_size'],vocab=d['plan']['vocab_size'],consent_id=adapter.pid,consent_digest=adapter.sha,revision=revision,c_node=r['node'],b_node=body['placements'][1]['node'],resource_id=resource_id,charge=charge,receiver=r,b_placement=body['placements'][1],declaration_digest=digest(d),authority_incarnation=issuer.incarnation)
    self=object.__new__(cls);self.adapter=adapter;self.issuer=issuer;self.registration=registration;self.descriptor=descriptor(value);self.parent=parent;self.maintenance=maintenance;self.pins=(parent,maintenance);self.claimed=False
   adapter._locked();return self
 def _check(self):
  a=self.adapter;c=a.consent;g=c.fence
  need(self.parent is self.pins[0] and self.maintenance is self.pins[1]);self.issuer._registration(self.registration)
  need(c.service._authority is c.authority and c.service._verifier is c.identity and c.authority._directory is not None and not getattr(getattr(c.authority,'_text_registration_owner',None),'_closing',False))
  need(not a.retired and not c.retired and not c.authority._poisoned and not g.dead and self.descriptor['task'] not in g.tasks and a.pid in c.live)
  need(c.trigger is c._trigger and getattr(c.trigger,'confirm',None)==c._confirm and c._now()<self.registration_body['policy']['expires_at'])
 @property
 def registration_body(self):
  import json
  return json.loads(self.registration.authorization)['body']
 @contextmanager
 def hold(self,d,action):
  need(d==self.descriptor)
  if action in ('revoke','stop-ack'):
   # System maintenance after USER/export revocation; no payload or new grant.
   need(self.maintenance is self.pins[1])
   with _context(self.maintenance.hold(d,action)) as allowed:_nothing(allowed);yield None
   return
  a=self.adapter;c=a.consent
  with c._user(a.credential,d['owner']),c.fence.lock:
   self._check();need('export' in self.registration_body['policy']['actions'])
   with _context(self.parent.hold(d,action)) as allowed:_nothing(allowed);self._check();yield None;self._check()
  need(c.service._download_owner(a.credential)==d['owner'])
  with c.fence.lock:self._check()
 def claim(self):
  c=self.adapter.consent
  with self.hold(self.descriptor,'pending'):
   if not hasattr(c.fence,'receiver_claims'):c.fence.receiver_claims=set()
   key=(c.boot,self.adapter.pid);need(not self.claimed and key not in c.fence.receiver_claims)
   c.fence.receiver_claims.add(key);self.claimed=True
