"""Confirmed local C permission AND genuine receiving policy, never B eligibility.

Only for C co-located with the consent authority. No serialized receipt restores
this handle on another process/host/restart. Parent must attest local node identity,
capacity, retention policy and original connection; missing parent denies.
"""
import os,time,json
from dataclasses import asdict
from contextlib import contextmanager
from private_task_consent import TaskConsent,need
from cluster_p1.store import canonical
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import Spec,nothing
from cluster_p1.split_role_transport import transport_scope,_result_binding,_generation,ResultIngress

class ReceiverConsent:
 def __init__(self):raise TypeError('use await create')
 @classmethod
 async def create(cls,consent,credential,proposal_id,sha,spec,binding,*,parent=None):
  need(cls is ReceiverConsent and type(consent)is TaskConsent and type(spec)is Spec and len(spec.inputs)==1 and callable(getattr(parent,'hold_receiver',None)))
  c=consent
  async with c.authority._lock:
   ledger=c._current();owner=c.service._download_owner(credential)
   with c._user(credential,owner):
    row=c._row(ledger,owner,proposal_id);body=c._valid(ledger,row,credential,owner)
    need(row[3]==sha and row[5]=='ACTIVE' and proposal_id in c.live and 'receiver' in body and 'export' in body['policy']['actions'])
    d=body['declaration'];s=d['session'];r=body['receiver'];need(s['expected_tokens']==1)
    need((spec.owner,spec.request,spec.session,spec.epoch,spec.model,spec.layout,spec.plan_digest,spec.input_snapshot,spec.token_digest,spec.preprocessing,spec.hidden,spec.vocab)==
         (owner,body['task'],s['session_id'],s['epoch'],d['task']['model_hash'],s['layout_hash'],d['plan_digest'],body['source']['input_snapshot'],body['source']['token_digest'],body['source']['preprocessing'],d['plan']['hidden_size'],d['plan']['vocab_size']))
    _result_binding(binding,spec,binding.scope.incarnation)
    need(binding.scope.peer_node==body['placements'][1]['node'] and r['node']!=binding.scope.peer_node and spec.output_bytes+32768<=r['max_charge_bytes'])
    self=object.__new__(cls);self.consent=c;self.credential=credential;self.proposal_id=proposal_id;self.sha=sha
    self.body=json.loads(canonical(body));self.spec=spec;self.expected=json.loads(json.dumps(asdict(binding)));self.parent=parent;self._parent=parent
    self.pid=os.getpid();self.retired=False;self.claimed=False;self.generation=None;self.wire_binding=None;self.deadline=time.monotonic()+max(0,body['policy']['expires_at']-c._now())
   c._current();c._valid(ledger,c._row(ledger,owner,proposal_id),credential,owner)
   with c.fence.lock:
    self._check()
    if not hasattr(c.fence,'receiver_claims'):c.fence.receiver_claims=set()
    key=(c.boot,proposal_id);need(key not in c.fence.receiver_claims)
    c.fence.receiver_claims.add(key) # One local receiving qualification; never clear on failure.
   return self
 def _check(self):
  c=self.consent;g=c.fence
  need(os.getpid()==self.pid and not self.retired and self.parent is self._parent)
  need(not c.retired and not c.authority._poisoned and not getattr(c.authority,'_task_consent_failed',False))
  need(c.authority._directory is not None and not getattr(getattr(c.authority,'_text_registration_owner',None),'_closing',False))
  need(c.service._verifier is c.identity and c.service._authority is c.authority)
  need(c.trigger is c._trigger and getattr(c.trigger,'confirm',None)==c._confirm)
  need(not g.dead and self.body['task'] not in g.tasks and self.proposal_id in c.live)
  need(c._now()<self.body['policy']['expires_at'] and time.monotonic()<self.deadline)
 @contextmanager
 def hold_receiver(self,context,action):
  c=self.consent
  try:
   need(self.claimed and self.generation is not None)
   need(action in {'result-metadata','read-metadata','result-reserve','result-store','result-commit','send-metadata'})
   need(type(context)is dict and set(context)=={'recipient_node','purpose','receive_binding','binding'})
   need(context['recipient_node']==self.body['receiver']['node'] and context['purpose']=='result-store' and context['receive_binding']==self.expected)
   current=context['binding'];scope=transport_scope(self.spec)
   if current!=scope:
    need(type(current)is dict and current.get('scope')==scope and current.get('actual_inputs')==list(self.spec.inputs)
         and current.get('generation')==self.generation and current.get('binding')==self.wire_binding)
   with _non_suppressing(c.identity.hold(self.credential)):
    need(c.service._download_owner(self.credential)==self.body['owner'])
    with c.fence.lock:
     self._check()
     # The parent sees confirmed policy, never just a free-form destination name.
     request=dict(context=context,receiver=self.body['receiver'],consent_id=self.proposal_id,consent_digest=self.sha)
     with _non_suppressing(self.parent.hold_receiver(json.loads(canonical(request)),action)) as allowed:
      nothing(allowed);self._check();yield None;self._check()
     self._check()
    need(c.service._download_owner(self.credential)==self.body['owner'])
   need(c.service._download_owner(self.credential)==self.body['owner'])
   with c.fence.lock:self._check()
  except BaseException:self.retired=True;raise
 def ingress(self,reader,writer,config,metadata_channel,binding,*,identity=None,authorize=None):
  need(not self.claimed and not self.retired and self.parent is not authorize)
  self.claimed=True
  try:
   need(json.loads(json.dumps(asdict(binding)))==self.expected)
   self.generation=_generation(writer)
   self.wire_binding=json.loads(json.dumps(asdict(_result_binding(binding,self.spec,self.generation))))
   return ResultIngress(reader,writer,config,metadata_channel,binding,identity=identity,authorize=authorize,
                        recipient_node=self.body['receiver']['node'],receive_authorize=self)
  except BaseException:self.retired=True;raise
