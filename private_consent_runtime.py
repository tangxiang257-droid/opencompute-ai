"""Local consent checks AND existing authority, never a standalone execution grant.

No network or async operation under identity/runtime locks. Remote roles require
an additional real online revocation protocol; this view is single-process only.
"""
import asyncio,json,os,threading,time
from contextlib import contextmanager
from private_task_consent import TaskConsent,need,digest,hashtext
from cluster_p1.store import canonical
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing,Spec

class IssuanceConsent:
 def __init__(self,consent,credential,proposal_id,proposal_digest,*,parent=None):
  need(type(consent)is TaskConsent)
  self.consent=consent;self.credential=credential;self.pid=proposal_id;self.sha=proposal_digest
  self.parent=parent;self._parent=parent;self.registration=None;self.context=None;self.retired=False;self.views=set()
 def _locked(self):
  c=self.consent;need(not self.retired and self.parent is self._parent and callable(getattr(self.parent,'hold',None)))
  ledger=c._current();need(c.authority._lock.locked())
  owner=c.service._download_owner(self.credential)
  row=c._row(ledger,owner,self.pid);body=c._valid(ledger,row,self.credential,owner)
  need(row[3]==self.sha and row[5]=='ACTIVE' and self.pid in c.live)
  with c.fence.lock:need(not c.fence.dead and body['task'] not in c.fence.tasks)
  return body,row[1]
 def context_locked(self,declaration,source):
  body,revision=self._locked();need(body['declaration']==declaration)
  need(source=={k:body['source'][k] for k in ('input_snapshot','token_digest','preprocessing')})
  result=dict(version=1,consent_id=self.pid,consent_digest=self.sha,revision=revision,authority_boot=self.consent.boot,body=body)
  if self.context is not None:need(canonical(result)==self.context)
  return result
 @contextmanager
 def hold(self,request,action):
  # Only synchronous issuer leaf under the source A/F boundary; not a wire permit.
  need(type(request)is dict and request.get('version')==2 and action in ('register','claim'))
  body,_=self._locked();need(action in body['policy']['actions'])
  if action=='register':
   need(set(request)=={'version','declaration','source_binding','authorization'})
   expected=self.context_locked(request['declaration'],request['source_binding'])
  else:
   need(set(request)=={'version','intent','authorization'})
   v=request['intent'];expected=self.context_locked(v['declaration'],v['source_binding'])
   need(v['owner']==body['owner'] and v['task']==body['task'])
   place=body['placements'][v['stage']]
   need(all(v[k]==place[k] for k in ('node','device','instance','resource_id')))
  need(request['authorization']==expected)
  try:
   with _non_suppressing(self.parent.hold(json.loads(canonical(request)),action)) as result:
    nothing(result);self._locked();yield None;self._locked()
   self._locked()
  except BaseException:self.retired=True;raise
 def registered_locked(self,issuer,registration,declaration,source):
  need(self.registration is None and issuer.require_consent and issuer.authorize is self)
  context=self.context_locked(declaration,source);need(registration.authorization==canonical(context))
  issuer._registration(registration);self.context=canonical(context);self.registration=(issuer,registration)
 async def claim_online(self,issuer,registration,intent,peer,admission):
  c=self.consent;attempted=False
  try:
   async with c.authority._lock:
    with c._user(self.credential,c.service._download_owner(self.credential)),_non_suppressing(admission()):
     self._locked();need(self.registration==(issuer,registration));attempted=True
     response=issuer.claim_stage(registration,intent,peer)
     self._locked()
    self._locked();return response
  except BaseException:
   if attempted:self.retired=True;issuer.retired=True
   raise
 async def runtime(self,stage,spec,*,parent=None,maintenance=None):
  c=self.consent
  async with c.authority._lock:
   with c._user(self.credential,c.service._download_owner(self.credential)):
    body,_=self._locked();need(self.registration is not None and stage not in self.views)
    issuer,registration=self.registration;issuer._registration(registration)
    view=RuntimeConsent(self,body,stage,spec,parent,maintenance)
   self._locked();self.views.add(stage);return view

class RuntimeConsent:
 def __init__(self,issuer,body,stage,spec,parent,maintenance):
  need(type(stage)is int and stage in (0,1) and type(spec)is Spec)
  need(parent is not None and maintenance is not None and callable(getattr(maintenance,'hold',None)))
  required=('hold','hold_load','hold_stage0') if stage==0 else ('hold','hold_load','hold_call','finalize')
  need(all(callable(getattr(parent,k,None)) for k in required))
  self.issuer=issuer;self.consent=issuer.consent;self.body=json.loads(canonical(body));self.stage=stage;self.spec=spec
  self.parent=parent;self._parent=parent;self.maintenance=maintenance;self._maintenance=maintenance
  self.pid=os.getpid();self.retired=False;self.binding=None;self.finalized=False
  self.deadline=time.monotonic()+max(0,body['policy']['expires_at']-self.consent._now())
  d=body['declaration'];s=d['session']
  need((spec.owner,spec.request,spec.session,spec.epoch,spec.model,spec.layout,spec.plan_digest,spec.input_snapshot,spec.token_digest,spec.preprocessing)==
       (body['owner'],body['task'],s['session_id'],s['epoch'],d['task']['model_hash'],s['layout_hash'],d['plan_digest'],body['source']['input_snapshot'],body['source']['token_digest'],body['source']['preprocessing']))
  need((spec.hidden,spec.vocab)==(d['plan']['hidden_size'],d['plan']['vocab_size']))
  if stage==0:need(spec.inputs==(body['source']['token_digest'],))
 def _identity(self):
  c=self.consent
  need(os.getpid()==self.pid and c.service._verifier is c.identity and c.service._authority is c.authority)
  need(self.parent is self._parent and self.maintenance is self._maintenance)
  return c.identity.hold(self.issuer.credential)
 def _check(self,action):
  c=self.consent;g=c.fence
  need(not self.retired and not self.issuer.retired and not c.retired and not c.authority._poisoned)
  need(not getattr(getattr(c.authority,'_text_registration_owner',None),'_closing',False))
  need(not getattr(c.authority,'_task_consent_failed',False) and c.authority._directory is not None)
  need(c.trigger is c._trigger and getattr(c.trigger,'confirm',None)==c._confirm)
  need(not g.dead and self.body['task'] not in g.tasks and self.issuer.pid in c.live)
  need(c._now()<self.body['policy']['expires_at'] and time.monotonic()<self.deadline)
  need(action in self.body['policy']['actions'])
 def check(self,action):
  try:
   with _non_suppressing(self._identity()):
    need(self.consent.service._download_owner(self.issuer.credential)==self.body['owner'])
    with self.consent.fence.lock:self._check(action)
   need(self.consent.service._download_owner(self.issuer.credential)==self.body['owner'])
   with self.consent.fence.lock:self._check(action)
  except BaseException:self.retired=True;raise
 def bind_control(self,binding):
  need(self.binding is None and type(binding)is dict)
  s=self.spec;p=self.body['placements'][self.stage]
  expected=dict(owner=s.owner,task=s.request,session=s.session,epoch=s.epoch,stage=self.stage,model=s.model,layout=s.layout,plan=s.plan_digest,input_snapshot=s.input_snapshot,token_digest=s.token_digest,preprocessing=s.preprocessing,input_digest=s.inputs[0],device=p['device'],instance=p['instance'],declaration_digest=digest(self.body['declaration']))
  need(all(binding.get(k)==v for k,v in expected.items()))
  self.check('install');self.binding=canonical(binding)
 @contextmanager
 def hold(self,binding,action):
  need(self.binding is not None and canonical(binding)==self.binding)
  if action in ('query','fail'):
   # Independent identity-scoped maintenance: no new work or quota release.
   with _non_suppressing(self._identity()),_non_suppressing(self.maintenance.hold(binding,action)) as result:
    nothing(result);need(self.consent.service._download_owner(self.issuer.credential)==self.body['owner']);yield None
    need(self.consent.service._download_owner(self.issuer.credential)==self.body['owner'])
   need(self.consent.service._download_owner(self.issuer.credential)==self.body['owner']);return
  mapping={'register':'install','prepare':'install','input-ready':'install','execute':'execute','execute-start':'execute','complete':'commit'}
  need(action in mapping);self.check(mapping[action])
  try:
   with _non_suppressing(self.parent.hold(binding,action)) as result:
    nothing(result);self.check(mapping[action]);yield None;self.check(mapping[action])
   self.check(mapping[action])
  except BaseException:self.retired=True;raise
 @contextmanager
 def _work(self,method,action,*args):
  self.check(action)
  try:
   with _non_suppressing(getattr(self.parent,method)(*args)) as result:
    nothing(result);self.check(action);yield None;self.check(action)
   self.check(action)
  except BaseException:self.retired=True;raise
 def hold_load(self,spec,stage):
  need(spec==self.spec and type(stage)is int and stage==self.stage);return self._work('hold_load','load',spec,stage)
 def hold_stage0(self,spec):
  need(self.stage==0 and spec==self.spec);return self._work('hold_stage0','execute',spec)
 def hold_call(self,spec,token):
  need(self.stage==1 and spec==self.spec and type(token)is int and token==0);return self._work('hold_call','execute',spec,token)
 def finalize(self,spec,token,output_digest,commit):
  need(self.stage==1 and spec==self.spec and type(token)is int and token==0 and callable(commit) and not self.finalized)
  hashtext(output_digest)
  # Identity -> short revocation fence -> existing bounded local journal commit.
  # Parent finalize must not perform network waits, compute or device sync.
  calls=0;completed=False;active=True;thread=threading.get_ident()
  def checked():
   nonlocal calls,completed
   try:
    calls+=1;need(active and threading.get_ident()==thread and calls==1)
    with _non_suppressing(self._identity()):
     need(self.consent.service._download_owner(self.issuer.credential)==self.body['owner'])
     with self.consent.fence.lock:
      self._check('commit');commit();self._check('commit')
    completed=True
   except BaseException:self.retired=True;raise
  try:
   self.check('commit');self.finalized=True
   nothing(self.parent.finalize(spec,token,output_digest,checked));need(calls==1 and completed);self.check('commit')
  except BaseException:self.retired=True;raise
  finally:active=False
