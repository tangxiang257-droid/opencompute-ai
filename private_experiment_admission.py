"""Fresh isolated experiment confirmation -> existing issuer registration only.

No execution/claim adapter, C import, public route or trusted default provider.
Old v12/v13 rows and plan prohibitions remain untouched. All reopen is query-only.
A single coordinator calls the existing issuer synchronously once; any uncertain
outcome burns eligibility, even when history currently reports no registration.
"""
import asyncio,json,os,stat,threading,uuid
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from private_task_consent import TaskConsent,need,text,hashtext,digest
from cluster_p1.online_issuance import _Database,IssuanceAuthority,_make_declaration
from cluster_p1.store import canonical
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing

PURPOSE='confirm-isolated-single-token-execution'
@dataclass(frozen=True)
class Confirmation:
 proposal_id:str
 proposal_digest:str
 owner:str
 action:str=PURPOSE
@dataclass(frozen=True)
class Receipt:
 proposal_id:str
 proposal_digest:str
 state:str
 execution_allowed:bool=False

class ExperimentAdmission(_Database):
 def __init__(self,path,consent,*,source_domain,trigger=None,policy_authority=None):
  need(type(consent)is TaskConsent);hashtext(source_domain)
  p=Path(path).absolute();s=p.parent.lstat();need(stat.S_ISDIR(s.st_mode) and s.st_uid==os.geteuid() and stat.S_IMODE(s.st_mode)&0o077==0)
  if p.exists() or p.is_symlink():need(stat.S_ISREG(p.lstat().st_mode) and not p.is_symlink())
  self.consent=consent;self.domain=source_domain;self.trigger=trigger;self.policy=policy_authority
  self._consent=consent;self._domain=source_domain;self._hold=getattr(policy_authority,'hold',None)
  self._trigger=trigger;self._confirm=getattr(trigger,'confirm',None);self._policy=policy_authority
  self.loop=asyncio.get_running_loop();self.boot=uuid.uuid4().hex;self.published=set();self.active=None;self._issuer=None;self._issuer_pin=None;self._issuer_resources=None
  self._open(p)
  try:
   with self._tx() as c:
    if self.fresh:
     c.execute('CREATE TABLE config(version INTEGER,domain TEXT)');c.execute('INSERT INTO config VALUES(1,?)',(source_domain,))
     c.execute('CREATE TABLE revoked(owner TEXT,source TEXT,PRIMARY KEY(owner,source))')
     c.execute("CREATE TABLE experiments(id TEXT PRIMARY KEY,owner TEXT,source TEXT,task TEXT UNIQUE,body TEXT,digest TEXT,boot TEXT,state TEXT CHECK(state IN('PENDING','CONFIRMING','CONFIRMED','DISPATCHING','REGISTERED','REVOKED','UNKNOWN')),registration TEXT,UNIQUE(owner,source))")
    need(c.execute('SELECT version,domain FROM config').fetchall()==[(1,source_domain)])
  except BaseException:self.close();raise
 def _ready(self,write=True):
  try:
   self._check(write);need(asyncio.get_running_loop() is self.loop)
   need(self.consent is self._consent and self.domain==self._domain)
   need(self.trigger is self._trigger and getattr(self.trigger,'confirm',None)==self._confirm and self.policy is self._policy and getattr(self.policy,'hold',None)==self._hold)
   self.consent._current()
   if self._issuer is not None:
    need((self._issuer.namespace,self._issuer.incarnation)==self._issuer_pin and self._issuer.source_provider is self and self._issuer.authorize is self and self._issuer.resources is self._issuer_resources)
    if write:self._issuer.ready()
   if write:need(callable(self._confirm) and callable(self._hold) and asyncio.current_task().cancelling()==0)
  except BaseException:self.retired=True;self.published.clear();raise
 def _row(self,owner,pid):
  text(pid);r=self.db.execute('SELECT body,digest,boot,state,registration FROM experiments WHERE id=? AND owner=?',(pid,owner)).fetchone();need(r is not None);return r
 def _source_revision(self,owner,source_task):
  c=self.consent;ledger=c._current()
  need(not c.fence.dead and source_task not in c.fence.tasks)
  row=ledger._db.execute('SELECT revision,revoked FROM consent_tasks WHERE task=? AND owner=?',(source_task,owner)).fetchone()
  need(row is None or row[1]==0)
  return 0 if row is None else row[0]
 def _valid(self,credential,owner,pid,states):
  self._ready();r=self._row(owner,pid);b=json.loads(r[0]);need(r[2]==self.boot and r[3] in states and digest(b)==r[1])
  need(not self.db.execute('SELECT 1 FROM revoked WHERE owner=? AND source=?',(owner,b['source_task'])).fetchone())
  need(self.consent._now()<b['policy']['expires_at'])
  need(self._source_revision(owner,b['source_task'])==b['source_revision'])
  need(self._issuer_pin==(b['issuer']['namespace'],b['issuer']['incarnation']))
  source,s=self.consent._source(credential,b['source_task'],owner);need(source==b['source'] and s.token_count==1)
  return b,r
 @contextmanager
 def _policy_scope(self,body,action):
  with _non_suppressing(self.policy.hold(json.loads(canonical(body)),action)) as v:nothing(v);yield
 def _unknown(self,pid):
  self.published.clear();self.retired=True
  try:
   with self._tx() as c:c.execute("UPDATE experiments SET state='UNKNOWN' WHERE id=? AND state!='REVOKED'",(pid,))
  except BaseException:pass # In-memory poison remains even if durable write is unknown.
 async def prepare(self,credential,source_task,declaration,policy,placements,*,receiver,c_input_copy,issuer):
  self._ready();text(source_task);c=self.consent;owner=c.service._download_owner(credential)
  need(type(issuer)is IssuanceAuthority and issuer.require_consent and issuer.source_provider is self and issuer.authorize is self)
  need(self._issuer is None or self._issuer is issuer);issuer.ready()
  self._issuer=issuer;self._issuer_pin=(issuer.namespace,issuer.incarnation);self._issuer_resources=issuer.resources
  d,p,places=c._proposal(declaration,policy,placements)
  need(d['task']['owner_id']==owner and d['task']['task_id']==source_task and d['session']['expected_tokens']==1)
  need(p['privacy'] in ('L1','L2','L3') and d['task']['privacy_level']==p['privacy'])
  need(Decimal(str(d['task']['cost_cap']))==Decimal(p['max_cost_microunits'])/1000000 and d['task']['deadline']==p['expires_at'])
  need(set(p['actions'])=={'register','claim','install','load','execute','commit','export'})
  receiver=json.loads(canonical(receiver));need(set(receiver)=={'node','identity','purpose','storage_policy','retention_seconds','max_charge_bytes'})
  for k in ('node','identity','storage_policy'):text(receiver[k])
  need(receiver['node'] not in [x['node'] for x in places] and receiver['purpose']=='result-store')
  need(type(receiver['retention_seconds'])is int and 0<receiver['retention_seconds']<=86400)
  need(type(receiver['max_charge_bytes'])is int and d['plan']['vocab_size']*4+32768<=receiver['max_charge_bytes']<=2097152)
  copy=json.loads(canonical(c_input_copy));need(set(copy)=={'allowed','destination','purpose','input_snapshot'} and copy['allowed'] is True and copy['destination']==receiver['node'] and copy['purpose']=='result-store');hashtext(copy['input_snapshot'])
  pid=uuid.uuid4().hex;execution=uuid.uuid4().hex;nonce=uuid.uuid4().hex;attempted=False
  try:
   async with c.authority._lock:
    self._ready()
    with c._user(credential,owner):
     source,s=c._source(credential,source_task,owner);revision=self._source_revision(owner,source_task)
     need(s.token_count==1 and copy['input_snapshot']==source['input_snapshot'])
     need((s.model.weights_sha256,s.model.config_sha256,s.vocab_size)==(d['task']['model_hash'],d['config_sha256'],d['plan']['vocab_size']))
     need(d['task']['payload_ref']==source['input_snapshot'])
     d['task']['task_id']=execution;d['session'].update(task_id=execution,session_id=nonce)
     for stage in d['stages']:stage['session_id']=nonce
     for tile in d['plan']['tiles']:tile.update(task_id=execution,session_id=nonce)
     d=_make_declaration(d['task'],d['session'],d['stages'],d['plan']['tiles'],d['plan']['hidden_size'],d['plan']['vocab_size'],d['config_sha256'])
     body=dict(version=1,purpose=PURPOSE,source_domain=self.domain,source_task=source_task,source_revision=revision,issuer=dict(namespace=issuer.namespace,incarnation=issuer.incarnation),execution_task=execution,owner=owner,source=source,source_status=dict(tokenizer=s.tokenizer_status,model=s.model_status,computation_verified=False),declaration=d,policy=p,placements=places,receiver=receiver,c_input_copy=copy)
     raw=canonical(body);need(len(raw.encode())<=30000);sha=digest(body)
     with self._policy_scope(body,'prepare'):
      with self._tx() as db:
       need(not db.execute('SELECT 1 FROM revoked WHERE owner=? AND source=?',(owner,source_task)).fetchone())
       attempted=True;db.execute("INSERT INTO experiments VALUES(?,?,?,?,?,?,?,'PENDING',NULL)",(pid,owner,source_task,execution,raw,sha,self.boot))
    self._valid(credential,owner,pid,{'PENDING'})
   return Receipt(pid,sha,'PENDING')
  except BaseException:
   if attempted:self._unknown(pid)
   raise
 async def confirm(self,credential,pid,sha):
  self._ready();hashtext(sha);c=self.consent;owner=c.service._download_owner(credential);attempted=False
  try:
   async with c.authority._lock:
    with c._user(credential,owner):
     body,r=self._valid(credential,owner,pid,{'PENDING'});need(r[1]==sha)
     with self._tx() as db:attempted=True;db.execute("UPDATE experiments SET state='CONFIRMING' WHERE id=?",(pid,))
   # No local identity/SQLite/resource lock across explicit user interaction.
   answer=await self._confirm(credential,Receipt(pid,sha,'CONFIRMING'),json.loads(canonical(body)))
   need(type(answer)is Confirmation and answer==Confirmation(pid,sha,owner))
   async with c.authority._lock:
    with c._user(credential,owner),self._policy_scope(body,'confirm'):
     self._valid(credential,owner,pid,{'CONFIRMING'})
     with self._tx() as db:db.execute("UPDATE experiments SET state='CONFIRMED' WHERE id=?",(pid,))
    # SQL CONFIRMED is not eligibility: publish only after all scope exits.
    self._valid(credential,owner,pid,{'CONFIRMED'});self.published.add(pid)
   return Receipt(pid,sha,'CONFIRMED')
  except BaseException:
   if attempted:self._unknown(pid)
   raise
 async def revoke(self,credential,source_task):
  self._ready();text(source_task);c=self.consent;owner=c.service._download_owner(credential)
  try:
   async with c.authority._lock:
    with c._user(credential,owner):
     with self._tx() as db:
      db.execute('INSERT OR IGNORE INTO revoked VALUES(?,?)',(owner,source_task))
      db.execute("UPDATE experiments SET state='REVOKED' WHERE owner=? AND source=?",(owner,source_task))
    self.published.difference_update(row[0] for row in self.db.execute('SELECT id FROM experiments WHERE owner=? AND source=?',(owner,source_task)))
  except BaseException:self.retired=True;self.published.clear();raise
 def check(self,namespace,source_id,owner,task_id,declaration_digest,source_binding):
  a=self.active;need(a is not None and a['thread']==threading.get_ident() and a['task'] is asyncio.current_task() and self.consent.authority._lock.locked() and not a['checked'])
  b,r=self._valid(a['credential'],owner,a['pid'],{'DISPATCHING'})
  need((namespace,source_id,owner,task_id,declaration_digest,source_binding)==(a['namespace'],a['source_id'],b['owner'],b['execution_task'],digest(b['declaration']),{k:b['source'][k] for k in ('input_snapshot','token_digest','preprocessing')}));a['checked']=True
 @contextmanager
 def hold(self,request,action):
  # Deliberately no claim/install/execute authority in this increment.
  a=self.active;need(action=='register' and a is not None and a['task'] is asyncio.current_task() and a['thread']==threading.get_ident() and self.consent.authority._lock.locked())
  b,r=self._valid(a['credential'],a['owner'],a['pid'],{'DISPATCHING'})
  need(request==dict(version=2,declaration=b['declaration'],source_binding={k:b['source'][k] for k in ('input_snapshot','token_digest','preprocessing')},authorization=a['context']))
  with self._policy_scope(b,'register'):yield None;self._valid(a['credential'],a['owner'],a['pid'],{'DISPATCHING'})
 async def dispatch(self,credential,pid,sha,issuer):
  self._ready();hashtext(sha);need(type(issuer)is IssuanceAuthority and issuer is self._issuer and issuer.require_consent and issuer.source_provider is self and issuer.authorize is self)
  c=self.consent;owner=c.service._download_owner(credential);attempted=False
  try:
   async with c.authority._lock:
    with c._user(credential,owner):
     b,r=self._valid(credential,owner,pid,{'CONFIRMED'});need(r[1]==sha and pid in self.published)
     with self._policy_scope(b,'admit'):
      with self._tx() as db:attempted=True;db.execute("UPDATE experiments SET state='DISPATCHING' WHERE id=?",(pid,))
     self.published.remove(pid)
     context=dict(version=1,kind='isolated-experiment-registration',experiment_id=pid,context_digest=sha,authority_boot=self.boot,body=b)
     source_id=digest([self.domain,owner,b['source_task'],PURPOSE])
     self.active=dict(pid=pid,source_id=source_id,owner=owner,credential=credential,namespace=issuer.namespace,task=asyncio.current_task(),thread=threading.get_ident(),checked=False,context=context)
     d=b['declaration']
     try:
      reg=issuer.register(d['task'],d['session'],d['stages'],d['plan']['tiles'],hidden_size=d['plan']['hidden_size'],vocab_size=d['plan']['vocab_size'],config_sha256=d['config_sha256'],source_id=source_id,source_binding={k:b['source'][k] for k in ('input_snapshot','token_digest','preprocessing')},authorization_context=context)
      need(self.active['checked'])
     finally:self.active=None
     self._valid(credential,owner,pid,{'DISPATCHING'})
     with self._tx() as db:db.execute("UPDATE experiments SET state='REGISTERED',registration=? WHERE id=?",(reg.registration_id,pid))
    self._valid(credential,owner,pid,{'REGISTERED'})
   # Only inert receipt, never the issuer's live _Registration object.
   return Receipt(pid,sha,'REGISTERED')
  except BaseException:
   self.active=None
   if attempted:self._unknown(pid);issuer.retired=True
   raise
 async def history(self,credential,pid):
  self._ready(False);c=self.consent;owner=c.service._download_owner(credential)
  async with c.authority._lock:
   with c._user(credential,owner):r=self._row(owner,pid)
  return Receipt(pid,r[1],r[3])
 def close(self):self.published.clear();self.active=None;super().close()
