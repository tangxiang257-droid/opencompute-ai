"""Private fresh-v13 task consent, unmounted and never an execution provider.

Identity is the existing service USER fence; a separately trusted explicit-action
provider is mandatory. Records bind real stored input and a complete proposed
P1 declaration. v12 execution prohibition and all device/resource gates remain.
"""
import asyncio,hashlib,json,math,time,uuid,threading
from dataclasses import dataclass
from contextlib import contextmanager
from input_declaration import InputDeclarationReader
from private_source_bridge import LocalUserFence
from text_input_ledger import TextInputLedger
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.online_issuance import _make_declaration
from cluster_p1.contracts import Task,Session,Stage
from cluster_p1.store import canonical

class ConsentRejected(ValueError):
 def __init__(self):super().__init__('task consent unavailable')
def need(value):
 if not value:raise ConsentRejected()
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()
def text(value):need(type(value)is str and 0<len(value)<=128 and all(32<ord(c)<127 for c in value))
def hashtext(value):need(type(value)is str and len(value)==64 and all(c in '0123456789abcdef' for c in value))

class ConsentLedger(TextInputLedger):
 """Fresh only: no migration, execution state or quota ledger."""
 VERSION=13
 SCHEMA=TextInputLedger.SCHEMA+(
  "CREATE TABLE consent_tasks(task TEXT PRIMARY KEY REFERENCES stored_tasks(task_id), owner TEXT NOT NULL, revision INTEGER NOT NULL, revoked INTEGER NOT NULL CHECK(revoked IN(0,1)))",
  "CREATE TABLE task_consents(id TEXT PRIMARY KEY, task TEXT NOT NULL REFERENCES consent_tasks(task), owner TEXT NOT NULL, revision INTEGER NOT NULL, body TEXT NOT NULL CHECK(length(body)<=32768), digest TEXT NOT NULL, boot TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN('PENDING','CONFIRMING','ACTIVE','REVOKED','FAILED')), UNIQUE(task))",
  "CREATE TRIGGER consent_task_binding BEFORE INSERT ON consent_tasks WHEN NOT EXISTS(SELECT 1 FROM stored_tasks WHERE task_id=NEW.task AND owner_id=NEW.owner) BEGIN SELECT RAISE(ABORT,'binding'); END",
  "CREATE TRIGGER consent_binding BEFORE INSERT ON task_consents WHEN NOT EXISTS(SELECT 1 FROM consent_tasks WHERE task=NEW.task AND owner=NEW.owner AND revision=NEW.revision AND revoked=0) BEGIN SELECT RAISE(ABORT,'binding'); END",
  "CREATE TRIGGER consent_task_identity BEFORE UPDATE OF task,owner ON consent_tasks BEGIN SELECT RAISE(ABORT,'immutable'); END",
  "CREATE TRIGGER consent_task_transition BEFORE UPDATE ON consent_tasks WHEN NOT(NEW.revoked=1 AND NEW.revision=OLD.revision+1) BEGIN SELECT RAISE(ABORT,'transition'); END",
  "CREATE TRIGGER consent_task_no_delete BEFORE DELETE ON consent_tasks BEGIN SELECT RAISE(ABORT,'immutable'); END",
  "CREATE TRIGGER consent_identity BEFORE UPDATE OF id,task,owner,revision,body,digest,boot ON task_consents BEGIN SELECT RAISE(ABORT,'immutable'); END",
  "CREATE TRIGGER consent_transition BEFORE UPDATE ON task_consents WHEN NOT((OLD.state='PENDING' AND NEW.state='CONFIRMING') OR (OLD.state='CONFIRMING' AND NEW.state='ACTIVE') OR (OLD.state IN('PENDING','CONFIRMING','ACTIVE') AND NEW.state IN('REVOKED','FAILED'))) BEGIN SELECT RAISE(ABORT,'transition'); END",
  "CREATE TRIGGER consent_no_delete BEFORE DELETE ON task_consents BEGIN SELECT RAISE(ABORT,'immutable'); END",
 )

@dataclass(frozen=True)
class Confirmation:
 """Trusted callback response only, not a portable execution credential."""
 proposal_id:str
 proposal_digest:str
 owner:str
 action:str='confirm-task-execution'
@dataclass(frozen=True)
class Receipt:
 proposal_id:str
 proposal_digest:str
 state:str
 execution_allowed:bool=False

class _RuntimeFence:
 """Process-local invalidation only; no remote replica or device-stop claim."""
 def __init__(self):self.lock=threading.RLock();self.dead=False;self.tasks=set()
 def invalidate(self,task=None):
  with self.lock:
   if task is None:self.dead=True
   else:self.tasks.add(task)

class TaskConsent:
 def __init__(self,reader,*,identity_fence=None,trigger=None,clock=time.time):
  need(type(reader)is InputDeclarationReader and isinstance(identity_fence,LocalUserFence))
  need(reader._service is not None and reader._service._verifier is identity_fence)
  self.reader=reader;self.service=reader._service;self.authority=reader._authority
  if not hasattr(self.authority,'_task_consent_runtime_fence'):self.authority._task_consent_runtime_fence=_RuntimeFence()
  self.fence=self.authority._task_consent_runtime_fence
  self.identity=identity_fence;self.trigger=trigger;self._trigger=trigger;self._confirm=getattr(trigger,'confirm',None);self.clock=clock;self.boot=uuid.uuid4().hex
  self.loop=asyncio.get_running_loop();self.live=set();self.retired=False;self.last_time=None
  self.pins=(reader._model,reader._model_record_digest,reader._preprocessing_version,reader._contract_sha256)
 def _current(self):
  if self.trigger is not self._trigger or getattr(self.trigger,'confirm',None)!=self._confirm:self.retired=True
  need(not self.retired and asyncio.get_running_loop() is self.loop)
  need(self.reader._service is self.service and self.reader._authority is self.authority and self.service._authority is self.authority and self.service._verifier is self.identity)
  need((self.reader._model,self.reader._model_record_digest,self.reader._preprocessing_version,self.reader._contract_sha256)==self.pins)
  need(not getattr(self.authority,'_task_consent_failed',False))
  self.authority._check();ledger=self.authority._middle_ledger();need(type(ledger)is ConsentLedger);return ledger
 def _now(self):
  value=self.clock();need(type(value) in (int,float) and math.isfinite(value))
  if self.last_time is not None and value<self.last_time:self.retired=True;raise ConsentRejected()
  self.last_time=value;return value
 @contextmanager
 def _user(self,credential,owner):
  with _non_suppressing(self.identity.hold(credential)):
   need(self.service._download_owner(credential)==owner);yield
   need(self.service._download_owner(credential)==owner)
 def _source(self,credential,task,owner):
  s=self.reader._read_locked(credential,task,owner)
  need(not s.execution_allowed and not s.computation_verified)
  return dict(input_snapshot=s.snapshot_digest,token_digest=s.token_blob_sha256,preprocessing=s.contract_sha256,model_record=s.model_record_digest),s
 def _proposal(self,declaration,policy,placements):
  # Freeze and run actual P1 complete-plan validation; reject unknown keys.
  raw=canonical(declaration);need(len(raw)<=32768);d=json.loads(raw)
  need(set(d)=={'task','session','stages','plan','plan_digest','config_sha256'})
  rebuilt=_make_declaration(d['task'],d['session'],d['stages'],d['plan']['tiles'],d['plan']['hidden_size'],d['plan']['vocab_size'],d['config_sha256'])
  need(canonical(rebuilt)==raw)
  p=json.loads(canonical(policy));need(set(p)=={'max_cost_microunits','currency','price_policy','privacy','privacy_policy','expires_at','actions','offline'})
  need(type(p['max_cost_microunits'])is int and 0<p['max_cost_microunits']<=2**53-1)
  for k in ('currency','price_policy','privacy','privacy_policy'):text(p[k])
  need(p['offline'] is False and type(p['expires_at'])is int and self._now()<p['expires_at']<=self.last_time+300)
  need(type(p['actions'])is list and p['actions']==sorted(set(p['actions'])) and p['actions'] and set(p['actions'])<= {'register','claim','install','load','execute','commit','export'})
  places=json.loads(canonical(placements));need(type(places)is list and len(places)==len(d['stages']))
  for stage,v in zip(d['stages'],places):
   need(type(v)is dict and set(v)=={'stage','node','device','instance','resource_id'} and type(v['stage'])is int and v['stage']==stage['stage_id'] and v['node']==stage['assigned_node'])
   for k in ('node','device','instance','resource_id'):text(v[k])
  return d,p,places
 async def prepare(self,credential,declaration,policy,placements,*,receiver=None):
  self._current();need(callable(getattr(self.trigger,'confirm',None)))
  d,p,places=self._proposal(declaration,policy,placements)
  receiver=None if receiver is None else json.loads(canonical(receiver))
  owner=self.service._download_owner(credential);task=d['task']['task_id'];need(d['task']['owner_id']==owner)
  async with self.authority._lock:
   ledger=self._current()
   with self._user(credential,owner):
    source,s=self._source(credential,task,owner)
    need((s.model.weights_sha256,s.model.config_sha256,s.token_count,s.vocab_size)==(d['task']['model_hash'],d['config_sha256'],d['session']['expected_tokens'],d['plan']['vocab_size']))
    proposal=dict(owner=owner,task=task,source=source,declaration=d,policy=p,placements=places)
    if receiver is not None:
     need(d['session']['expected_tokens']==1)
     r=json.loads(canonical(receiver));need(set(r)=={'version','node','identity','purpose','storage_policy','retention_seconds','max_charge_bytes'})
     need(type(r['version'])is int and r['version']==1 and r['purpose']=='result-store' and 'export' in p['actions'])
     for k in ('node','identity','storage_policy'):text(r[k])
     need(r['node'] not in [x['node'] for x in places])
     need(type(r['retention_seconds'])is int and 0<r['retention_seconds']<=86400)
     need(type(r['max_charge_bytes'])is int and d['plan']['vocab_size']*4+32768<=r['max_charge_bytes']<=2097152)
     proposal['receiver']=r
    body=canonical(proposal);need(len(body.encode())<=32768);pid=uuid.uuid4().hex;sha=hashlib.sha256(body.encode()).hexdigest()
    with ledger._transaction():
     ledger._db.execute('INSERT OR IGNORE INTO consent_tasks VALUES(?,?,0,0)',(task,owner))
     need(ledger._db.execute('SELECT owner,revision,revoked FROM consent_tasks WHERE task=?',(task,)).fetchone()==(owner,0,0))
     ledger._db.execute("INSERT INTO task_consents VALUES(?,?,?,0,?,?,?,'PENDING')",(pid,task,owner,body,sha,self.boot))
   self._current();self._valid(ledger,self._row(ledger,owner,pid),credential,owner)
   need(self._row(ledger,owner,pid)[5]=='PENDING')
   return Receipt(pid,sha,'PENDING')
 def _row(self,ledger,owner,pid):
  text(pid);row=ledger._db.execute('SELECT task,revision,body,digest,boot,state FROM task_consents WHERE id=? AND owner=?',(pid,owner)).fetchone();need(row is not None);return row
 def _valid(self,ledger,row,credential,owner):
  task,revision,raw,sha,boot,state=row;need(boot==self.boot and digest(json.loads(raw))==sha)
  need(ledger._db.execute('SELECT owner,revision,revoked FROM consent_tasks WHERE task=?',(task,)).fetchone()==(owner,revision,0))
  body=json.loads(raw);need(self._now()<body['policy']['expires_at'])
  source,_=self._source(credential,task,owner);need(source==body['source']);return body
 async def confirm(self,credential,pid,expected_digest):
  self._current();hashtext(expected_digest);owner=self.service._download_owner(credential);attempted=False
  try:
   async with self.authority._lock:
    ledger=self._current()
    with self._user(credential,owner):
     row=self._row(ledger,owner,pid);body=self._valid(ledger,row,credential,owner);need(row[3]==expected_digest and row[5]=='PENDING')
     with ledger._transaction():ledger._db.execute("UPDATE task_consents SET state='CONFIRMING' WHERE id=?",(pid,))
     attempted=True
   # No authority/identity lock over the real user's external confirmation wait.
   result=await self.trigger.confirm(credential,Receipt(pid,expected_digest,'CONFIRMING'),json.loads(canonical(body)))
   need(type(result)is Confirmation and result==Confirmation(pid,expected_digest,owner))
   async with self.authority._lock:
    ledger=self._current()
    with self._user(credential,owner):
     row=self._row(ledger,owner,pid);self._valid(ledger,row,credential,owner);need(row[5]=='CONFIRMING')
     with ledger._transaction():ledger._db.execute("UPDATE task_consents SET state='ACTIVE' WHERE id=?",(pid,))
    # Publish live qualification only AFTER all synchronous fences exit.
    self._current();final=self._row(ledger,owner,pid);self._valid(ledger,final,credential,owner)
    need(final[5]=='ACTIVE' and final[3]==expected_digest);self.live.add(pid)
    return Receipt(pid,expected_digest,'ACTIVE')
  except BaseException:
   if attempted:
    self.live.discard(pid)
    try:
     async with self.authority._lock:
      ledger=self._current()
      with ledger._transaction():ledger._db.execute("UPDATE task_consents SET state='FAILED' WHERE id=? AND state IN('PENDING','CONFIRMING','ACTIVE')",(pid,))
    except BaseException:self.retired=True
   raise
 async def revoke(self,credential,task):
  self._current();owner=self.service._download_owner(credential)
  async with self.authority._lock:
   ledger=self._current()
   # An unauthorized miss must not poison another user's live consents.
   with self._user(credential,owner):
    need(ledger._db.execute('SELECT 1 FROM stored_tasks WHERE task_id=? AND owner_id=?',(task,owner)).fetchone() is not None)
   self.fence.invalidate(task) # Conservative local fence precedes any uncertain durable revoke.
   try:
    with self._user(credential,owner):
     with ledger._transaction():
      ledger._db.execute('INSERT OR IGNORE INTO consent_tasks VALUES(?,?,0,0)',(task,owner))
      ledger._db.execute('UPDATE consent_tasks SET revision=revision+1,revoked=1 WHERE task=? AND owner=?',(task,owner))
      ledger._db.execute("UPDATE task_consents SET state='REVOKED' WHERE task=? AND owner=? AND state IN('PENDING','CONFIRMING','ACTIVE')",(task,owner))
    return None
   except BaseException:
    self.fence.invalidate();self.authority._task_consent_failed=True;self.retired=True;self.live.clear();raise
 async def check_context(self,credential,pid,expected_digest,action):
  """Consent fact check only. Does not authorize v12 execution or return a permit."""
  self._current();owner=self.service._download_owner(credential)
  async with self.authority._lock:
   ledger=self._current()
   with self._user(credential,owner):
    row=self._row(ledger,owner,pid);body=self._valid(ledger,row,credential,owner)
    need(pid in self.live and row[5]=='ACTIVE' and row[3]==expected_digest and action in body['policy']['actions'])
   self._current();final=self._row(ledger,owner,pid);self._valid(ledger,final,credential,owner)
   need(final[5]=='ACTIVE' and final[3]==expected_digest and pid in self.live)
   return Receipt(pid,expected_digest,'ACTIVE')

 async def history(self,credential,pid):
  # Authenticated history is not a live grant and cannot populate self.live.
  self._current();owner=self.service._download_owner(credential)
  async with self.authority._lock:
   ledger=self._current()
   with self._user(credential,owner):
    row=self._row(ledger,owner,pid)
    return Receipt(pid,row[3],row[5])


def open_consent_service(location,input_fd,result_fd,limits,*,tls,recipient,deployment=None,verifier=None):
 """Fresh-only private v13 service. No migration, mounting, plan provider or listener."""
 import controlled_delivery_authority as control
 import noncompute_application_service as app
 import task_input_api as identity
 from dataclasses import replace
 need(type(tls.peers)is dict and len(tls.peers)==1)
 service=object.__new__(app.ApplicationService)
 service._tls=replace(tls,peers=dict(tls.peers));service._verifier=identity.RejectAllUsers() if verifier is None else verifier
 service._plans=app.RejectPlans();service._connections={}
 service._authority=control._assemble_authority(location,input_fd,result_fd,limits,deployment=deployment,recipient=recipient,certificate_pins=dict(tls.peers),initialize=True,delivery_mode='framed_result_set_v1',enable_unified=True,_fresh_ledger=ConsentLedger)
 return service
