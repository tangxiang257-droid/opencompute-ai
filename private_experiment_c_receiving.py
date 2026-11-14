"""Fresh co-located experiment policy for the existing typed C protocol.

No remote permission proxy or B execution grant. Original live registration and
successful C import are required. Existing CommittedBInput and dispatch gate
remain mandatory; C's policy/resources/identity providers are never invented.
"""
import json
from contextlib import contextmanager,ExitStack
from private_experiment_c_import import LocalCInputImporter
from private_experiment_admission import need,digest
from cluster_p1.c_receiving import descriptor
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing
class _ExperimentBinding:
 @classmethod
 async def create(cls,importer,credential,pid,sha,*,parent,maintenance):
  need(type(importer)is LocalCInputImporter and callable(getattr(parent,'hold',None)) and callable(getattr(maintenance,'hold',None)))
  a=importer.admission;c=a.consent;owner=c.service._download_owner(credential)
  async with c.authority._lock:
   with c._user(credential,owner):
    importer._check();b,r=a._valid(credential,owner,pid,{'REGISTERED'});need(r[1]==sha)
    reg=a._registered.get(pid);need(reg is not None and reg.registration_id==r[4]);a._issuer._registration(reg);need(a._issuer.enable_receiving)
    row=a.db.execute('SELECT binding,charge,files,state FROM c_import_slots WHERE experiment=?',(pid,)).fetchone();need(row is not None and row[3]=='IMPORTED')
    request=json.loads(row[0]);need(request['context']==b and request['context_digest']==sha)
    d=b['declaration'];s=d['session'];source=b['source'];receiver=dict(b['receiver'],version=1)
    value=dict(version=1,purpose='result-receive',owner=owner,task=b['execution_task'],session=s['session_id'],epoch=s['epoch'],model=d['task']['model_hash'],layout=s['layout_hash'],plan_digest=d['plan_digest'],input_snapshot=source['input_snapshot'],token_digest=source['token_digest'],preprocessing=source['preprocessing'],hidden=d['plan']['hidden_size'],vocab=d['plan']['vocab_size'],consent_id=pid,consent_digest=sha,revision=b['source_revision'],c_node=receiver['node'],b_node=b['placements'][1]['node'],resource_id=receiver['resource_id'],charge=row[1],receiver=receiver,b_placement=b['placements'][1],declaration_digest=digest(d),authority_incarnation=a._issuer.incarnation)
    self=cls();self.importer=importer;self.credential=credential;self.pid=pid;self.sha=sha;self.owner=owner;self.body=b;self.row=row;self.request=request;self.registration=reg;self.issuer=a._issuer;self.descriptor=descriptor(value);self.parent=parent;self.maintenance=maintenance;self.pins=(parent,maintenance,parent.hold,maintenance.hold);self.claimed=False;self.dead=False;self.fixed=(importer,credential,pid,sha,owner,reg,a._issuer)
    self.body_digest=digest(b);self.descriptor_digest=digest(self.descriptor)
    with self.hold(self.descriptor,'prepare'):pass
   self.check();return self
 def check(self):
  try:return self._check()
  except BaseException:self.dead=True;raise
 def _check(self):
  need(not self.dead and (self.importer,self.credential,self.pid,self.sha,self.owner,self.registration,self.issuer)==self.fixed)
  need(json.loads(self.row[0])==self.request)
  i=self.importer;i._check();a=i.admission
  need(self.parent is self.pins[0] and self.maintenance is self.pins[1] and self.parent.hold==self.pins[2] and self.maintenance.hold==self.pins[3])
  need(digest(self.body)==self.body_digest and digest(self.descriptor)==self.descriptor_digest)
  # Runtime fences are synchronous: never acquire/retain the async source lock
  # over channel waits. Recheck original rows and actual immutable snapshot here.
  r=a._row(self.owner,self.pid);b=json.loads(r[0]);c=a.consent
  need(r[2]==a.boot and r[3]=='REGISTERED' and digest(b)==r[1] and c._now()<b['policy']['expires_at'])
  need(not a.db.execute('SELECT 1 FROM revoked WHERE owner=? AND source=?',(self.owner,b['source_task'])).fetchone())
  need(a._source_revision(self.owner,b['source_task'])==b['source_revision'])
  s=c.reader._read_current(self.credential,b['source_task'],self.owner)
  source=dict(input_snapshot=s.snapshot_digest,token_digest=s.token_blob_sha256,preprocessing=s.contract_sha256,model_record=s.model_record_digest)
  need(source==b['source'] and s.token_count==1 and not s.execution_allowed and not s.computation_verified)
  need(b==self.body and r[1]==self.sha and a._registered.get(self.pid) is self.registration and a._issuer is self.issuer)
  self.issuer._registration(self.registration)
  need(a.db.execute('SELECT binding,charge,files,state FROM c_import_slots WHERE experiment=?',(self.pid,)).fetchone()==self.row)
  task,_=i.artifacts._metadata(self.owner,b['execution_task']);need(task.snapshot.digest==b['source']['input_snapshot'] and task.state in ('STORED','RESULT_STORED'))
  need(i.artifacts._db.execute('SELECT state FROM inert_attempts WHERE task_id=?',(task.task_id,)).fetchone()==('PENDING',))
  if task.state=='RESULT_STORED':need(i.artifacts._db.execute('SELECT state FROM artifact_tickets WHERE task_id=? AND owner_id=?',(task.task_id,self.owner)).fetchone()==('STORED',))
 @contextmanager
 def hold(self,d,action):
  need(d==self.descriptor and digest(d)==self.descriptor_digest)
  if action in ('revoke','stop-ack'):
   need(self.maintenance is self.pins[1] and self.maintenance.hold==self.pins[3])
   with _non_suppressing(self.maintenance.hold(json.loads(json.dumps(d)),action)) as v:nothing(v);yield None
   return
  need(action in ('prepare','pending','activate','receive'))
  c=self.importer.admission.consent
  with c._user(self.credential,self.owner),c.fence.lock,ExitStack() as stack:
   self.check()
   for provider in (self.importer.identity,self.importer.capacity):nothing(stack.enter_context(_non_suppressing(provider.hold(json.loads(json.dumps(self.request)),'c-input-import'))))
   context=dict(version=1,purpose='experiment-c-receiving',context=self.body,context_digest=self.sha,descriptor=d,import_binding=self.request)
   nothing(stack.enter_context(_non_suppressing(self.parent.hold(json.loads(json.dumps(context)),action))))
   self.check();yield None;self.check()
  self.check()
 def claim(self):
  a=self.importer.admission
  with self.hold(self.descriptor,'pending'):
   if not hasattr(a,'_c_receiving_claims'):a._c_receiving_claims=set()
   need(not self.claimed and self.pid not in a._c_receiving_claims)
   a._c_receiving_claims.add(self.pid);self.claimed=True
