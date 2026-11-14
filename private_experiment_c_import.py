"""Central/co-located C input import into the EXISTING exact result ledger.

No remote transport, C ACK, receive/execute grant or quota release. Missing real
C identity/policy/capacity providers refuse. Only an original fresh admission
with a successful current REGISTERED outcome may spend its permanent import slot.
"""
import asyncio,json,os,stat
from pathlib import Path
from contextlib import ExitStack
from dataclasses import dataclass
from private_experiment_admission import ExperimentAdmission,need,digest
from cluster_p1.local_attempt import _non_suppressing
from cluster_p1.projection_driver import nothing
from cluster_p1.store import canonical
from result_artifacts import _Ledger
from token_input_snapshot import read_input,publish_input,_root

@dataclass(frozen=True)
class ImportReceipt:
 experiment_id:str
 task_id:str
 snapshot_digest:str
 state:str
 execution_allowed:bool=False
 receiving_allowed:bool=False

class LocalCInputImporter:
 def __init__(self,admission,artifacts,*,identity=None,capacity=None,overhead_bytes=None):
  need(type(admission)is ExperimentAdmission and type(artifacts)is _Ledger)
  need(type(overhead_bytes)is int and 4096<=overhead_bytes<=1048576)
  self.admission=admission;self.artifacts=artifacts;self.identity=identity;self.capacity=capacity;self.overhead=overhead_bytes
  self._pins=(admission,artifacts,identity,capacity,overhead_bytes,getattr(identity,'hold',None),getattr(capacity,'hold',None));self.retired=False
  self.pid=os.getpid();self.loop=asyncio.get_running_loop()
  self.target=self._directories();self.connection=artifacts._db;self.database=self._database()
 def _directories(self):
  values=[]
  for fd in (self.artifacts._directory,self.artifacts._results):
   duplicate=_root(fd)
   try:info=os.fstat(duplicate);values.append((info.st_dev,info.st_ino))
   finally:os.close(duplicate)
  need(values[0]!=values[1]);return tuple(values)
 def _database(self):
  rows=self.artifacts._db.execute('PRAGMA database_list').fetchall();need(len(rows)==1 and rows[0][1]=='main')
  path=Path(rows[0][2]);need(path.is_absolute() and path==path.resolve(strict=True))
  info=path.lstat();parent=path.parent.lstat()
  need(stat.S_ISREG(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1)
  need(stat.S_ISDIR(parent.st_mode) and parent.st_uid==os.geteuid() and stat.S_IMODE(parent.st_mode)==0o700)
  return (info.st_dev,info.st_ino)
 def _check(self):
  try:
   need(not self.retired and self.pid==os.getpid() and self.loop is asyncio.get_running_loop())
   need(all(x is y for x,y in zip((self.admission,self.artifacts,self.identity,self.capacity),self._pins[:4])) and self.overhead==self._pins[4] and getattr(self.identity,'hold',None)==self._pins[5] and getattr(self.capacity,'hold',None)==self._pins[6])
   need(callable(self._pins[5]) and callable(self._pins[6]) and self._directories()==self.target and self.artifacts._db is self.connection and self._database()==self.database)
   self.admission._ready();need(self.admission.schema_version==2)
  except BaseException:self.retired=True;raise
 def _fail(self,pid):
  self.retired=True;self.admission.retired=True;self.admission.published.clear()
  try:
   with self.admission._tx() as db:db.execute("UPDATE c_import_slots SET state='UNKNOWN' WHERE experiment=?",(pid,))
  except BaseException:pass # retained in-memory poison + reopen query-only
 async def import_registered(self,credential,pid,sha):
  self._check();a=self.admission;c=a.consent;owner=c.service._download_owner(credential);attempted=False
  try:
   async with c.authority._lock:
    self._check()
    with c._user(credential,owner):
     b,row=a._valid(credential,owner,pid,{'REGISTERED'});need(row[1]==sha and row[4] is not None and b['version']==2)
     need(a._issuer.db.execute('SELECT task,owner FROM registrations WHERE id=?',(row[4],)).fetchone()==(b['execution_task'],owner))
     copy=b['c_input_copy'];receiver=b['receiver'];need(copy==dict(allowed=True,destination=receiver['node'],purpose='result-store',input_snapshot=b['source']['input_snapshot']))
     source_ledger=c._current();task,binding=source_ledger._metadata(owner,b['source_task'])
     stored=read_input(source_ledger._directory,task.snapshot,binding)
     need(len(stored.tokens)==1 and stored.token_blob_sha256==b['source']['token_digest'] and task.snapshot.digest==copy['input_snapshot'])
     source_info=os.fstat(source_ledger._directory);need((source_info.st_dev,source_info.st_ino) not in self.target)
     file_info=os.stat(task.snapshot.digest+'.tokens',dir_fd=source_ledger._directory,follow_symlinks=False)
     need(stat.S_ISREG(file_info.st_mode) and 0<file_info.st_size<=4096)
     output=b['declaration']['plan']['vocab_size']*4;charge=file_info.st_size+output+32768+self.overhead
     need(charge<=receiver['max_charge_bytes'] and output<=self.artifacts._limits.file_bytes and output+32768<=self.artifacts._limits.task_bytes)
     # Preserve full original placement in the binding. Hashes qualify labels by
     # node and kind, not proof of physical device/instance uniqueness.
     assignments=tuple((p['node'],digest(['c-device-claim-v1',p['node'],p['device']]),digest(['c-instance-claim-v1',p['node'],p['instance']])) for p in b['placements'])
     request=dict(version=1,context=b,context_digest=sha,experiment_id=pid,registration_id=row[4],task_id=b['execution_task'],target_directories=[list(v) for v in self.target],target_database=list(self.database),input_bytes=file_info.st_size,output_bytes=output,manifest_bytes=32768,overhead_bytes=self.overhead,charge_bytes=charge,file_budget=8,assignments=[list(v) for v in assignments])
     raw=canonical(request);need(len(raw.encode())<=32768)
     with ExitStack() as stack:
      for provider in (a.policy,self.identity,self.capacity):nothing(stack.enter_context(_non_suppressing(provider.hold(json.loads(raw),'c-input-import'))))
      self._check();a._valid(credential,owner,pid,{'REGISTERED'})
      with a._tx() as db:
       need(not db.execute('SELECT 1 FROM c_import_slots WHERE experiment=?',(pid,)).fetchone());attempted=True
       db.execute("INSERT INTO c_import_slots VALUES(?,?,?,8,'CLAIMED')",(pid,raw,charge))
      reference=publish_input(self.artifacts._directory,binding,stored.tokens)
      need(reference==task.snapshot)
      # Actual import uses the shared validated input core, not copied SQLite or
      # fabricated test records. Every following failure keeps files/charge/slot.
      imported=self.artifacts._import_registered_input(owner,b['execution_task'],pid,binding,reference)
      need(imported.task_id==b['execution_task'] and imported.snapshot==reference)
      self.artifacts.record_pending(owner,imported.task_id,b['declaration']['session']['session_id'],1,b['declaration']['plan_digest'],assignments)
      self._check();a._valid(credential,owner,pid,{'REGISTERED'})
     self._check();a._valid(credential,owner,pid,{'REGISTERED'})
     with a._tx() as db:db.execute("UPDATE c_import_slots SET state='IMPORTED' WHERE experiment=?",(pid,))
    self._check();a._valid(credential,owner,pid,{'REGISTERED'})
   return ImportReceipt(pid,imported.task_id,reference.digest,'IMPORTED')
  except BaseException:
   if attempted:self._fail(pid)
   raise
