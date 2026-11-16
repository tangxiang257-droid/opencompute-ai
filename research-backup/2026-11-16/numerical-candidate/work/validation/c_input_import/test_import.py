"""LAB identity/capacity only; real confirmation, issuer, SQLite and snapshots."""
import json,os,sqlite3,unittest,hashlib,struct
from contextlib import contextmanager
from unittest.mock import patch
from validation.experiment_confirmation.test_entry import Tests as Base
from private_experiment_c_import import LocalCInputImporter
from result_artifacts import _Ledger,Limits
class LabProvider:
 def __init__(self):self.calls=[];self.fail=False;self.fail_exit=False
 @contextmanager
 def hold(self,context,action):
  self.calls.append((context,action))
  if self.fail:raise PermissionError('LAB reject')
  yield None
  if self.fail_exit:raise OSError('LAB exit unknown')
class Tests(Base):
 async def asyncSetUp(self):
  await super().asyncSetUp()
  for name in ('c-inputs','c-results'):(self.root/name).mkdir(mode=0o700)
  self.cin=os.open(self.root/'c-inputs',os.O_RDONLY|os.O_DIRECTORY);self.cout=os.open(self.root/'c-results',os.O_RDONLY|os.O_DIRECTORY)
  self.addCleanup(os.close,self.cin);self.addCleanup(os.close,self.cout)
  self.cdb=sqlite3.connect(self.root/'c.db',isolation_level=None);os.chmod(self.root/'c.db',0o600);self.addCleanup(self.cdb.close)
  self.artifacts=_Ledger(self.cdb,self.cin,result_fd=self.cout,limits=Limits(65536,131072,262144,524288,32,4),initialize=True)
  self.identity=LabProvider();self.capacity=LabProvider()
  self.importer=LocalCInputImporter(self.adm,self.artifacts,identity=self.identity,capacity=self.capacity,overhead_bytes=4096)
 async def registered(self):
  self.assertEqual((await self.request('POST',await self.get_form()))[0],200)
 async def run_import(self):return await self.importer.import_registered(self.token,self.receipt.proposal_id,self.receipt.proposal_digest)
 def slots(self):return self.adm.db.execute('SELECT state,charge,binding FROM c_import_slots').fetchall()
 async def test_actual_confirmation_to_exact_snapshot_and_pending(self):
  await self.registered();r=await self.run_import();body=json.loads(self.adm._row(self.owner,self.receipt.proposal_id)[0])
  self.assertEqual(r.task_id,body['execution_task']);self.assertEqual(r.snapshot_digest,self.ref.digest)
  self.assertFalse(r.execution_allowed or r.receiving_allowed)
  task,binding=self.artifacts._metadata(self.owner,r.task_id);self.assertEqual(task.snapshot,self.ref)
  self.assertEqual(self.cdb.execute('SELECT count(*) FROM inert_attempts').fetchone(),(1,))
  state,charge,raw=self.slots()[0];request=json.loads(raw)
  self.assertEqual(state,'IMPORTED');self.assertEqual(charge,request['input_bytes']+400+32768+4096)
  self.assertEqual(request['context']['receiver']['resource_id'],'c-reserved');self.assertEqual(len(self.identity.calls),1)
 async def test_missing_identity_refuses_before_writes(self):
  self.importer=LocalCInputImporter(self.adm,self.artifacts,capacity=self.capacity,overhead_bytes=4096)
  await self.registered()
  with self.assertRaises(Exception):await self.run_import()
  self.assertEqual(self.slots(),[]);self.assertEqual(os.listdir(self.cin),[])
 async def test_missing_capacity_refuses(self):
  self.importer=LocalCInputImporter(self.adm,self.artifacts,identity=self.identity,overhead_bytes=4096)
  await self.registered()
  with self.assertRaises(Exception):await self.run_import()
  self.assertEqual(self.slots(),[])
 async def test_capacity_rejection_before_publish(self):
  await self.registered();self.capacity.fail=True
  with self.assertRaises(Exception):await self.run_import()
  self.assertEqual(self.slots(),[]);self.assertEqual(os.listdir(self.cin),[])
 async def test_pending_not_importable(self):
  with self.assertRaises(Exception):await self.run_import()
  self.assertEqual(self.slots(),[])
 async def test_wrong_digest_refuses(self):
  await self.registered()
  with self.assertRaises(Exception):await self.importer.import_registered(self.token,self.receipt.proposal_id,'f'*64)
  self.assertEqual(self.slots(),[])
 async def test_revoke_after_registration_refuses(self):
  await self.registered();await self.consent.revoke(self.token,self.task.task_id)
  with self.assertRaises(Exception):await self.run_import()
  self.assertEqual(self.slots(),[])
 async def test_duplicate_no_second_copy(self):
  await self.registered();await self.run_import()
  with self.assertRaises(Exception):await self.run_import()
  self.assertEqual(len(self.slots()),1);self.assertEqual(len(os.listdir(self.cin)),1)
 async def test_pending_write_failure_retains_unknown(self):
  await self.registered()
  with patch.object(self.artifacts,'record_pending',side_effect=OSError('pending failure')):
   with self.assertRaises(OSError):await self.run_import()
  self.assertEqual(self.slots()[0][0],'UNKNOWN');self.assertGreater(self.slots()[0][1],32768);self.assertTrue(os.listdir(self.cin));self.assertTrue(self.adm.retired)
  with self.assertRaises(Exception):await self.run_import()
 async def test_capacity_exit_failure_retains_unknown(self):
  await self.registered();self.capacity.fail_exit=True
  with self.assertRaises(OSError):await self.run_import()
  self.assertEqual(self.slots()[0][0],'UNKNOWN');self.assertTrue(os.listdir(self.cin));self.assertTrue(self.adm.retired)
 async def test_database_mode_change_seals(self):
  await self.registered();os.chmod(self.root/'c.db',0o644)
  with self.assertRaises(Exception):await self.run_import()
  self.assertTrue(self.importer.retired);self.assertEqual(self.slots(),[])
 async def test_old_source_cannot_use_explicit_id(self):
  _,binding=self.ledger._metadata(self.owner,self.task.task_id)
  from snapshot_task_ledger import InputTaskLedger
  key=self.ledger._db.execute('SELECT idem_key FROM input_requests WHERE task_id=?',(self.task.task_id,)).fetchone()[0]
  with self.ledger._transaction():
   task=InputTaskLedger._commit_published_input(self.ledger,self.ledger._db,self.owner,key,binding,self.ref)
   self.assertEqual(task.snapshot,self.ref)
  before=self.ledger._db.execute('SELECT count(*) FROM stored_tasks').fetchone()
  with self.assertRaises(Exception):
   with self.ledger._transaction():InputTaskLedger._commit_published_input(self.ledger,self.ledger._db,self.owner,'new-key',binding,self.ref,_import_task_id='f'*32)
  self.assertEqual(self.ledger._db.execute('SELECT count(*) FROM stored_tasks').fetchone(),before)
 async def test_idempotency_cannot_substitute_execution_id(self):
  await self.registered();r=await self.run_import();_,binding=self.artifacts._metadata(self.owner,r.task_id)
  with self.assertRaises(Exception):self.artifacts._import_registered_input(self.owner,'f'*32,self.receipt.proposal_id,binding,self.ref)
  self.assertEqual(self.cdb.execute('SELECT count(*) FROM stored_tasks').fetchone(),(1,))
 async def test_import_feeds_existing_result_storage(self):
  await self.registered();r=await self.run_import()
  raw=struct.pack('<f',1.0)*100;entries=((0,1,hashlib.sha256(raw).hexdigest()),)
  result=self.artifacts.publish(self.owner,r.task_id,'lab-result','prefill_logits_v1','a'*64,entries,((raw,),))
  self.assertEqual(len(result),64);self.assertEqual(self.artifacts._metadata(self.owner,r.task_id)[0].state,'RESULT_STORED')
 async def test_claim_commit_reply_lost_no_publish(self):
  await self.registered();original=self.adm._tx
  @contextmanager
  def uncertain():
   with original() as db:yield db
   raise OSError('committed but reply lost')
  with patch.object(self.adm,'_tx',side_effect=uncertain):
   with self.assertRaises(OSError):await self.run_import()
  self.assertEqual(self.slots()[0][0],'UNKNOWN');self.assertEqual(os.listdir(self.cin),[])
 async def test_target_commit_reply_lost_retains_input(self):
  await self.registered();original=self.artifacts._import_registered_input
  def uncertain(*args):original(*args);raise OSError('target reply lost')
  with patch.object(self.artifacts,'_import_registered_input',side_effect=uncertain):
   with self.assertRaises(OSError):await self.run_import()
  self.assertEqual(self.slots()[0][0],'UNKNOWN');self.assertTrue(os.listdir(self.cin))
  self.assertEqual(self.cdb.execute('SELECT count(*) FROM stored_tasks').fetchone(),(1,))
def load_tests(loader,tests,pattern):return unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
