"""New reconstruction evidence; ordinary bytes/temporary SQLite only."""
import asyncio, hashlib, sqlite3, struct, tempfile, unittest
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch
from test_p1_tile_contract_v2 import fixture
from cluster_p1.local_attempt import LocalAttemptJournal
from cluster_p1.store import Conflict, canonical
@contextmanager
def auth(*args): yield 'owner'
class Tests(unittest.IsolatedAsyncioTestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.path=Path(self.tmp.name)/'db';self.args=fixture();self.calls=[]
  self.j=LocalAttemptJournal.create(self.path,*self.args[:3],authorize=auth,execution_authorize=auth,enable_stage1=True)
  self.addCleanup(self.j.close)
  self.j.register(self.args[3],hidden_size=24,vocab_size=32,event_id='plan')
  self.inputs=(struct.pack('<2q',1,2),struct.pack('<2q',3,4),struct.pack('<q',5))
 async def upstream(self):
  o=self.j.make_owner(lambda v,**k: struct.pack('<f',1.)*(len(v)//8*24),self.inputs,instance_id='up')
  try:
   for v,s,n in zip(self.inputs,(0,2,4),(2,2,1)):await o.call(v,token_start=s,token_count=n)
  finally:await o.close()
 def downstream(self,op=None):
  def operation(v,**kw):self.calls.append(v);return b'\0'*(len(v)//24*32)
  return self.j.make_stage1_owner(op or operation,instance_id='down')
 async def call(self,o):
  try:return await o.call(struct.pack('<f',1.)*48,token_start=0,token_count=2)
  finally:await o.close()
 async def test_complete_prefix_actual_bytes_and_tail(self):
  await self.upstream();o=self.downstream()
  try:
   for tid,s,n in ((0,0,2),(1,2,2),(2,4,1)):
    v=self.j.query(tid)['output'];out=await o.call(v,token_start=s,token_count=n)
    self.assertEqual(out,b'\0'*(n*32*4));self.assertEqual(self.j.query(tid,stage_id=1)['output'],out)
  finally:await o.close()
  self.assertEqual([len(v) for v in self.calls],[192,192,96])
  with sqlite3.connect(self.path) as c:self.assertEqual(c.execute('select sum(reserved) from p1_local_stage1_attempts').fetchone()[0],1120)
 async def test_incomplete_prefix_rejected(self):
  with self.assertRaises(Conflict):self.downstream()
 async def test_source_digest_mismatch_at_binding(self):
  await self.upstream()
  with sqlite3.connect(self.path) as c:c.execute("update p1_local_attempts set output=? where tile=0",(b'\0'*192,))
  with self.assertRaises(Conflict):self.downstream()
 async def test_source_and_event_replacement_after_binding_rejected(self):
  await self.upstream();o=self.downstream();self.replace_source()
  with self.assertRaises(Conflict):await self.call(o)
  self.assertIsNone(self.j.query(0,stage_id=1));self.assertEqual(self.calls,[])
 def replace_source(self):
  v=b'\0'*192
  with sqlite3.connect(self.path) as c:
   plan=c.execute('select plan from p1_local_attempts where tile=0').fetchone()[0]
   c.execute('update p1_local_attempts set output=? where tile=0',(v,))
   c.execute('update p1_events set payload=? where id=?',(canonical(['local_output',plan,0,hashlib.sha256(v).hexdigest()]),'local:output:0'))
 async def test_source_replacement_during_operation_keeps_admitted(self):
  await self.upstream()
  def op(v,**kw):self.replace_source();return b'\0'*256
  with self.assertRaises(Conflict):await self.call(self.downstream(op))
  self.assertEqual(self.j.query(0,stage_id=1)['state'],'ADMITTED')
 async def test_cancel_before_admission(self):
  await self.upstream();o=self.downstream();self.j.cancel(event_id='cancel')
  with self.assertRaises(Conflict):await self.call(o)
  self.assertIsNone(self.j.query(0,stage_id=1));self.assertEqual(self.calls,[])
 async def test_cancel_during_operation(self):
  await self.upstream()
  def op(v,**kw):self.j.cancel(event_id='cancel');return b'\0'*256
  with self.assertRaises(Conflict):await self.call(self.downstream(op))
  self.assertEqual(self.j.query(0,stage_id=1)['state'],'ADMITTED')
 async def test_two_owners_only_one_operation(self):
  await self.upstream();a=self.downstream();b=self.downstream()
  results=await asyncio.gather(self.call(a),self.call(b),return_exceptions=True)
  self.assertEqual(sum(type(r) is bytes for r in results),1);self.assertEqual(len(self.calls),1)
 async def test_wrong_input_no_admission(self):
  await self.upstream();o=self.downstream()
  try:
   with self.assertRaises(Conflict):await o.call(b'\0'*192,token_start=0,token_count=2)
  finally:await o.close()
  self.assertIsNone(self.j.query(0,stage_id=1))
 async def test_shared_budget_refuses_admission(self):
  await self.upstream();o=self.downstream()
  with patch('cluster_p1.local_attempt.MAX_RESERVED',967):
   with self.assertRaises(Conflict):await self.call(o)
  self.assertIsNone(self.j.query(0,stage_id=1));self.assertEqual(self.calls,[])
 async def test_shared_budget_exact_scaled_boundary(self):
  await self.upstream();o=self.downstream()
  with patch('cluster_p1.local_attempt.MAX_RESERVED',968):await self.call(o)
  self.assertEqual(len(self.calls),1)
 async def test_output_failure_retains_attempt_without_retry(self):
  await self.upstream()
  with self.assertRaises(Conflict):await self.call(self.downstream(lambda v,**kw:b'x'))
  self.assertEqual(self.j.query(0,stage_id=1)['state'],'ADMITTED')
  with self.assertRaises(Conflict):await self.call(self.downstream())
  self.assertEqual(self.calls,[])
 async def test_event_failure_rolls_back_admission(self):
  await self.upstream();o=self.downstream()
  with patch.object(self.j,'_record_event',side_effect=RuntimeError('fault')):
   with self.assertRaises(RuntimeError):await self.call(o)
  self.assertIsNone(self.j.query(0,stage_id=1));self.assertEqual(self.calls,[])
 async def test_suppressing_context_cannot_reexecute(self):
  await self.upstream();await self.call(self.downstream())
  @contextmanager
  def suppress(*args):
   try:yield 'owner'
   except BaseException:pass
  self.j._authorize=suppress;self.j._execution=suppress
  with self.assertRaises(Conflict):await self.call(self.downstream())
  self.assertEqual(len(self.calls),1)
 async def test_default_off_no_table(self):
  j=LocalAttemptJournal.create(Path(self.tmp.name)/'old',*self.args[:3],authorize=auth,execution_authorize=auth)
  try:
   with self.assertRaises(Conflict):j.make_stage1_owner(lambda *a:None,instance_id='down')
   with sqlite3.connect(j._path) as c:self.assertFalse(c.execute("select name from sqlite_master where name='p1_local_stage1_attempts'").fetchall())
  finally:j.close()
 async def test_stale_epoch_before_admission(self):
  import json
  await self.upstream();o=self.downstream()
  with sqlite3.connect(self.path) as c:
   body=json.loads(c.execute('select body from p1_sessions').fetchone()[0]);body['epoch']=2
   c.execute('update p1_sessions set body=?',(canonical(body),))
  with self.assertRaises(Conflict):await self.call(o)
  self.assertIsNone(self.j.query(0,stage_id=1));self.assertEqual(self.calls,[])
 async def test_live_execution_revocation(self):
  await self.upstream();o=self.downstream()
  @contextmanager
  def revoked(spec):
   raise PermissionError('revoked')
   yield
  self.j._execution=revoked
  with self.assertRaises(PermissionError):await self.call(o)
  self.assertIsNone(self.j.query(0,stage_id=1));self.assertEqual(self.calls,[])
 async def test_output_event_failure_rolls_back_output(self):
  await self.upstream();o=self.downstream();original=self.j._record_event
  def record(c,event,*args):
   if event.startswith('local1:output:'):raise RuntimeError('output fault')
   return original(c,event,*args)
  with patch.object(self.j,'_record_event',side_effect=record):
   with self.assertRaises(RuntimeError):await self.call(o)
  self.assertEqual(self.j.query(0,stage_id=1)['state'],'ADMITTED')
  self.assertIsNone(self.j.query(0,stage_id=1)['output']);self.assertEqual(len(self.calls),1)
 async def test_post_commit_context_exit_is_not_retry_permission(self):
  await self.upstream();o=self.downstream();calls=[0]
  @contextmanager
  def exit_fault(spec):
   calls[0]+=1
   yield 'owner'
   if calls[0]==2:raise RuntimeError('after commit')
  self.j._execution=exit_fault
  with self.assertRaises(RuntimeError):await self.call(o)
  self.assertEqual(self.j.query(0,stage_id=1)['state'],'OUTPUT_COMMITTED')
  self.j._execution=auth
  with self.assertRaises(Conflict):await self.call(self.downstream())
  self.assertEqual(len(self.calls),1)
