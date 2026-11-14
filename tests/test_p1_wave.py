"""Ordinary-thread/SQLite wave evidence, never a real model test."""
import asyncio, hashlib, sqlite3, struct, tempfile, threading, unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from test_p1_tile_contract_v2 import fixture
from cluster_p1.store import Conflict, canonical
try:
 from cluster_p1.local_wave import WaveAttemptJournal, SourceNotReady, run_waves
except ImportError:
 WaveAttemptJournal=None
@contextmanager
def auth(*args):yield 'owner'
class Tests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.assertIsNotNone(WaveAttemptJournal,'explicit wave adapter not implemented')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.path=Path(self.tmp.name)/'db';self.args=fixture();self.loop=asyncio.get_running_loop()
  self.j=WaveAttemptJournal.create(self.path,*self.args[:3],authorize=auth,execution_authorize=auth,enable_wave=True)
  self.addCleanup(self.j.close)
  self.j.register(self.args[3],hidden_size=24,vocab_size=32,event_id='plan')
  self.inputs=(struct.pack('<2q',1,2),struct.pack('<2q',3,4),struct.pack('<q',5))
  self.log=[];self.next=[0,0];self.lock=threading.Lock()
 def op(self,v,*,token_start,stage_id):
  count=len(v)//(8 if stage_id==0 else 96)
  with self.lock:
   assert self.next[stage_id]==token_start
   self.next[stage_id]+=count;self.log.append((stage_id,token_start,v))
  return struct.pack('<f',1.)*(count*(24 if stage_id==0 else 32))
 async def run_wave(self,a=None,b=None):
  return await run_waves(self.j,a or self.op,b or self.op,self.inputs,stage0_instance='up',stage1_instance='down')
 async def first_upstream(self):
  o=self.j.make_owner(self.op,self.inputs,instance_id='up')
  try:return await o.call(self.inputs[0],token_start=0,token_count=2)
  finally:await o.close()
 async def test_no_full_prefix_needed_and_not_ready_no_retirement(self):
  d=self.j.make_wave_stage1_owner(self.op,instance_id='down')
  try:
   with self.assertRaises(SourceNotReady):await d.call_next()
   await self.first_upstream();out=await d.call_next()
   self.assertEqual(len(out),256);self.assertEqual(d.state.next_token,2)
   self.assertIsNone(self.j.query(1))
  finally:await d.close()
 async def test_exact_same_request_overlap_barrier_and_tail(self):
  entered=[asyncio.Event(),asyncio.Event()];release=threading.Event();self.addCleanup(release.set)
  def operation(v,**kw):
   slot=0 if (kw['stage_id'],kw['token_start'])==(0,2) else 1 if (kw['stage_id'],kw['token_start'])==(1,0) else None
   if slot is not None:
    self.loop.call_soon_threadsafe(entered[slot].set)
    if not release.wait(5):raise RuntimeError('test deadlock')
   return self.op(v,**kw)
  task=asyncio.create_task(self.run_wave(operation,operation))
  try:
   await asyncio.wait_for(asyncio.gather(*(e.wait() for e in entered)),3)
   self.assertFalse(task.done());release.set();await task
  finally:
   release.set()
   if not task.done():task.cancel()
   await asyncio.gather(task,return_exceptions=True)
  self.assertEqual(self.next,[5,5]);self.assertEqual([x[1] for x in self.log if x[0]==1],[0,2,4])
  self.assertEqual(len(self.j.query(2,stage_id=1)['output']),128)
 async def test_cancellation_waits_for_both_inflight_operations(self):
  entered=[asyncio.Event(),asyncio.Event()];release=threading.Event();exited=[threading.Event(),threading.Event()]
  self.addCleanup(release.set)
  def operation(v,**kw):
   slot=0 if (kw['stage_id'],kw['token_start'])==(0,2) else 1 if (kw['stage_id'],kw['token_start'])==(1,0) else None
   if slot is not None:
    self.loop.call_soon_threadsafe(entered[slot].set)
    try:
     if not release.wait(5):raise RuntimeError('deadlock')
    finally:exited[slot].set()
   return self.op(v,**kw)
  task=asyncio.create_task(self.run_wave(operation,operation))
  try:
   await asyncio.wait_for(asyncio.gather(*(e.wait() for e in entered)),3);task.cancel()
   # Event-loop turn barrier, not an elapsed-time/throughput assertion.
   tick=asyncio.Event();self.loop.call_soon(tick.set);await tick.wait()
   self.assertFalse(task.done());release.set()
   with self.assertRaises(asyncio.CancelledError):await task
  finally:
   release.set();await asyncio.gather(task,return_exceptions=True)
  self.assertTrue(all(e.is_set() for e in exited));self.assertFalse(any(x[1]==4 for x in self.log))
  self.assertEqual(self.j.query(1)['state'],'ADMITTED');self.assertEqual(self.j.query(0,stage_id=1)['state'],'ADMITTED')
 async def test_failure_retires_sibling_and_no_next_wave(self):
  def op(v,**kw):
   if kw['stage_id']==1:raise ValueError('downstream failure')
   return self.op(v,**kw)
  with self.assertRaises(BaseExceptionGroup):await self.run_wave(self.op,op)
  self.assertFalse(any(x[1]==4 for x in self.log));self.assertEqual(self.j.query(0,stage_id=1)['state'],'ADMITTED')
 async def test_handoff_preflight_is_read_only_and_not_permission(self):
  with sqlite3.connect(self.path) as c:before=tuple(c.iterdump())
  r=self.j.preflight_handoff_budget(wire_layout_version=2)
  self.assertFalse(r.execution_allowed);self.assertEqual(r.local_retained_bytes,1640)
  with sqlite3.connect(self.path) as c:self.assertEqual(tuple(c.iterdump()),before)
  self.assertEqual(self.log,[])
 async def test_handoff_v1_rejected(self):
  with self.assertRaises(ValueError):self.j.preflight_handoff_budget(wire_layout_version=1)
 async def test_budget_preflight_before_operation(self):
  with patch('cluster_p1.local_wave.MAX_RESERVED',1639):
   with self.assertRaises(Conflict):await self.run_wave()
  self.assertEqual(self.log,[]);self.assertIsNone(self.j.query(0))
 async def test_all_wave_budget_retained(self):
  await self.run_wave()
  with sqlite3.connect(self.path) as c:
   total=sum(c.execute(f'select sum(reserved) from {t}').fetchone()[0] for t in ('p1_local_attempts','p1_local_stage1_attempts'))
  self.assertEqual(total,1640)
 async def test_epoch_change_before_next_admission(self):
  import json
  await self.first_upstream();d=self.j.make_wave_stage1_owner(self.op,instance_id='down')
  with sqlite3.connect(self.path) as c:
   s=json.loads(c.execute('select body from p1_sessions').fetchone()[0]);s['epoch']=2;c.execute('update p1_sessions set body=?',(canonical(s),))
  try:
   with self.assertRaises(Conflict):await d.call_next()
  finally:await d.close()
  self.assertIsNone(self.j.query(0,stage_id=1))
 async def test_source_change_during_operation_rejects_commit(self):
  await self.first_upstream()
  def op(v,**kw):
   with sqlite3.connect(self.path) as c:
    plan=c.execute('select plan from p1_local_attempts where tile=0').fetchone()[0]
    value=b'\0'*192;c.execute('update p1_local_attempts set output=? where tile=0',(value,))
    c.execute('update p1_events set payload=? where id=?',(canonical(['local_output',plan,0,hashlib.sha256(value).hexdigest()]),'local:output:0'))
   return self.op(v,**kw)
  d=self.j.make_wave_stage1_owner(op,instance_id='down')
  try:
   with self.assertRaises(Conflict):await d.call_next()
  finally:await d.close()
  self.assertEqual(self.j.query(0,stage_id=1)['state'],'ADMITTED')
 async def test_duplicate_owner_not_permission(self):
  await self.first_upstream();a=self.j.make_wave_stage1_owner(self.op,instance_id='down');b=self.j.make_wave_stage1_owner(self.op,instance_id='down')
  try:
   results=await asyncio.gather(a.call_next(),b.call_next(),return_exceptions=True)
   self.assertEqual(sum(type(r)is bytes for r in results),1)
  finally:await a.close();await b.close()
 async def test_explicit_mode_required(self):
  with self.assertRaises(ValueError):WaveAttemptJournal.create(Path(self.tmp.name)/'other',*self.args[:3],authorize=auth,execution_authorize=auth,enable_wave=False)
 async def test_execution_revocation_denies_admission(self):
  await self.first_upstream();d=self.j.make_wave_stage1_owner(self.op,instance_id='down')
  @contextmanager
  def deny(spec):
   raise PermissionError('revoked')
   yield
  self.j._execution=deny
  try:
   with self.assertRaises(PermissionError):await d.call_next()
  finally:await d.close()
  self.assertIsNone(self.j.query(0,stage_id=1))
 async def test_suppression_does_not_allow_duplicate(self):
  await self.first_upstream();a=self.j.make_wave_stage1_owner(self.op,instance_id='down')
  try:await a.call_next()
  finally:await a.close()
  @contextmanager
  def swallow(*args):
   try:yield 'owner'
   except BaseException:pass
  self.j._authorize=swallow;self.j._execution=swallow
  b=self.j.make_wave_stage1_owner(self.op,instance_id='down')
  try:
   with self.assertRaises(Conflict):await b.call_next()
  finally:await b.close()
 async def test_adapter_direct_entry_checks_budget(self):
  with patch('cluster_p1.local_wave.MAX_BYTES',100):
   with self.assertRaises(Conflict):self.j.make_wave_stage1_owner(self.op,instance_id='down')
 async def test_single_prefill_tile_drains(self):
  self.j.close();args=list(self.args);args[1]=args[1].model_copy(update={'expected_tokens':2})
  self.j=WaveAttemptJournal.create(Path(self.tmp.name)/'single',*args[:3],authorize=auth,execution_authorize=auth,enable_wave=True)
  self.addCleanup(self.j.close);self.j.register(args[3][:2],hidden_size=24,vocab_size=32,event_id='plan')
  self.inputs=self.inputs[:1];await self.run_wave();self.assertEqual(self.next,[2,2])
 async def test_output_event_failure_has_no_retry(self):
  original=self.j._record_event
  def fail(c,event,*args):
   if event=='local1:output:0':raise RuntimeError('output rollback')
   return original(c,event,*args)
  with patch.object(self.j,'_record_event',side_effect=fail):
   with self.assertRaises(BaseExceptionGroup):await self.run_wave()
  self.assertEqual(self.j.query(0,stage_id=1)['state'],'ADMITTED')
  d=self.j.make_wave_stage1_owner(self.op,instance_id='down')
  try:
   with self.assertRaises(Conflict):await d.call_next()
  finally:await d.close()
  self.assertFalse(any(x[1]==4 for x in self.log))
 async def test_partial_output_commit_survives_other_wave_failure(self):
  committed=asyncio.Event();self.addCleanup(committed.set)
  original=self.j._record_event
  def record(c,event,*args):
   result=original(c,event,*args)
   if event=='local1:output:0':committed.set()
   return result
  # Use an event from the loop to release the upstream failure after B[0] commit.
  gate=threading.Event();self.addCleanup(gate.set)
  def up(v,**kw):
   if kw['token_start']==2:
    if not gate.wait(5):raise RuntimeError('deadlock')
    raise ValueError('upstream fault after downstream commit')
   return self.op(v,**kw)
  with patch.object(self.j,'_record_event',side_effect=record):
   task=asyncio.create_task(self.run_wave(up,self.op))
   try:
    await asyncio.wait_for(committed.wait(),3);gate.set()
    with self.assertRaises(BaseExceptionGroup):await task
   finally:gate.set();await asyncio.gather(task,return_exceptions=True)
  self.assertEqual(self.j.query(0,stage_id=1)['state'],'OUTPUT_COMMITTED')
  self.assertEqual(self.j.query(1)['state'],'ADMITTED')
 async def test_wave_admission_is_not_atomic(self):
  original=self.j._record_event
  def fail(c,event,*args):
   if event=='local1:admit:0':raise RuntimeError('downstream admission rollback')
   return original(c,event,*args)
  with patch.object(self.j,'_record_event',side_effect=fail):
   with self.assertRaises(BaseExceptionGroup):await self.run_wave()
  # Upstream admission committed separately; this is not all-or-none reserve.
  self.assertIsNotNone(self.j.query(1));self.assertIsNone(self.j.query(0,stage_id=1))
  self.assertFalse(any(x[1]==4 for x in self.log))
 async def test_prebinding_matching_replacement_is_not_authenticity_proof(self):
  await self.first_upstream()
  value=struct.pack('<f',2.)*48
  with sqlite3.connect(self.path) as c:
   plan=c.execute('select plan from p1_local_attempts where tile=0').fetchone()[0]
   c.execute('update p1_local_attempts set output=? where tile=0',(value,))
   c.execute('update p1_events set payload=? where id=?',(canonical(['local_output',plan,0,hashlib.sha256(value).hexdigest()]),'local:output:0'))
  d=self.j.make_wave_stage1_owner(self.op,instance_id='down')
  try:await d.call_next()
  finally:await d.close()
  self.assertEqual(self.log[-1][2],value) # trusted store: consistency, not origin proof
