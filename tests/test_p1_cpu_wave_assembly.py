"""Trusted assembly with ordinary loader/operation stand-ins only."""
import asyncio,hashlib,sqlite3,struct,tempfile,unittest
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch
from test_p1_tile_contract_v2 import fixture
from cluster_p1.store import Conflict,canonical
from cluster_p1.local_wave import WaveAttemptJournal
from cluster_p1.stage_ownership import StageBinding,_PlannedOperation
try:
 import cluster_p1.cpu_wave_assembly as assembly
except ImportError:assembly=None
@contextmanager
def auth(*args):yield 'owner'
class Tests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.assertIsNotNone(assembly,'private CPU assembly not implemented')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.args=fixture()
  self.path=Path(self.tmp.name)/'db'
  self.j=WaveAttemptJournal.create(self.path,*self.args[:3],authorize=auth,execution_authorize=auth,enable_wave=True)
  self.addCleanup(self.j.close);self.j.register(self.args[3],hidden_size=24,vocab_size=32,event_id='plan')
  self.inputs=(struct.pack('<2q',1,2),struct.pack('<2q',3,4),struct.pack('<q',5));self.loads=[];self.calls=[];self.ops=[];self.preps=[]
 def factory(self,root,**kw):
  self.loads.append((root,kw));s=kw['session'];stage=kw['stage_id'];next_token=[0]
  def op(value,*,token_start,stage_id):
   assert stage_id==stage and token_start==next_token[0]
   n=len(value)//(8 if stage==0 else 96);next_token[0]+=n;self.calls.append((stage,token_start,value))
   return struct.pack('<f',.5)*(n*(24 if stage==0 else 32))
  ranges=tuple((t.token_start,t.token_count) for t in kw['tiles'] if t.stage_id==stage)
  out=_PlannedOperation(StageBinding(s.task_id,s.session_id,s.epoch,stage,s.model_hash,s.layout_hash),ranges,op)
  self.ops.append(out);return out
 @contextmanager
 def prepare(self,spec):self.preps.append(spec);yield 'owner'
 async def run_assembly(self,**kw):
  params=dict(expected_weight_sha256='a'*64,expected_config_sha256='c'*64,token_inputs=self.inputs,
      stage0_instance='up',stage1_instance='down',prepare_authorize=self.prepare)
  params.update(kw)
  return await assembly._run_cpu_prefill_waves(self.j,'never-read',**params)
 async def test_exact_planned_operations_and_full_flow(self):
  original=assembly.run_waves;seen=[]
  async def run(j,a,b,*args,**kw):seen.extend((a,b));return await original(j,a,b,*args,**kw)
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory),patch.object(assembly,'run_waves',side_effect=run):await self.run_assembly()
  self.assertEqual(seen,self.ops);self.assertTrue(all(type(o)is _PlannedOperation for o in seen))
  self.assertEqual([x[1]['stage_id'] for x in self.loads],[0,1]);self.assertEqual(len(self.calls),6)
  self.assertEqual([x.phase for x in self.preps],['load0','load1','postload'])
  for root,kw in self.loads:
   self.assertEqual(root,'never-read');self.assertEqual(kw['expected_sha256'],'a'*64);self.assertEqual(kw['expected_config_sha256'],'c'*64)
  self.assertEqual(len(self.j.query(2,stage_id=1)['output']),128)
 async def test_wrong_pin_before_loader(self):
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory):
   with self.assertRaises(Conflict):await self.run_assembly(expected_weight_sha256='b'*64)
  self.assertEqual(self.loads,[])
 async def test_bad_input_before_loader(self):
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory):
   with self.assertRaises(ValueError):await self.run_assembly(token_inputs=(b'x',))
  self.assertEqual(self.loads,[])
 async def test_preparation_permission_before_loader(self):
  @contextmanager
  def deny(spec):
   raise PermissionError('no load authority')
   yield
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory):
   with self.assertRaises(PermissionError):await self.run_assembly(prepare_authorize=deny)
  self.assertEqual(self.loads,[])
 async def test_stage0_load_failure_no_second_load_or_forward(self):
  def fail(*a,**kw):self.loads.append(kw);raise RuntimeError('load0')
  with patch.object(assembly,'_cpu_factory',side_effect=fail):
   with self.assertRaises(RuntimeError):await self.run_assembly()
  self.assertEqual(len(self.loads),1);self.assertEqual(self.calls,[]);self.assertIsNone(self.j.query(0))
 async def test_stage1_load_failure_no_forward(self):
  def fail(*a,**kw):
   if kw['stage_id']==1:raise RuntimeError('load1')
   return self.factory(*a,**kw)
  with patch.object(assembly,'_cpu_factory',side_effect=fail):
   with self.assertRaises(RuntimeError):await self.run_assembly()
  self.assertEqual(self.calls,[]);self.assertIsNone(self.j.query(0))
 async def test_revocation_during_load_checked_before_next_stage(self):
  def load(*a,**kw):
   op=self.factory(*a,**kw);self.j.cancel(event_id='cancel');return op
  with patch.object(assembly,'_cpu_factory',side_effect=load):
   with self.assertRaises(Conflict):await self.run_assembly()
  self.assertEqual(len(self.loads),1);self.assertEqual(self.calls,[])
 async def test_pending_cancellation_after_synchronous_load(self):
  finished=[]
  def load(*a,**kw):
   op=self.factory(*a,**kw);asyncio.current_task().cancel();finished.append(True);return op
  with patch.object(assembly,'_cpu_factory',side_effect=load):
   task=asyncio.create_task(self.run_assembly())
   with self.assertRaises(asyncio.CancelledError):await task
  self.assertEqual(finished,[True]);self.assertEqual(len(self.loads),1);self.assertEqual(self.calls,[])
 async def test_existing_attempt_not_loaded_again(self):
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory):
   await self.run_assembly();count=len(self.loads)
   with self.assertRaises(Conflict):await self.run_assembly()
  self.assertEqual(len(self.loads),count)
 async def test_wrapped_factory_operation_rejected_before_dispatch(self):
  def wrapped(*a,**kw):
   op=self.factory(*a,**kw);return lambda *a,**kw:op(*a,**kw)
  with patch.object(assembly,'_cpu_factory',side_effect=wrapped):
   with self.assertRaises(TypeError):await self.run_assembly()
  self.assertEqual(self.calls,[])
 async def test_postload_preparation_permission_rechecked(self):
  @contextmanager
  def prepare(spec):
   if spec.phase=='postload':raise PermissionError('revoked')
   yield 'owner'
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory):
   with self.assertRaises(PermissionError):await self.run_assembly(prepare_authorize=prepare)
  self.assertEqual(len(self.loads),2);self.assertEqual(self.calls,[])
 async def test_suppressed_loader_failure_still_fails(self):
  @contextmanager
  def swallow(spec):
   try:yield 'owner'
   except BaseException:pass
  with patch.object(assembly,'_cpu_factory',side_effect=RuntimeError('load')):
   with self.assertRaises(RuntimeError):await self.run_assembly(prepare_authorize=swallow)
  self.assertEqual(self.calls,[])
 async def test_invalid_config_pin_before_loader(self):
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory):
   with self.assertRaises(ValueError):await self.run_assembly(expected_config_sha256='not-a-pin')
  self.assertEqual(self.loads,[])
 async def test_budget_before_loader(self):
  with patch('cluster_p1.local_wave.MAX_RESERVED',100),patch.object(assembly,'_cpu_factory',side_effect=self.factory):
   with self.assertRaises(Conflict):await self.run_assembly()
  self.assertEqual(self.loads,[])
 async def test_wrong_loaded_binding_never_dispatched(self):
  def load(*a,**kw):
   op=self.factory(*a,**kw);op._binding=StageBinding('wrong','s',1,kw['stage_id'],'a'*64,'a'*64);return op
  with patch.object(assembly,'_cpu_factory',side_effect=load):
   with self.assertRaises(Conflict):await self.run_assembly()
  self.assertEqual(len(self.loads),1);self.assertEqual(self.calls,[])
 async def test_post_load_authorization_exit_failure_no_forward(self):
  @contextmanager
  def exit_fault(spec):
   yield 'owner'
   raise RuntimeError('after load')
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory):
   with self.assertRaises(RuntimeError):await self.run_assembly(prepare_authorize=exit_fault)
  self.assertEqual(len(self.loads),1);self.assertEqual(self.calls,[])
 async def test_live_execution_permission_still_required(self):
  @contextmanager
  def deny(spec):
   raise PermissionError('no execution authority')
   yield
  self.j._execution=deny
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory):
   with self.assertRaises(PermissionError):await self.run_assembly()
  self.assertEqual(len(self.loads),2);self.assertEqual(self.calls,[]);self.assertIsNone(self.j.query(0))
 async def test_fake_conversion_failure_after_private_state_advance_no_retry(self):
  original=self.factory
  def load(*a,**kw):
   op=original(*a,**kw)
   if kw['stage_id']==1:
    operation=op._operation
    def broken(*a,**kw):operation(*a,**kw);raise ValueError('conversion after fake KV advance')
    op._operation=broken
   return op
  with patch.object(assembly,'_cpu_factory',side_effect=load):
   with self.assertRaises(BaseExceptionGroup):await self.run_assembly()
   before=len(self.loads)
   with self.assertRaises(Conflict):await self.run_assembly()
  self.assertEqual(len(self.loads),before)
  self.assertEqual(self.j.query(0,stage_id=1)['state'],'ADMITTED')
 async def test_postload_exit_revokes_only_preparation_not_execution(self):
  allowed=[True]
  @contextmanager
  def prepare(spec):
   if not allowed[0]:raise PermissionError('preparation revoked')
   yield 'owner'
   if spec.phase=='postload':allowed[0]=False
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory):await self.run_assembly(prepare_authorize=prepare)
  self.assertFalse(allowed[0]);self.assertEqual(len(self.calls),6)
 async def test_postload_exit_revokes_execution_before_admission(self):
  @contextmanager
  def deny(spec):
   raise PermissionError('execution revoked')
   yield
  @contextmanager
  def prepare(spec):
   yield 'owner'
   if spec.phase=='postload':self.j._execution=deny
  with patch.object(assembly,'_cpu_factory',side_effect=self.factory):
   with self.assertRaises(PermissionError):await self.run_assembly(prepare_authorize=prepare)
  self.assertEqual(len(self.loads),2);self.assertEqual(self.calls,[]);self.assertIsNone(self.j.query(0))
