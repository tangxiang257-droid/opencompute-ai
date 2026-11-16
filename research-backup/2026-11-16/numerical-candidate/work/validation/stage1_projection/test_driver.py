import dataclasses,hashlib,struct,tempfile,threading,unittest
from contextlib import contextmanager
from pathlib import Path
try:
 from validation.stage1_projection.driver import Spec,Policy,Journal,Driver,Denied
except ImportError:Spec=Policy=Journal=Driver=None;Denied=ValueError

class Doubles:
 def __init__(self):self.log=[];self.fail=None;self.final='once';self.blocks=[];self.barrier=None
 def hit(self,name):
  self.log.append(name)
  if self.barrier and name==self.barrier[0]:self.barrier[1].set();assert self.barrier[2].wait(5)
  if name==self.fail:raise RuntimeError(name)
 @contextmanager
 def hold_call(self,spec,token):
  self.hit('auth_enter')
  try:yield None
  finally:self.hit('auth_exit')
 def finalize(self,spec,token,digest,commit):
  self.hit('finalize')
  if self.final=='zero':return
  commit()
  if self.final=='double':
   try:commit()
   except BaseException:pass
  if self.final=='lost':raise RuntimeError('response lost')
 def reserve_request(self,spec,policy):self.hit('reserve');return object()
 def verify_token(self,spec,ticket,token):self.hit('verify')
 def quarantine(self,*args):self.hit('quarantine')
 def check_binding(self,spec):self.hit('binding')
 def prepare_token(self,scope,value,token):self.hit('prepare');return token
 def project_slice(self,scope,handle,column,count):self.hit('project');return handle,column,count
 def materialize(self,scope,pending,size):
  self.hit('materialize');token,col,count=pending
  return struct.pack('<'+'f'*count,*[float(token*100000+i) for i in range(col,col+count)])
 def open_scope(self,spec,token,scope):self.hit('scope')
 def finish_scope(self,scope):self.hit('finish')
 def install_cache(self,scope,handle):self.hit('install')
 def retire(self,spec):self.hit('backend_retire')
 def append(self,token,column,block):self.hit('append');self.blocks.append((token,column,block))
 def seal(self,token,digest):self.hit('seal')

class Tests(unittest.TestCase):
 def setUp(self):
  self.assertIsNotNone(Driver,'driver missing');self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.values=(struct.pack('<2f',1,2),struct.pack('<2f',3,4))
  self.spec=Spec('owner','request','nonce',1,*['a'*64]*6,2,3,tuple(hashlib.sha256(v).hexdigest() for v in self.values))
  self.policy=Policy(100,200,300,1000000,262144,2097152,2097152)
  self.d=Doubles()
 def make(self,spec=None,**kw):
  spec=spec or self.spec;j=Journal(self.root/'journal',spec);self.addCleanup(j.close)
  return Driver(j,spec,self.policy,**({'authority':self.d,'resources':self.d,'backend':self.d,'completion':self.d,'sink':self.d}|kw)),j
 def test_default_deny_no_backend(self):
  x,j=self.make(authority=None)
  with self.assertRaises(Denied):x.run(0,self.values[0])
  self.assertNotIn('prepare',self.d.log);self.assertTrue(j.retired())
 def test_continuation_same_driver_scope_order_and_full_values(self):
  x,j=self.make();a=x.run(0,self.values[0]);b=x.run(1,self.values[1])
  self.assertFalse(a.execution_allowed);self.assertFalse(b.device_completion_verified)
  self.assertEqual(self.d.log.count('reserve'),1);self.assertEqual(self.d.log.count('prepare'),2)
  self.assertLess(self.d.log.index('scope'),self.d.log.index('prepare'))
  self.assertLess(self.d.log.index('finish'),self.d.log.index('seal'));self.assertLess(self.d.log.index('install'),self.d.log.index('finalize'))
  self.assertEqual([struct.unpack('<3f',v) for _,_,v in self.d.blocks],[(0,1,2),(100000,100001,100002)])
  with self.assertRaises(Denied):x.run(1,self.values[1])
 def test_second_driver_and_journal_reopen_denied(self):
  x,j=self.make()
  with self.assertRaises(Denied):Driver(j,self.spec,self.policy)
  with self.assertRaises(FileExistsError):Journal(self.root/'journal',self.spec)
 def test_actual_input_and_wrong_token_fail_before_prepare(self):
  x,j=self.make()
  with self.assertRaises(Denied):x.run(1,self.values[0])
  self.assertNotIn('prepare',self.d.log)
 def test_nonfinite_input_even_matching_digest_denied(self):
  bad=struct.pack('<2I',0x7f800000,0)
  spec=dataclasses.replace(self.spec,inputs=(hashlib.sha256(bad).hexdigest(),))
  x,j=self.make(spec)
  with self.assertRaises(Denied):x.run(0,bad)
  self.assertNotIn('prepare',self.d.log)
 def test_policy_full_plan_and_independent_pools(self):
  spec=dataclasses.replace(self.spec,vocab=151936,inputs=(self.spec.inputs[0],)*3)
  need=self.policy.requirements(spec);self.assertEqual(need['source'],1856000);self.assertEqual(need['destination'],1856000)
  with self.assertRaises(Denied):dataclasses.replace(self.policy,host_capacity=1).requirements(spec)
  with self.assertRaises(Denied):dataclasses.replace(self.policy,workspace=0).requirements(spec)
  with self.assertRaises(Denied):dataclasses.replace(spec,inputs=(self.spec.inputs[0],)*4)
 def test_8193_vocab_exact_projection_tail(self):
  spec=dataclasses.replace(self.spec,vocab=8193,inputs=(self.spec.inputs[0],));x,j=self.make(spec);x.run(0,self.values[0])
  self.assertEqual([(c,len(b)) for _,c,b in self.d.blocks],[(0,32768),(8192,4)])
 def test_faults_retire_and_no_continuation_or_reprojection(self):
  for phase in ('prepare','project','materialize','append','finish','seal','install','auth_exit','finalize'):
   with tempfile.TemporaryDirectory() as tmp:
    old=self.root;self.root=Path(tmp);self.d=Doubles();self.d.fail=phase;x,j=self.make()
    with self.assertRaises(RuntimeError):x.run(0,self.values[0])
    count=self.d.log.count('prepare')
    with self.assertRaises(Denied):x.run(0,self.values[0])
    self.assertEqual(self.d.log.count('prepare'),count);self.assertTrue(j.retired());self.assertIn('quarantine',self.d.log)
    self.root=old
 def test_finalize_zero_double_and_committed_response_lost(self):
  for mode in ('zero','double','lost'):
   with tempfile.TemporaryDirectory() as tmp:
    old=self.root;self.root=Path(tmp);self.d=Doubles();self.d.final=mode;x,j=self.make()
    with self.assertRaises((Denied,RuntimeError)):x.run(0,self.values[0])
    self.assertTrue(j.retired())
    with self.assertRaises(Denied):x.run(1,self.values[1])
    self.assertEqual(self.d.log.count('prepare'),1);self.root=old
 def test_completion_and_sink_barriers_prevent_next_slice(self):
  for phase in ('materialize','append'):
   with tempfile.TemporaryDirectory() as tmp:
    old=self.root;self.root=Path(tmp);self.d=Doubles();entered=threading.Event();release=threading.Event();self.d.barrier=(phase,entered,release)
    spec=dataclasses.replace(self.spec,vocab=8193,inputs=(self.spec.inputs[0],));x,j=self.make(spec);errors=[]
    def run():
     try:x.run(0,self.values[0])
     except BaseException as e:errors.append(e)
    t=threading.Thread(target=run);t.start()
    try:
     self.assertTrue(entered.wait(5));self.assertEqual(self.d.log.count('project'),1);x.retire();release.set();t.join(5)
    finally:release.set();t.join(5)
    self.assertFalse(t.is_alive());self.assertTrue(errors);self.assertEqual(self.d.log.count('project'),1);self.assertTrue(j.retired());self.root=old
 def test_synthetic_permit_not_accepted_as_authority_context(self):
  from validation.private_producer.contract import Permit
  @contextmanager
  def forged(spec,token):yield Permit('owner','a'*64)
  self.d.hold_call=forged;x,j=self.make()
  with self.assertRaises(Denied):x.run(0,self.values[0])
  self.assertNotIn('prepare',self.d.log)

 def test_large_vocab_nineteen_projection_slices_no_frame_claim(self):
  spec=dataclasses.replace(self.spec,vocab=151936,inputs=(self.spec.inputs[0],));x,j=self.make(spec);x.run(0,self.values[0])
  self.assertEqual(len(self.d.blocks),19);self.assertEqual([len(b) for _,_,b in self.d.blocks],([32768]*18)+[17920])
 def test_suppressing_authority_does_not_hide_prepare_failure(self):
  @contextmanager
  def hold(spec,token):
   try:yield None
   except BaseException:pass
  self.d.hold_call=hold;self.d.fail='prepare';x,j=self.make()
  with self.assertRaises(RuntimeError):x.run(0,self.values[0])
  self.assertTrue(j.retired());self.assertIn('scope',self.d.log);self.assertIn('quarantine',self.d.log)
 def test_materialization_must_be_owned_finite_exact_bytes(self):
  self.d.materialize=lambda *args:memoryview(bytes(12));x,j=self.make()
  with self.assertRaises(Denied):x.run(0,self.values[0])
  self.assertNotIn('append',self.d.log);self.assertTrue(j.retired())
 def test_retirement_write_failure_retains_consumed_no_retry(self):
  from unittest.mock import patch
  self.d.fail='prepare';x,j=self.make()
  with patch.object(j,'retire',side_effect=OSError('retirement storage failed')):
   with self.assertRaises(BaseExceptionGroup):x.run(0,self.values[0])
  self.assertFalse(j.retired());self.assertIn('quarantine',self.d.log)
  with self.assertRaises(Denied):x.run(0,self.values[0])
  self.assertEqual(self.d.log.count('prepare'),1)
  with self.assertRaises(Denied):Driver(j,self.spec,self.policy)
 def test_quarantine_failure_keeps_stage_retired_and_no_release(self):
  x,j=self.make();self.d.fail='prepare'
  def quarantine(*args):raise RuntimeError('quarantine unknown')
  self.d.quarantine=quarantine
  with self.assertRaises(BaseExceptionGroup):x.run(0,self.values[0])
  self.assertTrue(j.retired());self.assertFalse(hasattr(x,'release'))
 def test_finalize_swallowed_commit_error_cannot_advance(self):
  from unittest.mock import patch
  def finalize(spec,token,digest,commit):
   try:commit()
   except BaseException:pass
  self.d.finalize=finalize;x,j=self.make()
  with patch.object(j,'commit',side_effect=OSError('commit failed')):
   with self.assertRaises(Denied):x.run(0,self.values[0])
  self.assertTrue(j.retired());self.assertIn('install',self.d.log)
 def test_shared_resource_double_denies_second_request_without_prepare(self):
  class Pool:
   def __init__(self):self.used=False;self.ticket=object()
   def reserve_request(self,spec,policy):
    if self.used:raise Denied('shared test pool exhausted')
    self.used=True;return self.ticket
   def verify_token(self,spec,ticket,token):
    if ticket is not self.ticket:raise Denied('wrong ticket')
   def quarantine(self,*args):pass  # keep charge; no refund
  pool=Pool();x,j=self.make(resources=pool);x.run(0,self.values[0]);x.run(1,self.values[1])
  spec=dataclasses.replace(self.spec,request='other');other=Journal(self.root/'other',spec);self.addCleanup(other.close)
  y=Driver(other,spec,self.policy,authority=self.d,resources=pool,backend=self.d,completion=self.d,sink=self.d)
  before=self.d.log.count('prepare')
  with self.assertRaises(Denied):y.run(0,self.values[0])
  self.assertEqual(self.d.log.count('prepare'),before);self.assertTrue(pool.used)

 def test_foreign_thread_commit_is_not_a_valid_finalize(self):
  errors=[]
  def finalize(spec,token,digest,commit):
   def other():
    try:commit()
    except BaseException as e:errors.append(e)
   t=threading.Thread(target=other);t.start();t.join(5);self.assertFalse(t.is_alive())
  self.d.finalize=finalize;x,j=self.make()
  with self.assertRaises(Denied):x.run(0,self.values[0])
  self.assertTrue(errors);self.assertTrue(j.retired())
 def test_escaped_commit_after_success_cannot_reenter(self):
  captured=[]
  def finalize(spec,token,digest,commit):captured.append(commit);commit()
  self.d.finalize=finalize;x,j=self.make();x.run(0,self.values[0])
  with self.assertRaises(Denied):captured[0]()
  self.assertTrue(j.retired())
 def test_awaitable_project_result_rejected_before_materialization(self):
  async def pending():return 'not a device event'
  self.d.project_slice=lambda *args:pending();x,j=self.make()
  with self.assertRaises(Denied):x.run(0,self.values[0])
  self.assertNotIn('materialize',self.d.log);self.assertTrue(j.retired())

 def test_reserve_response_lost_quarantines_without_ticket(self):
  quarantines=[]
  def reserve(spec,policy):self.d.log.append('charged');raise RuntimeError('ticket lost')
  self.d.reserve_request=reserve;self.d.quarantine=lambda *args:quarantines.append(args)
  x,j=self.make()
  with self.assertRaises(RuntimeError):x.run(0,self.values[0])
  self.assertEqual(quarantines,[(self.spec,None,None)]);self.assertNotIn('prepare',self.d.log);self.assertTrue(j.retired())
 def test_open_scope_failure_is_quarantined_without_prepare_handle(self):
  self.d.fail='scope';quarantines=[];self.d.quarantine=lambda *args:quarantines.append(args)
  x,j=self.make()
  with self.assertRaises(RuntimeError):x.run(0,self.values[0])
  self.assertTrue(any(len(a)==1 and type(a[0])is str for a in quarantines));self.assertNotIn('prepare',self.d.log)
 def test_next_token_on_foreign_thread_retires_stage(self):
  x,j=self.make();x.run(0,self.values[0]);errors=[]
  def run():
   try:x.run(1,self.values[1])
   except Denied:errors.append('denied')
  t=threading.Thread(target=run);t.start();t.join(5);self.assertFalse(t.is_alive())
  self.assertEqual(errors,['denied']);self.assertEqual(self.d.log.count('prepare'),1);self.assertTrue(j.retired())
 def test_busy_call_retires_started_prepare_without_second_call(self):
  entered=threading.Event();release=threading.Event();self.d.barrier=('prepare',entered,release);x,j=self.make();errors=[]
  def run():
   try:x.run(0,self.values[0])
   except Denied:errors.append('retired')
  t=threading.Thread(target=run);t.start()
  try:
   self.assertTrue(entered.wait(5))
   with self.assertRaises(Denied):x.run(0,self.values[0])
  finally:release.set();t.join(5)
  self.assertFalse(t.is_alive());self.assertEqual(self.d.log.count('prepare'),1);self.assertNotIn('project',self.d.log);self.assertEqual(errors,['retired'])

if __name__=='__main__':unittest.main()
