"""Fake CUDA API only: no torch import, model or physical completion proof."""
import unittest,threading,struct
from contextlib import contextmanager
from types import SimpleNamespace
try:
 from cluster_p1.device_completion import DeviceCompletion
except ImportError:DeviceCompletion=None

class Gates:
 def __init__(self):self.allowed=True;self.calls=[]
 @contextmanager
 def hold_device(self,spec,device,stage,action):
  self.calls.append(action)
  if not self.allowed:raise PermissionError('revoked')
  yield None
  if not self.allowed:raise PermissionError('revoked')
class Runtime:
 def __init__(self):self.events=[];self.stream=1;self.fail=None;self.enter=None;self.release=None
 def check(self,device,version):self.events.append('check')
 def current_stream(self,device):return self.stream
 def synchronize(self,device):
  self.events.append('sync')
  if self.enter:self.enter.set();assert self.release.wait(5)
  if self.fail=='sync':raise RuntimeError('async error')
 def copy(self,tensor,device,count):
  self.events.append('copy')
  if self.fail=='copy':raise RuntimeError('copy error')
  return list(tensor)
 def encode(self,value,count):self.events.append('encode');return struct.pack('<'+'f'*count,*value)
class Tests(unittest.TestCase):
 def setUp(self):self.assertIsNotNone(DeviceCompletion,'missing bounded device provider')
 def make(self,stage=1,bind=True):
  self.r=Runtime();self.g=Gates();self.spec=__import__('cluster_p1.projection_driver',fromlist=['Spec']).Spec('owner','request','session',1,*(['a'*64]*6),2,8193,('a'*64,))
  self.p=DeviceCompletion(self.spec,stage=stage,device='cuda:0',torch_version='2.5.1',authority=self.g,capacity=self.g,_runtime=self.r)
  self.p.open_scope(self.spec,0,'scope')
  with self.p.hold_load(self.spec,stage):pass
  self.owner=SimpleNamespace(stage_id=stage,_device='cuda:0')
  if bind:self.p.require_open('scope',self.spec,self.owner)
  return self.p
 def pending(self,n=2):return SimpleNamespace(scope='scope',owner=self.owner,count=n,tensor=[float(x) for x in range(n)])
 def test_default_denies_before_runtime(self):
  self.make();p=DeviceCompletion(self.spec,stage=1,device='cuda:0',torch_version='2.5.1')
  with self.assertRaises(Exception):p.open_scope(self.spec,0,'x')
 def test_load_copy_finish_order(self):
  p=self.make();self.assertEqual(self.r.events,['check','sync','sync'])
  self.assertEqual(p.materialize('scope',self.pending(),8),struct.pack('<ff',0,1))
  self.assertEqual(self.r.events[-4:],['sync','copy','sync','encode'])
  p.finish_scope('scope');p.require_finished('scope',self.owner)
  self.assertEqual(p.state,'FINISHED')
 def test_finish_waits_event(self):
  p=self.make(bind=False);entered=threading.Event();release=threading.Event();self.r.enter=entered;self.r.release=release
  result=[]
  # Same worker opens and finishes; creation/load may occur on another thread (A).
  p=self.p
  def work():
   p.require_open('scope',self.spec,self.owner);p.finish_scope('scope');result.append('done')
  t=threading.Thread(target=work);t.start();self.assertTrue(entered.wait(5));self.assertEqual(result,[]);self.assertNotEqual(p.state,'FINISHED');release.set();t.join(5);self.assertFalse(t.is_alive());self.assertEqual(result,['done'])
 def test_async_error_retires_and_retains(self):
  p=self.make();v=self.pending();self.r.fail='sync'
  with self.assertRaises(RuntimeError):p.materialize('scope',v,8)
  self.assertEqual(p.state,'UNKNOWN');self.assertIs(p.pending,v)
  with self.assertRaises(Exception):p.finish_scope('scope')
 def test_copy_error_retains(self):
  p=self.make();v=self.pending();self.r.fail='copy'
  with self.assertRaises(RuntimeError):p.materialize('scope',v,8)
  self.assertIs(p.pending,v);self.assertEqual(p.state,'UNKNOWN')
 def test_revoke_denies(self):
  p=self.make();self.g.allowed=False
  with self.assertRaises(Exception):p.materialize('scope',self.pending(),8)
  self.assertEqual(p.state,'UNKNOWN')
 def test_wrong_scope_owner_size(self):
  for field,val in [('scope','other'),('owner',object()),('count',8193)]:
   p=self.make();v=self.pending();setattr(v,field,val)
   with self.assertRaises(Exception):p.materialize('scope',v,v.count*4)
   self.assertEqual(p.state,'UNKNOWN')
 def test_stream_drift_rejects(self):
  p=self.make();self.r.stream=2
  with self.assertRaises(Exception):p.materialize('scope',self.pending(),8)
 def test_no_second_load_or_scope(self):
  p=self.make()
  with self.assertRaises(Exception):
   with p.hold_load(self.spec,1):pass
  with self.assertRaises(Exception):p.open_scope(self.spec,0,'new')
 def test_quarantine_never_reports_finished(self):
  p=self.make();p.quarantine('scope');self.assertEqual(p.state,'UNKNOWN')
  with self.assertRaises(Exception):p.require_finished('scope',self.owner)
 def test_load_exception_retired(self):
  self.make();p=DeviceCompletion(self.spec,stage=1,device='cuda:0',torch_version='2.5.1',authority=self.g,capacity=self.g,_runtime=self.r);p.open_scope(self.spec,0,'scope')
  with self.assertRaises(RuntimeError):
   with p.hold_load(self.spec,1):raise RuntimeError('partial load')
  self.assertEqual(p.state,'UNKNOWN')
 def test_stage0_limit(self):
  p=self.make(0);v=self.pending(16385)
  with self.assertRaises(Exception):p.materialize('scope',v,65540)
 def test_post_copy_sync_failure_retains_both(self):
  p=self.make();v=self.pending();original=self.r.synchronize;calls=[]
  def sync(device):
   calls.append(device)
   if len(calls)==2:raise RuntimeError('D2H async')
   original(device)
  self.r.synchronize=sync
  with self.assertRaises(RuntimeError):p.materialize('scope',v,8)
  self.assertIs(p.pending,v);self.assertIsNotNone(p.host);self.assertNotIn('encode',self.r.events)
 def test_fence_exit_failure_retains_both(self):
  p=self.make();v=self.pending()
  @contextmanager
  def hold(*args):
   yield None
   raise PermissionError('lost lease')
  self.g.hold_device=hold
  with self.assertRaises(PermissionError):p.materialize('scope',v,8)
  self.assertIs(p.pending,v);self.assertIsNotNone(p.host);self.assertEqual(p.state,'UNKNOWN')
 def test_quarantine_during_sync_returns_and_late_finish_denies(self):
  p=self.make(bind=False);entered=threading.Event();release=threading.Event();self.r.enter=entered;self.r.release=release;errors=[]
  def work():
   try:p.require_open('scope',self.spec,self.owner);p.finish_scope('scope')
   except BaseException as e:errors.append(e)
  t=threading.Thread(target=work);t.start();self.assertTrue(entered.wait(5))
  # Deliberately do not release the fence until quarantine returns.
  p.quarantine('scope');self.assertEqual(p.state,'UNKNOWN');release.set();t.join(5)
  self.assertFalse(t.is_alive());self.assertEqual(len(errors),1);self.assertEqual(p.state,'UNKNOWN')
 def test_thread_exit_is_not_completion(self):
  p=self.make(bind=False)
  t=threading.Thread(target=lambda:p.require_open('scope',self.spec,self.owner));t.start();t.join(5)
  with self.assertRaises(Exception):p.require_finished('scope',self.owner)
 def test_load_and_cleanup_errors_aggregated(self):
  self.make();p=DeviceCompletion(self.spec,stage=1,device='cuda:0',torch_version='2.5.1',authority=self.g,capacity=self.g,_runtime=self.r);p.open_scope(self.spec,0,'scope')
  with self.assertRaises(BaseExceptionGroup) as caught:
   with p.hold_load(self.spec,1):
    self.r.fail='sync';raise ValueError('load')
  self.assertEqual([type(e) for e in caught.exception.exceptions],[ValueError,RuntimeError]);self.assertEqual(p.state,'UNKNOWN')
if __name__=='__main__':unittest.main()
