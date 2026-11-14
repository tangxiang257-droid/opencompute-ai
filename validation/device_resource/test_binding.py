"""Real local SQLite and resource view; synthetic authority/capacity only."""
import unittest,json,threading
from contextlib import contextmanager
from dataclasses import replace
from validation.authority_resources import test_resources as fixture
from cluster_p1.role_resources import Ticket
class Tests(unittest.IsolatedAsyncioTestCase):
 asyncSetUp=fixture.Tests.asyncSetUp
 cleanup=fixture.Tests.cleanup
 make=fixture.Tests.make
 reserve=fixture.Tests.reserve
 binding=fixture.Tests.binding
 def setup_view(self,physical=True):
  r=self.make();self.assertTrue(hasattr(r,'device_capacity'),'missing same-promise device view')
  t=self.reserve();r.bind_control(t.reservation_id,'control',self.binding(),64)
  self.calls=[]
  @contextmanager
  def physical_hold(d,demands,action):self.calls.append((d,demands,action));yield None
  if physical:self.c.hold_device=physical_hold
  self.t=t;self.view=r.device_capacity(t);return r
 async def test_same_record_no_new_reservation(self):
  r=self.setup_view();before=r.db.execute('SELECT * FROM promises').fetchall()
  for action in ('open','load','execute','materialize','finish','inspect-finished'):
   with self.view.hold_device(self.spec,self.descriptor['device'],0,action):pass
  self.assertEqual(before,r.db.execute('SELECT * FROM promises').fetchall());self.assertEqual(r.db.execute('SELECT count(*) FROM controls').fetchone(),(1,));self.assertEqual(len(self.calls),6)
  self.assertTrue(all(x[0]['resource_id']==self.t.reservation_id for x in self.calls));self.assertFalse(r.query(self.t.reservation_id)['physical_capacity_verified'])
 async def test_missing_real_capacity_hook_denies_unknown(self):
  r=self.setup_view(False)
  with self.assertRaises(Exception):
   with self.view.hold_device(self.spec,self.descriptor['device'],0,'open'):pass
  self.assertEqual(r.query(self.t.reservation_id)['state'],'UNKNOWN');self.assertEqual(r.query(self.t.reservation_id)['demand'],self.demand)
 async def test_copied_ticket_and_query_not_qualification(self):
  r=self.setup_view()
  for value in (Ticket(self.t.reservation_id),r.query(self.t.reservation_id),self.t.reservation_id):
   with self.assertRaises(Exception):r.device_capacity(value)
 async def test_wrong_device_poison_same_reservation(self):
  r=self.setup_view()
  with self.assertRaises(Exception):
   with self.view.hold_device(self.spec,'cuda:99',0,'load'):pass
  self.assertEqual(self.calls,[]);self.assertEqual(r.query(self.t.reservation_id)['state'],'UNKNOWN')
 async def test_body_failure_retained(self):
  r=self.setup_view()
  with self.assertRaises(RuntimeError):
   with self.view.hold_device(self.spec,self.descriptor['device'],0,'execute'):raise RuntimeError('device unknown')
  self.assertEqual(r.query(self.t.reservation_id)['state'],'UNKNOWN')
 async def test_no_control_mapping_rejects(self):
  r=self.make();self.assertTrue(hasattr(r,'device_capacity'));t=self.reserve();v=r.device_capacity(t)
  with self.assertRaises(Exception):
   with v.hold_device(self.spec,self.descriptor['device'],0,'open'):pass
  self.assertEqual(r.query(t.reservation_id)['state'],'UNKNOWN')
 async def test_external_fence_without_local_lock(self):
  r=self.setup_view();entered=threading.Event();release=threading.Event();errors=[]
  @contextmanager
  def physical(*args):entered.set();assert release.wait(5);yield None
  self.c.hold_device=physical
  def run():
   try:
    with self.view.hold_device(self.spec,self.descriptor['device'],0,'execute'):pass
   except BaseException as e:errors.append(e)
  t=threading.Thread(target=run);t.start();self.assertTrue(entered.wait(5))
  r.quarantine(self.spec,self.t,None);release.set();t.join(5);self.assertFalse(t.is_alive());self.assertEqual(len(errors),1);self.assertEqual(r.query(self.t.reservation_id)['state'],'UNKNOWN')
 async def test_device_completion_uses_same_existing_ticket(self):
  from validation.device_completion.test_provider import Runtime,Gates
  from cluster_p1.device_completion import DeviceCompletion
  from types import SimpleNamespace
  self.descriptor['device']='cuda:0';r=self.setup_view();runtime=Runtime()
  p=DeviceCompletion(self.spec,stage=0,device='cuda:0',torch_version='synthetic',authority=Gates(),capacity=self.view,_runtime=runtime)
  p.open_scope(self.spec,0,'scope')
  with p.hold_load(self.spec,0):pass
  owner=SimpleNamespace(stage_id=0,_device='cuda:0');p.require_open('scope',self.spec,owner)
  pending=SimpleNamespace(scope='scope',owner=owner,count=24,tensor=[1.]*24)
  self.assertEqual(len(p.materialize('scope',pending,96)),96);p.finish_scope('scope');p.require_finished('scope',owner)
  self.assertEqual(p.state,'FINISHED');self.assertEqual(r.db.execute('SELECT count(*) FROM promises').fetchone(),(1,))
 async def test_exit_error_retains_original_charge(self):
  r=self.setup_view()
  @contextmanager
  def physical(*args):
   yield None
   raise PermissionError('lost lease reply')
  self.c.hold_device=physical
  with self.assertRaises(PermissionError):
   with self.view.hold_device(self.spec,self.descriptor['device'],0,'execute'):pass
  self.assertEqual(r.query(self.t.reservation_id)['state'],'UNKNOWN');self.assertEqual(r.query(self.t.reservation_id)['charge'],64)
 async def test_control_replacement_on_exit_rejected(self):
  r=self.setup_view()
  @contextmanager
  def physical(*args):
   yield None
   r.db.execute("UPDATE controls SET rid='replacement'")
  self.c.hold_device=physical
  with self.assertRaises(Exception):
   with self.view.hold_device(self.spec,self.descriptor['device'],0,'execute'):pass
  self.assertEqual(r.query(self.t.reservation_id)['state'],'UNKNOWN')
 async def test_wrong_actual_input_no_physical_call(self):
  r=self.setup_view()
  with self.assertRaises(Exception):
   with self.view.hold_device(replace(self.spec,inputs=('f'*64,)),self.descriptor['device'],0,'execute'):pass
  self.assertEqual(self.calls,[])
 async def test_missing_authority_after_binding_poison(self):
  r=self.setup_view();r.authority=None
  with self.assertRaises(Exception):
   with self.view.hold_device(self.spec,self.descriptor['device'],0,'open'):pass
  self.assertEqual(r.query(self.t.reservation_id)['state'],'UNKNOWN')
 async def test_B_uses_only_once_bound_actual_input(self):
  from cluster_p1.role_resources import RoleResources
  r=self.make();self.assertTrue(hasattr(r,'device_capacity'))
  d=self.authority.describe(self.registration,1,node='b',device='cuda:1',instance='instance-b',resource_id='promise-b')
  r.close();r=RoleResources(self.root/'b.db',node='b',pool_limits=self.demand,extra_host_bytes=32768,authority=self.a,capacity=self.c);self.addCleanup(r.close);self.r=r
  t=self.reserve(d);r.bind_control(t.reservation_id,'control-b',self.binding(d,input_digest='f'*64),64)
  calls=[]
  @contextmanager
  def physical(*args):calls.append(args);yield None
  self.c.hold_device=physical;v=r.device_capacity(t)
  with v.hold_device(replace(self.spec,inputs=('f'*64,)),'cuda:1',1,'open'):pass
  self.assertEqual(len(calls),1)
  with self.assertRaises(Exception):
   with v.hold_device(self.spec,'cuda:1',1,'open'):pass
  self.assertEqual(len(calls),1);self.assertEqual(r.query(t.reservation_id)['state'],'UNKNOWN')
 async def test_reopen_never_reissues_view(self):
  from cluster_p1.role_resources import RoleResources
  r=self.setup_view();r.close()
  reopened=RoleResources(r.path,node='a',pool_limits=self.demand,extra_host_bytes=32768,authority=self.a,capacity=self.c);self.addCleanup(reopened.close)
  with self.assertRaises(Exception):reopened.device_capacity(self.t)
  with self.assertRaises(Exception):
   with self.view.hold_device(self.spec,self.descriptor['device'],0,'open'):pass
  self.assertEqual(reopened.query(self.t.reservation_id)['charge'],64)
 async def test_preexisting_control_mismatch_rejected(self):
  r=self.setup_view();b=self.binding(owner='different-owner')
  r.db.execute('UPDATE controls SET binding=?',(json.dumps(b,sort_keys=True,separators=(',',':')),))
  with self.assertRaises(Exception):
   with self.view.hold_device(self.spec,self.descriptor['device'],0,'open'):pass
  self.assertEqual(self.calls,[])
 async def test_pid_failure_cannot_restore_old_view(self):
  from unittest.mock import patch
  r=self.setup_view()
  with patch.object(r,'pid',-1):
   with self.assertRaises(BaseExceptionGroup):
    with self.view.hold_device(self.spec,self.descriptor['device'],0,'open'):pass
  self.assertIn(self.t.reservation_id,r.poisoned)
  with self.assertRaises(Exception):
   with self.view.hold_device(self.spec,self.descriptor['device'],0,'open'):pass
 async def test_unknown_write_failure_keeps_local_poison(self):
  from unittest.mock import patch
  r=self.setup_view()
  with patch.object(r,'_commit',side_effect=OSError('disk failed')):
   with self.assertRaises(BaseExceptionGroup):
    with self.view.hold_device(self.spec,self.descriptor['device'],0,'execute'):raise RuntimeError('failure')
  self.assertIn(self.t.reservation_id,r.poisoned)
  with self.assertRaises(Exception):
   with self.view.hold_device(self.spec,self.descriptor['device'],0,'execute'):pass
if __name__=='__main__':unittest.main()
