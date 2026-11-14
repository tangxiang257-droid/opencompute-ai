"""Existing configured ActionServer uses concrete original-channel identity."""
import unittest
from validation.config_assembly.test_config import Configured

class Flow(Configured):
 async def bind_server(self,stage,control,ch,**changes):
  changes.update(identity=None,identity_mode='channel')
  service=await self.configured(stage,control,ch,**changes);self.owned.append(service)
  from cluster_p1.scoped_identity import ScopedChannelIdentity
  self.assertIs(type(service.identity),ScopedChannelIdentity);self.assertIs(service.server.identity,service.identity)
  self.concrete=getattr(self,'concrete',[])+[stage];return service
 async def test_concrete_identity_A_B_C(self):
  await self.run_chain();self.assertEqual(self.concrete,[0,1])
 async def test_explicit_mode_no_provider_fallback(self):
  _,control,ch=await self.prepare_a(bind=False)
  for change in ({'identity':None},{'identity_mode':'unknown'},{'identity_mode':'channel'},{'identity':None,'identity_mode':'channel','resources':None}):
   with self.assertRaises(Exception):await self.configured(0,control,ch,**change)
  self.assertFalse(getattr(ch,'_experiment_action_owned',False));self.assertFalse(ch.w.is_closing());self.assertEqual(self.calls,[[],[]])
 async def test_clock_callback_cannot_replace_resource_hold(self):
  _,control,ch=await self.prepare_a(bind=False);old=self.clock.sample;injected=[]
  def sample():
   injected.append(1);self.central_resources.hold=lambda *a:None;return old()
  self.clock.sample=sample
  with self.assertRaises(Exception):await self.configured(0,control,ch,identity=None,identity_mode='channel')
  self.assertTrue(injected);self.assertFalse(getattr(ch,'_experiment_action_owned',False));self.assertEqual(self.calls,[[],[]])
 async def test_task_revoke_still_blocks_body(self):
  _,control=await self.prepare_a();await self.adm.revoke(self.token,self.task.task_id)
  self.assertEqual((await control.dispatch())['state'],'UNKNOWN');self.assertEqual(self.calls,[[],[]]);self.assertEqual(self.action_rows(),[(0,'control-bound')])
  await self.settle_expected_role_failure()
 async def test_resource_denial_still_blocks_body(self):
  _,control=await self.prepare_a();self.central_resources.fail=True
  self.assertEqual((await control.dispatch())['state'],'UNKNOWN');self.assertEqual(self.calls,[[],[]]);self.assertEqual(self.action_rows(),[(0,'control-bound')])
  await self.settle_expected_role_failure()

def load_tests(loader,tests,pattern):return unittest.TestSuite(Flow(n) for n in Flow.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
