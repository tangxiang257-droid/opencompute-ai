import asyncio,unittest
from unittest.mock import patch
from validation.hidden_host.test_pipeline import Flow
from private_b_hidden_host import BHiddenHost
class Review(Flow):
 async def serve_installation_endpoint(self,service,launch,args,policy):
  if launch.config.data['role']=='A':return await super().serve_installation_endpoint(service,launch,args,policy)
  original=BHiddenHost.install;self.close_job=None;self.barrier_entered=False
  async def install(host,**kwargs):
   async def barrier():
    self.barrier_entered=True
    self.assertIsNotNone(host.prepared.journal)
    self.assertFalse(host.prepared.channel._receipt_h_started)
    self.assertTrue(all(f[:1] not in (b'H',b'C',b'D') for f in self.hidden_raw[1].frames))
    self.close_job=asyncio.create_task(host.close() if self.target=='host' else host.launcher.close())
    await asyncio.Event().wait()
   return await original(host,**dict(kwargs,prepare_fence=barrier))
  with patch.object(BHiddenHost,'install',install):
   with self.assertRaises(asyncio.CancelledError):await super().serve_installation_endpoint(service,launch,args,policy)
  self.assertTrue(self.barrier_entered)
  await asyncio.wait_for(asyncio.shield(self.close_job),3)
  self.assertFalse(self.hidden_host.prepared.channel._receipt_h_started)
  self.assertTrue(all(f[:1] not in (b'H',b'C',b'D') for f in self.hidden_raw[1].frames))
 async def asyncSetUp(self):
  self.target='host' if self._testMethodName.endswith('host') else 'launcher'
  await super().asyncSetUp()
 async def test_pre_H_external_host(self):
  self.assertTrue(self.hidden_host.launcher._closing)
  self.assertEqual(self.hidden_host.state,'UNKNOWN')
  self.assertTrue(self.hidden_raw[1].closed)
  self.assertEqual(self.calls,[[],[]])
 async def test_pre_H_external_launcher(self):
  self.assertTrue(self.hidden_host.launcher._closing)
  self.assertEqual(self.hidden_host.state,'UNKNOWN')
  self.assertTrue(self.hidden_raw[1].closed)
  self.assertEqual(self.calls,[[],[]])
def load_tests(*a):return unittest.TestSuite(Review(n) for n in Review.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
