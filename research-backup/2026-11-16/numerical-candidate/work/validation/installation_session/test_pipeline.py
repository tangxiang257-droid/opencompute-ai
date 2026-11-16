"""Same actual A/B/C pipeline with explicit original-install session policy; LAB."""
import unittest
from validation.control_bootstrap.test_topology import Topology
from cluster_p1.installation_session import SessionPolicy
class Pipeline(Topology):
 async def install_stages(self):
  self.installation_session_policy=SessionPolicy()
  await super().install_stages()
 async def test_actual_chain_uses_active_original_sessions(self):
  await self.run_chain()
  for stage in (0,1):
   self.assertTrue(self.launchers[stage].startup.client._session.active)
   self.assertTrue(self.adm._installations[(self.receipt.proposal_id,stage)]['service']._session.active)
 async def test_current_consent_revocation_still_blocks_A(self):
  _,control=await self.prepare_a();await self.adm.revoke(self.token,self.task.task_id)
  try:receipt=await control.dispatch()
  except Exception:receipt=None
  if receipt is not None:
   self.assertEqual(receipt['state'],'UNKNOWN');self.assertTrue(receipt['retired'])
  self.assertEqual(self.action_rows(),[(0,'control-bound')])
  self.assertEqual(self.calls,[[],[]])
def load_tests(loader,tests,pattern):return unittest.TestSuite(Pipeline(n) for n in Pipeline.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
