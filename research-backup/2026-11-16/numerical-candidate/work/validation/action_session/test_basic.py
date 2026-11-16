from validation.action_session.test_pipeline import Flow
from validation.hidden_host.test_pipeline import Flow as Hidden
import unittest
class Basic(Flow):
 async def execute_a(self):return await Hidden.execute_a(self)
 async def test_existing_chain_with_explicit_action_sessions(self):
  await self.run_chain()
  self.assertEqual(sum(x[0]=='stage0' for x in self.calls[0]),1)
  self.assertEqual(sum(x[0]=='stage1' for x in self.calls[1]),1)
  for host in self.control_hosts:
   self.assertEqual(host.action_count,2)
   self.assertTrue(host.assembly.server._action_session.active)
def load_tests(*args):return unittest.TestSuite([Basic('test_existing_chain_with_explicit_action_sessions')])
if __name__=='__main__':unittest.main()
