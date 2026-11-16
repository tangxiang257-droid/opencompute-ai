from validation.data_session_wiring.test_pipeline import Waited
from cluster_p1.data_session import DataSessionPolicy
import unittest
class Flow(Waited):
 async def host_inputs(self,stage,spec):
  result=await super().host_inputs(stage,spec)
  self.host_kw['action_session_policy']=DataSessionPolicy()
  self.dial_kw['action_session_policy']=DataSessionPolicy()
  return result
def load_tests(*args):return unittest.TestSuite([Flow('test_result_after_verified_pulses_beyond_original_idle_limit')])
if __name__=='__main__':unittest.main()
