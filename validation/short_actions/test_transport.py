"""Real control transport. This first regression denies an unattached gate."""
import unittest
from validation.control_exchange.test_transport import Tests as Base
class Tests(Base):
 async def test_gate_removal_cannot_run_body(self):
  s=self.services[0];s._action_gate_pin=object();s._action_gate=None;calls=[]
  with self.assertRaises(Exception):await self.pair.dispatch(0,self.spec,lambda:calls.append(1))
  self.assertEqual(calls,[])
def load_tests(loader,tests,pattern):return unittest.TestSuite(Tests(n) for n in Tests.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
