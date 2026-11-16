import unittest
from contextlib import contextmanager
from unittest.mock import patch
from validation.hidden_host.test_pipeline import Flow
class Permissions(Flow):
 async def test_pulse_does_not_check_execution_readiness(self):
  for owner,session in ((self.a_session,self.a_session.session),(self.hidden_host,self.hidden_host.data_session)):
   with patch.object(owner.launcher,'check',side_effect=AssertionError('execution readiness is separate from liveness')):
    with session.fence():pass
 async def test_pulse_requires_original_task_admission(self):
  @contextmanager
  def revoked(*args):raise PermissionError('LAB original task permission revoked');yield
  for owner,session in ((self.a_session,self.a_session.session),(self.hidden_host,self.hidden_host.data_session)):
   with patch.object(owner.startup.client,'admission',revoked):
    with self.assertRaises(PermissionError):
     with session.fence():pass
def load_tests(*args):return unittest.TestSuite(Permissions(n) for n in Permissions.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
