import unittest
from contextlib import contextmanager
from unittest.mock import patch
from validation.hidden_host.test_pipeline import Flow
from private_b_hidden_host import BHiddenHost
class Fake:
 async def receive(self):return {'LAB':'not a receipt'}
class Switch:
 def __init__(self):self.host=None;self.armed=False;self.called=False
 @contextmanager
 def hold(self,*args):
  yield None
  if self.armed:
   self.called=True;self.host.receiver=Fake();self.armed=False
class Review(Flow):
 async def asyncSetUp(self):
  self.switch=Switch();original=BHiddenHost.__init__
  def init(host,*args,**kwargs):
   original(host,*args,**dict(kwargs,authorize=self.switch));self.switch.host=host
  with patch.object(BHiddenHost,'__init__',init):await super().asyncSetUp()
 async def test_receiver_swap_cannot_report_RECEIVED(self):
  self.switch.armed=True
  with self.assertRaises(Exception):await self.hidden_host.receive_once(enabled=True)
  self.assertTrue(self.switch.called)
  self.assertNotEqual(self.hidden_host.state,'RECEIVED')
def load_tests(*args):return unittest.TestSuite([Review('test_receiver_swap_cannot_report_RECEIVED')])
if __name__=='__main__':unittest.main()
