import unittest
from validation.hidden_host.test_pipeline import Flow
class SenderPin(Flow):
 async def test_committed_sender_cannot_be_replaced(self):
  _,control=await self.prepare_a();self.assertEqual((await control.dispatch())['state'],'COMMITTED')
  calls=[]
  class Fake:
   async def send(self,raw):calls.append(raw);return {}
  self.a_session.sender=Fake()
  with self.assertRaises(Exception):await self.a_session.send_committed(enabled=True)
  self.assertEqual(calls,[])
def load_tests(*args):return unittest.TestSuite([SenderPin('test_committed_sender_cannot_be_replaced')])
if __name__=='__main__':unittest.main()
