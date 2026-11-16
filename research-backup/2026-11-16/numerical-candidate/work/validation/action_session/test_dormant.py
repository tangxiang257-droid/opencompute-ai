"""Native engine scheduling only; providers/SSL are explicit ordinary stand-ins."""
import asyncio,struct,unittest
from validation.data_session.test_core import Tests
from cluster_p1.data_session import DataSession,DataSessionPolicy
class Dormant(Tests):
 def dormant(self):
  s=DataSession._attach(self.a,self.binding,DataSessionPolicy(.02,.1,.2),identity=self.p,authorize=self.p,check=lambda:None,purpose='action-session',dormant=True)
  self.sessions.append(s);return s
 async def test_early_ping_is_bounded_not_liveness(self):
  s=self.dormant();deadline=s.deadline
  s._accept(struct.pack('!BBQ32s32s',1,1,1,s.digest,b'x'*32))
  barrier=asyncio.Event();asyncio.get_running_loop().call_soon(barrier.set);await barrier.wait()
  self.assertFalse(s.active);self.assertEqual(s.deadline,deadline);self.assertFalse(s.tasks)
  self.assertEqual(s.ping,(1,b'x'*32));self.assertFalse(self.a.w.frames)
  with self.assertRaises(Exception):s._accept(struct.pack('!BBQ32s32s',1,1,2,s.digest,b'y'*32))
 async def test_dormant_deadline_is_terminal(self):
  s=self.dormant()
  with self.assertRaises(Exception):await s.ready()
  self.assertEqual(s.phase,'CLOSED');self.assertTrue(self.a._closed)
def load_tests(*args):return unittest.TestSuite(Dormant(n) for n in Dormant.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
