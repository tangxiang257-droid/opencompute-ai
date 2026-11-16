"""Regression: existing action owner must not return a revoked delivery."""
import unittest,asyncio
from cluster_p1.channel import Channel,Delivery
from cluster_p1.data_session import DataSession,DataSessionPolicy
from validation.role_bridge.test_transport import streams,config
from validation.data_session.test_core import Provider
class Existing(unittest.IsolatedAsyncioTestCase):
 async def test_existing_postpop_revocation(self):
  r,w,_,_=streams('a','b');c=Channel(r,w,config('b'),{},timeout=.1);p=Provider()
  try:
   s=DataSession._attach(c,{'test':'original-action'},DataSessionPolicy(.01,.1,.3),identity=p,authorize=p,check=lambda:None,purpose='action-session',dormant=True);s.phase='ACTIVE'
   d=Delivery(c,object(),4);c._queue.put_nowait(d);c._used_bytes=4;c._used_tiles=1;pop=c._queue.get_nowait
   def revoke():
    actual=pop();s._fail(RuntimeError('revoked after pop'));return actual
   c._queue.get_nowait=revoke
   with self.assertRaises(Exception):await c.receive(_session_owner=s)
   self.assertEqual(c._undelivered,[d]);self.assertEqual(c._used_bytes,4);self.assertFalse(d._released)
  finally:await c.close()
if __name__=='__main__':unittest.main()
