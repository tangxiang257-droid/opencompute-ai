import asyncio,unittest
from private_control_action_host import _Endpoint
from cluster_p1.channel import Channel,Grant
from validation.role_bridge.test_transport import streams,config
class Test(unittest.IsolatedAsyncioTestCase):
 async def test_wait_closed_failure_must_stay_unknown(self):
  a,w,b,z=streams('controller','a');marker=OSError('LAB close acknowledgement failed')
  async def failed():raise marker
  w.wait_closed=failed
  slot=_Endpoint(asyncio.get_running_loop(),dict(close_seconds=2))
  slot.writer=w;slot.channel=Channel(a,w,config('a'),{'a':set()})
  try:
   with self.assertRaises(BaseException):await slot.close()
   self.assertEqual(slot.state,'UNKNOWN')
  finally:z.close()
if __name__=='__main__':unittest.main()
