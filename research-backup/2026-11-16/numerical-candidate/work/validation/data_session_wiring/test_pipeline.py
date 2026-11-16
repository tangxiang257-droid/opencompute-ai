"""Real authority/receipt/result path, injected native heartbeat streams."""
from validation.hidden_host.test_pipeline import *
from validation.hidden_host.test_pipeline import Flow as Base

class Waited(Base):
 async def execute_a(self):
  actual=await super().execute_a()
  a=self.hidden_send;b=self.hidden_host.prepared.channel
  before=[(c._credits_bytes,c._credits_tiles,c._used_bytes,c._used_tiles,c._peer_capacity) for c in (a,b)]
  frames=[sum(f[:1] in (b'H',b'C',b'D') for f in c.w.frames) for c in (a,b)]
  begin=asyncio.get_running_loop().time();pulse=asyncio.Event();original=b.w.write;seen=[]
  def observed(raw):
   original(raw)
   if raw[:1]==b'P':
    seen.append(asyncio.get_running_loop().time()-begin)
    if seen[-1]>b.timeout:pulse.set()
  b.w.write=observed
  try:
   await asyncio.wait_for(pulse.wait(),b.timeout+8)
   self.assertFalse(a._task.done());self.assertFalse(b._task.done())
   self.a_session.session.check();self.hidden_host.data_session.check()
   self.assertTrue(self.a_session.session.active and self.hidden_host.data_session.active)
   self.assertGreater(len(seen),1)
   self.assertEqual(before,[(c._credits_bytes,c._credits_tiles,c._used_bytes,c._used_tiles,c._peer_capacity) for c in (a,b)])
   self.assertEqual(frames,[sum(f[:1] in (b'H',b'C',b'D') for f in c.w.frames) for c in (a,b)])
   self.assertTrue(all(c._queue.empty() for c in (a,b)))
  finally:b.w.write=original
  return actual
 async def test_result_after_verified_pulses_beyond_original_idle_limit(self):await self.run_chain()

def load_tests(*args):return unittest.TestSuite([Waited('test_result_after_verified_pulses_beyond_original_idle_limit')])
if __name__=='__main__':unittest.main()
