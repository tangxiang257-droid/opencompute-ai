"""Ordinary orchestration stand-in only; not an authority or identity test."""
import asyncio,unittest
from types import SimpleNamespace
from private_experiment_actions import ActionServer
class Sequence(unittest.IsolatedAsyncioTestCase):
 async def test_server_does_not_consume_sequence_before_open(self):
  entered=asyncio.Event();checked=asyncio.Event();release=asyncio.Event()
  async def opening():entered.set();await release.wait()
  s=object.__new__(ActionServer);s._sequence=0;s._action_session=object();s.authority=SimpleNamespace(dead=False)
  s.check=checked.set;s._action_session_open=asyncio.create_task(opening())
  await entered.wait();task=asyncio.create_task(s.serve_once())
  barrier=asyncio.Event();asyncio.get_running_loop().call_soon(barrier.set);await barrier.wait()
  try:self.assertEqual(s._sequence,0)
  finally:
   task.cancel();s._action_session_open.cancel();await asyncio.gather(task,s._action_session_open,return_exceptions=True)
if __name__=='__main__':unittest.main()
