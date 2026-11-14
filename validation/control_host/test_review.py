import unittest,asyncio
from validation.install_host.test_host import Socket,context
import private_control_action_host as module
from contextlib import contextmanager
from unittest.mock import patch
from validation.control_host.test_failures import Cases
from validation.control_exchange.test_transport import Holds
class Review(Cases):
 async def asyncSetUp(self):
  original=Holds.hold
  @contextmanager
  def held(obj,*a,**kw):
   with original(obj,*a,**kw):yield None
   hook=getattr(obj,'review_exit',None)
   if hook:hook()
  self.patcher=patch.object(Holds,'hold',held);self.patcher.start();self.addCleanup(self.patcher.stop)
  await super().asyncSetUp()
 async def test_installation_replacement_on_provider_exit_refuses(self):
  host=await self.candidate();v=await host._preflight();key=(host.c['experiment'],0)
  def replace():self.adm._installations[key]=dict(v)
  self.central_resources.review_exit=replace
  try:
   with self.assertRaises(Exception):await host._preflight()
  finally:
   self.central_resources.review_exit=None;self.adm._installations[key]=v
  await host.close()
 async def test_external_close_during_control_bootstrap(self):
  host=await self.candidate();sockets=[Socket(),Socket()];accepted=[Socket(),Socket()];entered=asyncio.Event()
  async def accept(sock):return accepted[sockets.index(sock)],('127.0.0.1',30000)
  async def handshake(slot,sock,ctx,*args):return self.central_streams[('control','actions')[accepted.index(sock)]]
  async def receive(*a,**kw):entered.set();await asyncio.Event().wait()
  with patch.object(module.socket,'socket',side_effect=sockets),patch.object(host.loop,'sock_accept',accept),patch.object(module,'load_context',lambda *a,**kw:context()),patch.object(module,'_accepted_tls_streams',handshake),patch.object(module,'receive_control',receive):
   start=asyncio.create_task(host.start(enabled=True));await asyncio.wait_for(entered.wait(),3)
   close=asyncio.create_task(host.close())
   done,pending=await asyncio.wait((start,close),timeout=3)
   self.assertFalse(pending,'bootstrap cancellation cleanup cycle')
   results=await asyncio.gather(start,close,return_exceptions=True)
   self.assertIsInstance(results[0],asyncio.CancelledError);self.assertIsNone(results[1])
  self.assertTrue(all(slot.writer.is_closing() and slot.channel._task.done() for slot in host.endpoints.values()))
  self.assertTrue(host.claimed);self.assertEqual(self.calls,[[],[]])
def load_tests(*args):return unittest.TestSuite(Review(n) for n in Review.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
