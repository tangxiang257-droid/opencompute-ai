import asyncio,unittest
from contextlib import contextmanager
from dataclasses import replace
from unittest.mock import patch
from validation.c_result_host.test_failures import Cases
from validation.install_host.test_host import Socket,context
import private_c_result_host as module
class Review(Cases):
 async def test_attached_spec_drift_on_provider_exit_is_rejected(self):
  async def probe(h,p):
   cls=type(h.role.resources);old=cls.hold
   @contextmanager
   def held(obj,*a,**kw):
    with old(obj,*a,**kw):yield None
    if h.role.claimed:h.role.spec=replace(h.spec,inputs=('0'*64,))
   sockets=[Socket(),Socket(),Socket()];accepted=[Socket(),Socket(),Socket()]
   async def accept(sock):return accepted[sockets.index(sock)],('127.0.0.1',30000)
   async def handshake(slot,sock,*args):return p[('data','metadata','gate')[accepted.index(sock)]]
   with patch.object(cls,'hold',held):
    h.pin=h._pins()
    with patch.object(module.socket,'socket',side_effect=sockets),patch.object(h.loop,'sock_accept',accept),patch.object(module,'load_context',lambda *a,**k:context()),patch.object(module,'_accepted_tls_streams',handshake):
     with self.assertRaises(Exception):await h.start(enabled=True)
  await self.run_probe(probe)
 async def test_external_close_second_cancel_retains_late_TLS(self):
  async def probe(h,p):
   sockets=[Socket(),Socket(),Socket()];entered=asyncio.Event();cancelled=asyncio.Event();release=asyncio.Event()
   async def accept(sock):return Socket(),('127.0.0.1',30000)
   async def handshake(*args):
    entered.set()
    try:await asyncio.Event().wait()
    except asyncio.CancelledError:cancelled.set();await release.wait();return p['data']
   with patch.object(module.socket,'socket',side_effect=sockets),patch.object(h.loop,'sock_accept',accept),patch.object(module,'load_context',lambda *a,**k:context()),patch.object(module,'_accepted_tls_streams',handshake):
    start=asyncio.create_task(h.start(enabled=True));await asyncio.wait_for(entered.wait(),2)
    close=asyncio.create_task(h.launcher.close());await asyncio.wait_for(cancelled.wait(),2)
    start.cancel();self.assertFalse(close.done());release.set()
    done,pending=await asyncio.wait((start,close),timeout=4);self.assertFalse(pending)
    results=await asyncio.gather(start,close,return_exceptions=True)
    self.assertIsInstance(results[0],asyncio.CancelledError);self.assertIsNone(results[1])
   self.assertTrue(p['data'][1].closed);self.assertTrue(all(s.closed for s in sockets));self.assertEqual(h.state,'UNKNOWN')
  await self.run_probe(probe)
def load_tests(*args):return unittest.TestSuite(Review(n) for n in Review.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
