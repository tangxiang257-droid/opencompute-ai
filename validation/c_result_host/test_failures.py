import asyncio,hashlib,unittest
from contextlib import contextmanager
from dataclasses import replace
from unittest.mock import patch
from validation.c_result_host.test_host import Flow
from validation.install_host.test_host import Socket,context
from cluster_p1.control_exchange import _json,_parse
from cluster_p1.role_launcher import ReceivingLauncher
import private_c_result_host as module
class Stop(Exception):pass
class Cases(Flow):
 async def start_c_host(self,host,pairs):
  self.owned.remove(host)
  try:await self.probe(host,pairs)
  finally:
   if host.cleanup is None:await host.close()
  raise Stop
 async def run_probe(self,probe):
  self.probe=probe
  with self.assertRaises(Stop):await self.run_chain()
 async def test_disabled_missing_provider_or_foreign_resource(self):
  async def probe(h,p):
   with patch.object(module.socket,'socket',side_effect=AssertionError('must not listen')):
    with self.assertRaises(Exception):await h.start()
    for k in self.c_host_kw:
     with self.assertRaises(Exception):module.CResultHost(*self.c_host_args,**dict(self.c_host_kw,**{k:None}))
    from validation.control_exchange.test_transport import Holds
    other=module.CResultHost(*self.c_host_args,**dict(self.c_host_kw,resources=Holds()))
    with self.assertRaises(Exception):await other.start(enabled=True)
   self.assertFalse(h.claimed);self.assertFalse(other.claimed);self.assertFalse(h.launcher._closing)
  await self.run_probe(probe)
 async def test_invalid_bind_and_wrong_actual_input(self):
  async def probe(h,p):
   c=_parse(h.raw);c['data']['bind']='0.0.0.0';raw=_json(c)
   with self.assertRaises(Exception):module.CResultHost(raw,hashlib.sha256(raw).hexdigest(),*self.c_host_args[2:],**self.c_host_kw)
   bad=replace(h.spec,inputs=('0'*64,));c=_parse(h.raw)
   import json
   from dataclasses import asdict
   c['spec_sha256']=hashlib.sha256(_json(json.loads(json.dumps(asdict(bad))))).hexdigest();raw=_json(c)
   other=module.CResultHost(raw,hashlib.sha256(raw).hexdigest(),h.launcher,bad,*self.c_host_args[4:],**self.c_host_kw)
   with patch.object(module.socket,'socket',side_effect=AssertionError('must not listen')):
    with self.assertRaises(Exception):await other.start(enabled=True)
   self.assertFalse(other.claimed)
  await self.run_probe(probe)
 async def test_revoke_on_provider_exit_before_listen(self):
  async def probe(h,p):
   cls=type(h.role.resources);old=cls.hold
   @contextmanager
   def held(obj,*a,**kw):
    with old(obj,*a,**kw):yield None
    h.role.receiving=False
   # Instantiate within patched provider method so the method pin is genuine.
   with patch.object(cls,'hold',held):
    other=module.CResultHost(*self.c_host_args,**self.c_host_kw)
    with patch.object(module.socket,'socket',side_effect=AssertionError('must not listen')):
     with self.assertRaises(Exception):await other.start(enabled=True)
   self.assertFalse(other.claimed)
  await self.run_probe(probe)
 async def test_wrong_TLS_peer_retires_and_no_retry(self):
  async def probe(h,p):
   p['data'][1].cert=b'foreign';sockets=[Socket(),Socket(),Socket()];accepted=Socket()
   async def accept(sock):return accepted,('127.0.0.1',30000)
   async def handshake(*a):return p['data']
   with patch.object(module.socket,'socket',side_effect=sockets),patch.object(h.loop,'sock_accept',accept),patch.object(module,'load_context',lambda *a,**k:context()),patch.object(module,'_accepted_tls_streams',handshake):
    with self.assertRaises(Exception):await h.start(enabled=True)
    with self.assertRaises(Exception):await h.start(enabled=True)
   self.assertTrue(h.launcher._closing);self.assertEqual(h.state,'UNKNOWN');self.assertTrue(all(s.closed for s in sockets));self.assertTrue(p['data'][1].closed)
   self.assertEqual(h.role.store.history(h.spec.owner,h.spec.request)['state'],'UNKNOWN')
  await self.run_probe(probe)
 async def test_external_launcher_close_waits_late_TLS(self):
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
    self.assertFalse(close.done(),'launcher forgot late TLS responsibility');release.set()
    done,pending=await asyncio.wait((start,close),timeout=4);self.assertFalse(pending)
    results=await asyncio.gather(start,close,return_exceptions=True);self.assertIsInstance(results[0],asyncio.CancelledError);self.assertIsNone(results[1])
   self.assertTrue(p['data'][1].closed);self.assertTrue(all(s.closed for s in sockets));self.assertEqual(h.state,'UNKNOWN')
  await self.run_probe(probe)
 async def test_gate_attach_failure_retires_consumed_ingress(self):
  async def probe(h,p):
   sockets=[Socket(),Socket(),Socket()];accepted=[Socket(),Socket(),Socket()]
   async def accept(sock):return accepted[sockets.index(sock)],('127.0.0.1',30000)
   async def handshake(slot,sock,*args):return p[('data','metadata','gate')[accepted.index(sock)]]
   def failed(*a,**k):raise OSError('LAB gate attach failed')
   with patch.object(module.socket,'socket',side_effect=sockets),patch.object(h.loop,'sock_accept',accept),patch.object(module,'load_context',lambda *a,**k:context()),patch.object(module,'_accepted_tls_streams',handshake),patch.object(ReceivingLauncher,'attach_gate',failed):
    with self.assertRaises(OSError):await h.start(enabled=True)
   self.assertTrue(h.role.claimed);self.assertTrue(h.launcher._closing);self.assertEqual(h.state,'UNKNOWN');self.assertTrue(all(w.closed for _,w in p.values()))
   self.assertEqual(h.role.store.history(h.spec.owner,h.spec.request)['state'],'UNKNOWN')
  await self.run_probe(probe)
def load_tests(*args):return unittest.TestSuite(Cases(n) for n in Cases.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
