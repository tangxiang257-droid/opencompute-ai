"""Injected faults, real admission and installation records; no sockets opened."""
import asyncio,hashlib,unittest
from unittest.mock import patch
from validation.control_host.test_host import Flow
from validation.install_host.test_host import Socket,context
from cluster_p1.control_exchange import _json,_parse
import private_control_action_host as module

class Cases(Flow):
 async def candidate(self):
  await self.host_inputs(0,self.spec)
  return module.ControlActionHost(*self.host_args,**self.host_kw)
 async def test_disabled_and_missing_providers(self):
  host=await self.candidate()
  with patch.object(module.socket,'socket',side_effect=AssertionError('must not listen')):
   with self.assertRaises(Exception):await host.start()
   for k in self.host_kw:
    with self.assertRaises(Exception):module.ControlActionHost(*self.host_args,**dict(self.host_kw,**{k:None}))
  self.assertFalse(host.started);await host.close();self.assertEqual(self.calls,[[],[]])
 async def test_invalid_clock_stops_before_socket(self):
  host=await self.candidate();entered=[]
  class BadClock:
   def sample(self):raise ValueError('LAB clock unavailable')
  host=module.ControlActionHost(*self.host_args,**dict(self.host_kw,clock=BadClock()))
  def opened(*a):entered.append(1);raise RuntimeError('LAB socket reached')
  with patch.object(module,'load_context',lambda *a,**k:context()),patch.object(module.socket,'socket',opened):
   with self.assertRaises(Exception):await host.start(enabled=True)
  self.assertEqual(entered,[])
 async def test_public_binding_and_material_rejected(self):
  await self.candidate()
  for key,value in [('bind','0.0.0.0'),('port',True),('material_sha256','0'*64)]:
   c=_parse(self.host_args[0]);c['control'][key]=value;raw=_json(c)
   with self.assertRaises(Exception):module.ControlActionHost(raw,hashlib.sha256(raw).hexdigest(),*self.host_args[2:],**self.host_kw)
 async def test_wrong_issuer_rejected_before_listen(self):
  host=await self.candidate();c=_parse(host.raw);c['actions']['node']='foreign';raw=_json(c)
  host=module.ControlActionHost(raw,hashlib.sha256(raw).hexdigest(),*self.host_args[2:],**self.host_kw)
  with patch.object(module.socket,'socket',side_effect=AssertionError('must not listen')):
   with self.assertRaises(Exception):await host.start(enabled=True)
  self.assertFalse(host.claimed);self.assertEqual(self.calls,[[],[]])
 async def test_second_bind_failure_closes_first_and_no_retry(self):
  host=await self.candidate();sockets=[Socket(),Socket()]
  def failed(address):raise OSError('LAB second bind')
  sockets[1].bind=failed
  with patch.object(module,'load_context',lambda *a,**k:context()),patch.object(module.socket,'socket',side_effect=sockets):
   with self.assertRaises(OSError):await host.start(enabled=True)
   with self.assertRaises(Exception):await host.start(enabled=True)
  self.assertTrue(all(s.closed for s in sockets));self.assertTrue(host.claimed);self.assertEqual(self.calls,[[],[]])
 async def test_cancel_accept_retains_late_socket_and_competing_host_refuses(self):
  host=await self.candidate();other=module.ControlActionHost(*self.host_args,**self.host_kw)
  sockets=[Socket(),Socket()];late=Socket();entered=asyncio.Event();cancelled=asyncio.Event();release=asyncio.Event()
  async def accept(sock):
   entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError:cancelled.set();await release.wait();return late,('127.0.0.1',10000)
  with patch.object(module,'load_context',lambda *a,**k:context()),patch.object(module.socket,'socket',side_effect=sockets),patch.object(host.loop,'sock_accept',accept):
   task=asyncio.create_task(host.start(enabled=True));await entered.wait()
   with self.assertRaises(Exception):await other.start(enabled=True)
   self.assertFalse(other.claimed);self.assertFalse(task.done())
   close=asyncio.create_task(host.close());await asyncio.wait_for(cancelled.wait(),2);release.set()
   done,pending=await asyncio.wait((task,close),timeout=3);self.assertFalse(pending)
   results=await asyncio.gather(task,close,return_exceptions=True)
   self.assertIsInstance(results[0],asyncio.CancelledError);self.assertIsNone(results[1])
  self.assertTrue(late.closed);self.assertTrue(all(s.closed for s in sockets));self.assertEqual(self.calls,[[],[]])
 async def test_original_installation_cannot_be_replaced(self):
  host=await self.candidate();v=await host._preflight();key=(host.c['experiment'],0)
  self.adm._installations[key]=dict(v)
  try:
   with self.assertRaises(Exception):await host._preflight()
  finally:self.adm._installations[key]=v
  await host.close()
 async def test_cleanup_failure_is_retained(self):
  host=await self.candidate();marker=OSError('LAB listener cleanup failed');slot=host.endpoints['control'];sock=Socket();slot.listener=sock
  def fail():raise marker
  sock.close=fail
  for _ in range(2):
   with self.assertRaises(BaseExceptionGroup) as caught:await host.close()
   self.assertIs(caught.exception.exceptions[0].exceptions[0].exceptions[0],marker)
  self.assertEqual(host.state,'UNKNOWN');self.assertIs(slot.listener,sock)

def load_tests(loader,tests,pattern):return unittest.TestSuite(Cases(n) for n in sorted(Cases.__dict__) if n.startswith('test_'))
if __name__=='__main__':unittest.main()
