"""LAB authority/byte fixtures; injected accept/TLS, never real sockets."""
import asyncio,unittest
from contextlib import contextmanager
from unittest.mock import patch
from validation.install_host.test_host import Flow,Socket,context
from validation.role_bridge import test_transport as wire
from cluster_p1.channel import Grant
class Failures(Flow):
 async def install_stages(self):
  s=self.spec;r,w,_,_=wire.streams('issuer','a');g=Grant(s.request,s.session,1,0,(0,1),s.model,2)
  self.host=await self.create_installation_endpoint(0,r,w,wire.config('a'),{'a':{g}})
 @contextmanager
 def io(self,accept,handshake=None):
  self.listener=Socket()
  with patch('private_installation_host.socket.socket',return_value=self.listener) as factory,patch.object(asyncio.get_running_loop(),'sock_accept',accept),patch('private_installation_host.load_context',lambda *a,**k:context()):
   if handshake:
    with patch('private_installation_host.InstallationHost._handshake',handshake):yield factory
   else:yield factory
 async def test_disabled_and_missing_current_resource_before_socket(self):
  with patch('private_installation_host.socket.socket') as factory:
   with self.assertRaises(Exception):await self.host.start()
   self.host.resources=object()
   with self.assertRaises(Exception):await self.host.start(enabled=True)
   factory.assert_not_called()
 async def test_cancel_accept_retains_late_socket(self):
  entered=asyncio.Event();accepted=Socket()
  async def accept(sock):
   entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError:return accepted,('127.0.0.1',1)
  with self.io(accept):
   run=asyncio.create_task(self.host.start(enabled=True));await entered.wait();run.cancel()
   with self.assertRaises(asyncio.CancelledError):await run
  self.assertTrue(accepted.closed);self.assertTrue(self.listener.closed);self.assertEqual(self.host.state,'CLOSED')
 async def test_handshake_exception_never_installs(self):
  accepted=Socket()
  async def accept(sock):return accepted,('127.0.0.1',1)
  async def handshake(host,sock,ctx):raise ValueError('LAB handshake denied')
  with self.io(accept,handshake):
   with self.assertRaises(ValueError):await self.host.start(enabled=True)
  self.assertTrue(accepted.closed);self.assertNotIn((self.receipt.proposal_id,0),self.adm._installations)
 async def test_unobserved_handoff_is_unknown(self):
  accepted=Socket()
  async def accept(sock):return accepted,('127.0.0.1',1)
  async def handshake(host,sock,ctx):host.handed=True;raise ValueError('LAB uncertain handoff')
  with self.io(accept,handshake):
   with self.assertRaises(BaseException):await self.host.start(enabled=True)
  self.assertEqual(self.host.state,'UNKNOWN');self.assertFalse(accepted.closed)
  accepted.close() # Test boundary owns the synthetic leak; not host cleanup evidence.
 async def test_same_admission_stage_cannot_reopen(self):
  async def accept(sock):raise ValueError('LAB failed first accept')
  with self.io(accept):
   with self.assertRaises(ValueError):await self.host.start(enabled=True)
  original=self.host;await self.install_stages()
  with patch('private_installation_host.socket.socket') as factory:
   with self.assertRaises(Exception):await self.host.start(enabled=True)
   factory.assert_not_called()
  self.assertIs(self.adm._installation_hosts[(self.receipt.proposal_id,0)],original)
 async def test_authorization_revoked_after_accept(self):
  accepted=Socket()
  async def accept(sock):self.adm._installation_hold=None;return accepted,('127.0.0.1',1)
  with self.io(accept),patch('private_installation_host.InstallationHost._handshake') as handshake:
   with self.assertRaises(Exception):await self.host.start(enabled=True)
   handshake.assert_not_called()
  self.assertTrue(accepted.closed);self.assertTrue(self.listener.closed)
 async def test_cancel_with_cleanup_cause_is_unknown(self):
  entered=asyncio.Event()
  async def accept(sock):
   entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError as error:raise error from OSError('LAB cleanup failure')
  with self.io(accept):
   run=asyncio.create_task(self.host.start(enabled=True));await entered.wait();run.cancel()
   with self.assertRaises(BaseException):await run
  self.assertEqual(self.host.state,'UNKNOWN')
 async def test_mutated_peer_pin_rejected_before_socket(self):
  self.host.tls.peers['a']='0'*64
  async def accept(sock):raise ValueError('LAB sentinel')
  with self.io(accept) as factory:
   with self.assertRaises(Exception):await self.host.start(enabled=True)
   factory.assert_not_called()
 async def test_wrong_authenticated_peer_never_installs(self):
  accepted=Socket();r,w,_,_=wire.streams('issuer','evil')
  async def accept(sock):return accepted,('127.0.0.1',1)
  async def handshake(host,sock,ctx):return r,w
  with self.io(accept,handshake):
   with self.assertRaises(Exception):await self.host.start(enabled=True)
  self.assertNotIn((self.receipt.proposal_id,0),self.adm._installations)
 async def test_actual_handshake_method_retains_transport_before_return(self):
  host=self.host;entered=asyncio.Event();accepted=Socket()
  class Transport:
   def __init__(self):self.closed=False;self.protocol=None
   def get_extra_info(self,name,default=None):return default
   def is_closing(self):return self.closed
   def close(self):
    self.closed=True;self.protocol.connection_lost(None)
  transport=Transport()
  async def connect(factory,sock,**kw):
   self.assertIs(sock,accepted);self.assertEqual(kw['ssl'].verify_mode,2)
   p=factory();transport.protocol=p;p.connection_made(transport);entered.set();await asyncio.Event().wait()
  with patch.object(asyncio.get_running_loop(),'connect_accepted_socket',connect):
   host.accepted=accepted;task=asyncio.create_task(host._handshake(accepted,context()));host.pending['handshake']=task
   await entered.wait();await host.close()
  self.assertTrue(transport.closed);self.assertFalse(accepted.closed);self.assertEqual(host.state,'CLOSED')
 async def test_cleanup_timeout_retains_task_and_unknown(self):
  host=self.host;entered=asyncio.Event();release=asyncio.Event()
  class Writer:
   def close(self):entered.set()
   async def wait_closed(self):await release.wait()
  host.writer=Writer();host.c['close_seconds']=0.01
  try:
   with self.assertRaises(TimeoutError):await host.close()
   self.assertTrue(entered.is_set());self.assertFalse(host.cleanup.done());self.assertEqual(host.state,'UNKNOWN')
  finally:release.set();await host.close()
  self.assertEqual(host.state,'UNKNOWN')
 async def test_bind_and_listen_failure_close_owned_socket(self):
  async def accept(sock):raise AssertionError('must not accept')
  with self.io(accept),patch.object(Socket,'listen',side_effect=OSError('LAB bind policy')):
   with self.assertRaises(OSError):await self.host.start(enabled=True)
  self.assertTrue(self.listener.closed)
 async def test_writer_close_failure_still_waits(self):
  calls=[]
  class Writer:
   def close(self):calls.append('close');raise OSError('LAB close error')
   async def wait_closed(self):calls.append('wait')
  self.host.writer=Writer()
  with self.assertRaises(BaseExceptionGroup):await self.host.close()
  self.assertEqual(calls,['close','wait']);self.assertEqual(self.host.state,'UNKNOWN')
 async def test_bind_policy_and_missing_provider(self):
  import hashlib
  from private_installation_host import InstallationHost
  from cluster_p1.control_exchange import _json
  h=self.host
  for address in ('0.0.0.0','::','8.8.8.8','169.254.1.2','224.0.0.1','example.org'):
   c=dict(h.c,bind=address);raw=_json(c)
   with self.assertRaises(Exception):InstallationHost(raw,hashlib.sha256(raw).hexdigest(),h.a,h.credential,h.tls,h.grants,h.material,identity=h.identity,authorize=h.authorize,resources=h.resources)
  with self.assertRaises(Exception):InstallationHost(h.raw,h.pin,h.a,h.credential,h.tls,h.grants,h.material)
 async def test_user_exit_revocation_before_listen(self):
  original=self.adm.consent._user;calls=[]
  @contextmanager
  def changed(*args):
   with original(*args):yield
   calls.append('exit')
   if len(calls)==2:self.adm._installation_hold=None
  async def accept(sock):raise ValueError('LAB sentinel after listener')
  with patch.object(self.adm.consent,'_user',changed),self.io(accept) as factory:
   with self.assertRaises(Exception):await self.host.start(enabled=True)
   self.assertEqual(len(calls),2)
   factory.assert_not_called()
def load_tests(loader,tests,pattern):return unittest.TestSuite(Failures(n) for n in Failures.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
