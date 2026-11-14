import asyncio,hashlib,json,ssl,unittest
from dataclasses import replace,asdict
from unittest.mock import patch
from validation.b_result_dial.test_pipeline import Flow,digest
from cluster_p1.result_dial import BResultDial
from cluster_p1.control_exchange import _json,_parse
import cluster_p1.result_dial as module
class Cases(Flow):
 async def candidate(self):
  actual=await self.execute_a();await self.pending_endpoints(actual)
  return self.result_dial,self.result_host
 def context(self,*a,**kw):return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
 async def test_default_missing_provider_and_wrong_input_before_network(self):
  b,c=await self.candidate();called=[]
  async def opened(*a,**kw):called.append(1);raise AssertionError('no network')
  with patch.object(module.asyncio,'open_connection',opened):
   with self.assertRaises(Exception):await b.prepare()
   for key in self.dial_kw:
    with self.assertRaises(Exception):BResultDial(*self.dial_args,**dict(self.dial_kw,**{key:None}))
   spec=replace(b.spec,inputs=('0'*64,));conf=_parse(b.raw);conf['spec_sha256']=digest(asdict(spec));raw=_json(conf)
   other=BResultDial(raw,hashlib.sha256(raw).hexdigest(),b.launcher,spec,*self.dial_args[4:],**self.dial_kw)
   with self.assertRaises(Exception):await other.prepare(enabled=True)
  self.assertEqual(called,[]);self.assertFalse(b.claimed);self.assertFalse(other.claimed);self.assertFalse(b.launcher._closing)
 async def test_second_dial_failure_retires_original_B(self):
  b,c=await self.candidate();calls=[]
  async def opened(*a,**k):
   calls.append(1)
   if len(calls)==2:raise OSError('LAB second result connection failed')
   return self.result_streams['data'][0]
  with patch.object(module,'load_context',self.context),patch.object(module.asyncio,'open_connection',opened):
   with self.assertRaises(OSError):await b.prepare(enabled=True)
   with self.assertRaises(Exception):await b.prepare(enabled=True)
  self.assertEqual(len(calls),2);self.assertTrue(b.launcher._closing);self.assertTrue(self.result_streams['data'][0][1].closed);self.assertEqual(self.calls[1],[])
  self.assertEqual(b.resources.query(b.ticket.reservation_id)['state'],'UNKNOWN')
 async def test_wrong_C_certificate_rejected(self):
  b,c=await self.candidate();pair=self.result_streams['data'][0];pair[1].cert=b'foreign'
  async def opened(*a,**k):return pair
  with patch.object(module,'load_context',self.context),patch.object(module.asyncio,'open_connection',opened):
   with self.assertRaises(Exception):await b.prepare(enabled=True)
  self.assertTrue(pair[1].closed);self.assertTrue(b.launcher._closing);self.assertEqual(b.state,'UNKNOWN')
 async def test_external_close_and_second_cancel_retain_late_stream(self):
  b,c=await self.candidate();entered=asyncio.Event();cancelled=asyncio.Event();release=asyncio.Event()
  async def opened(*a,**k):
   entered.set()
   try:await asyncio.Event().wait()
   except asyncio.CancelledError:cancelled.set();await release.wait();return self.result_streams['data'][0]
  with patch.object(module,'load_context',self.context),patch.object(module.asyncio,'open_connection',opened):
   start=asyncio.create_task(b.prepare(enabled=True));await entered.wait();close=asyncio.create_task(b.launcher.close());await asyncio.wait_for(cancelled.wait(),2)
   self.assertFalse(close.done());start.cancel();release.set();done,pending=await asyncio.wait((start,close),timeout=4);self.assertFalse(pending)
   result=await asyncio.gather(start,close,return_exceptions=True);self.assertIsInstance(result[0],asyncio.CancelledError);self.assertIsNone(result[1])
  self.assertTrue(self.result_streams['data'][0][1].closed);self.assertEqual(b.state,'UNKNOWN')
 async def test_competing_dial_does_not_retire_owner(self):
  b,c=await self.candidate();entered=asyncio.Event()
  async def opened(*a,**k):entered.set();await asyncio.Event().wait()
  with patch.object(module,'load_context',self.context),patch.object(module.asyncio,'open_connection',opened):
   start=asyncio.create_task(b.prepare(enabled=True));await entered.wait()
   other=BResultDial(*self.dial_args,**self.dial_kw)
   with self.assertRaises(Exception):await other.prepare(enabled=True)
   self.assertFalse(other.claimed);self.assertFalse(b.launcher._closing);self.assertFalse(start.done())
   start.cancel()
   with self.assertRaises(asyncio.CancelledError):await start
 async def test_raw_connected_streams_owned_by_launcher(self):
  b,c=await self.candidate();await self.connect_endpoints(b,c)
  await b.launcher.close();await c.launcher.close()
  self.assertTrue(all(w.closed for pair in self.result_streams.values() for _,w in pair));self.assertEqual(self.calls[1],[])
 async def test_pending_C_cannot_attach_or_ACK(self):
  b,c=await self.candidate();await self.connect_endpoints(b,c)
  with self.assertRaises(Exception):await c.attach_ready(enabled=True)
  self.assertIsNone(c.role.ingress);self.assertIsNone(c.launcher.gate);self.assertEqual(c.state,'UNKNOWN');self.assertEqual(self.calls[1],[])
 async def test_expired_preparation_has_no_execution_qualification(self):
  b,c=await self.candidate();await self.connect_endpoints(b,c);b.deadline=b.loop.time()-1
  with self.assertRaises(Exception):b.generation()
  with self.assertRaises(Exception):await b.attach_ready(enabled=True)
  self.assertTrue(b.launcher._closing);self.assertEqual(b.state,'UNKNOWN');self.assertEqual(self.calls[1],[])
 async def test_C_unknown_record_cannot_prepare(self):
  b,c=await self.candidate();c.role.store.unknown(c.role.d)
  with patch('private_c_result_host.socket.socket',side_effect=AssertionError('must not listen')):
   with self.assertRaises(Exception):await c.prepare(enabled=True)
  self.assertFalse(c.claimed);self.assertIsNone(c.role.ingress)
def load_tests(*args):return unittest.TestSuite(Cases(n) for n in Cases.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
