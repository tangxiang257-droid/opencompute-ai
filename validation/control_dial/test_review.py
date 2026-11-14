"""Independent memory-only failure review; no actual TLS or network."""
import asyncio,unittest
from unittest.mock import patch
from validation.control_dial.test_failures import Cases,context
import cluster_p1.role_dial as dial
import private_experiment_topology as topology
from validation.control_exchange.test_transport import Holds
class Review(Cases):
 async def test_wrong_actual_action_certificate(self):
  l=await self.prepare_dial();self.streams(1)[1].cert=b'foreign';count=[]
  async def opened(*a,**kw):i=len(count);count.append(i);return self.streams(i)
  with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
   with self.assertRaises(BaseException):await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
  self.assertEqual(len(count),2);self.assertTrue(l._closing);self.assertTrue(l.operation.retired)
  self.assertTrue(all(self.streams(i)[1].is_closing() for i in (0,1)));self.assertEqual(self.calls,[[],[]])
 async def test_wrong_actual_control_certificate(self):
  l=await self.prepare_dial();self.streams(0)[1].cert=b'foreign';count=[]
  async def opened(*a,**kw):i=len(count);count.append(i);return self.streams(i)
  with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
   with self.assertRaises(BaseException):await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
  self.assertEqual(len(count),2);self.assertTrue(l._closing);self.assertTrue(l.operation.retired)
  self.assertTrue(all(self.streams(i)[1].is_closing() for i in (0,1)));self.assertEqual(self.calls,[[],[]])
 async def test_resource_unknown_after_first_connection(self):
  l=await self.prepare_dial();count=[];r=l.startup.runtime['resources'];t=l.startup.runtime['ticket'];before=r.query(t.reservation_id)['demand']
  async def opened(*a,**kw):count.append(0);r._unknown(t.reservation_id);return self.streams(0)
  with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
   with self.assertRaises(BaseException):await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
  self.assertEqual(len(count),1);self.assertTrue(self.streams(0)[1].is_closing());self.assertTrue(l.operation.retired)
  self.assertEqual(r.query(t.reservation_id)['state'],'UNKNOWN');self.assertEqual(r.query(t.reservation_id)['demand'],before)
 async def test_operation_runtime_drift_refuses_before_second_dial(self):
  l=await self.prepare_dial();count=[];original=l.operation.resources;ticket=l.operation.ticket;before=original.query(ticket.reservation_id)['demand'];seen=[]
  class Foreign:
   def verify_stage(self,*args):return None
   def quarantine(self,*args):seen.append(True)
  async def opened(*a,**kw):
   i=len(count);count.append(i)
   if i==0:l.operation.resources=Foreign()
   return self.streams(i)
  try:
   with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context):
    with self.assertRaises(BaseException):await dial.dial_control_ready(*self.dial_args,**self.dial_kw)
   self.assertEqual(self.calls,[[],[]]);self.assertEqual(original.query(ticket.reservation_id)['demand'],before)
   self.assertEqual(len(count),1,'operation runtime drift reached second dial')
  finally:l.operation.resources=original # fixture restoration only, no recovery
 async def test_announce_external_close_has_no_cycle(self):
  l=await self.prepare_dial();client=self.channels[-2];count=[];entered=asyncio.Event();release=asyncio.Event();old=topology._send
  async def opened(*a,**kw):i=len(count);count.append(i);return self.streams(i)
  async def send(ep,scope,seq,value,fence):
   if value['kind']=='ANNOUNCE':entered.set();await release.wait()
   return await old(ep,scope,seq,value,fence)
  with patch.object(dial.asyncio,'open_connection',opened),patch.object(dial,'load_context',context),patch.object(topology,'_send',send):
   task=asyncio.create_task(dial.dial_control_ready(*self.dial_args,**self.dial_kw))
   central=asyncio.create_task(topology.receive_control(self.adm,self.token,self.receipt.proposal_id,self.receipt.proposal_digest,0,client,identity=Holds(),resources=self.central_resources,expected_charge=2097152))
   close=None
   try:
    await asyncio.wait_for(entered.wait(),5);self.assertIn(task,l._jobs)
    close=asyncio.create_task(l.close());done,pending=await asyncio.wait((task,central,close),timeout=5)
    self.assertFalse(pending,'close waited on a caller joining itself')
    results=await asyncio.gather(task,central,close,return_exceptions=True)
    self.assertIsInstance(results[0],asyncio.CancelledError);self.assertIsNone(results[2]);self.assertIsInstance(results[1],BaseException)
   finally:
    release.set()
    for t in (task,central,close):
     if t is not None and not t.done():t.cancel()
    await asyncio.gather(*(t for t in (task,central,close) if t is not None),return_exceptions=True)
  self.assertTrue(l._close_task.done());self.assertTrue(all(self.streams(i)[1].is_closing() for i in (0,1)));self.assertEqual(self.calls,[[],[]])
def load_tests(*args):return unittest.TestSuite(Review(n) for n in Review.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
