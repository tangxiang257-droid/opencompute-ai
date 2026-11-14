"""Actual boundary regression tests; ordinary bytes and synthetic explicit policy."""
import asyncio,struct,unittest
from contextlib import contextmanager
from unittest.mock import patch
from validation.split_roles.test_transport import Tests as Base
from cluster_p1.envelope import Envelope
class Tests(Base):
 def hidden_bytes(self):
  s=self.spec;return Envelope(s.request,s.session,1,0,0,(0,1),s.model,2,(1,1,s.hidden),'float32',struct.pack('<f',1.)*s.hidden).encode()
 async def test_revoke_while_receiving_blocks_durable_hidden_receipt(self):
  entered=asyncio.Event();resume=asyncio.Event();old=self.bound.receive_once
  async def waiting(*args,**kw):entered.set();await resume.wait();return await old(*args,**kw)
  with patch.object(self.bound,'receive_once',waiting):
   tasks=[asyncio.create_task(self.hs.send(self.hidden_bytes())),asyncio.create_task(self.hr.receive())]
   try:
    await asyncio.wait_for(entered.wait(),3);self.authority.revoked=True;resume.set()
    result=await asyncio.wait_for(asyncio.gather(*tasks,return_exceptions=True),3)
   finally:
    resume.set()
    for t in tasks:
     if not t.done():t.cancel()
    await asyncio.gather(*tasks,return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in result))
  # Revocation must reach the actual receipt commit fence.
  with self.receiver._store._tx() as c:self.assertEqual(c.execute('SELECT count(*) FROM p1_received_frames').fetchone(),(0,))
 async def test_revoke_while_waiting_write_lock_blocks_D_write(self):
  entered=asyncio.Event();old=self.hidden_send._frame;written=[];original_write=self.hidden_send.w.write
  async def frame(kind,data,**kw):
   if kind==b'D':entered.set()
   return await old(kind,data,**kw)
  def write(raw):
   if raw[:1]==b'D':written.append(self.authority.revoked)
   original_write(raw)
  await self.hidden_send._write.acquire()
  with patch.object(self.hidden_send,'_frame',frame),patch.object(self.hidden_send.w,'write',write):
   tasks=[asyncio.create_task(self.hs.send(self.hidden_bytes())),asyncio.create_task(self.hr.receive())]
   try:
    await asyncio.wait_for(entered.wait(),3);self.authority.revoked=True;self.hidden_send._write.release()
    await asyncio.wait_for(asyncio.gather(*tasks,return_exceptions=True),3)
   finally:
    if self.hidden_send._write.locked():self.hidden_send._write.release()
    for t in tasks:
     if not t.done():t.cancel()
    await asyncio.gather(*tasks,return_exceptions=True)
  # No D write after revocation wins while awaiting the write lock.
  self.assertEqual(written,[])
 async def test_C_missing_own_authorization_denies(self):
  from cluster_p1.split_role_transport import ResultIngress
  from validation.role_bridge.test_transport import config
  with self.assertRaises(Exception):ResultIngress(self.result_reader,self.result_writer,config('b'),self.mr,self.binding,**self.kw)
 async def test_write_fence_exit_failure_keeps_already_written_frame(self):
  from cluster_p1.envelope import decode
  @contextmanager
  def uncertain():yield None;raise OSError('write fence exit unknown')
  before=sum(f[:1]==b'D' for f in self.hidden_send.w.frames)
  with self.assertRaises(OSError):await self.hidden_send.send(decode(self.hidden_bytes()),write_fence=uncertain)
  self.assertEqual(sum(f[:1]==b'D' for f in self.hidden_send.w.frames),before+1)
  with self.assertRaises(Exception):await self.hidden_send.send(decode(self.hidden_bytes()),write_fence=uncertain)
  self.assertEqual(sum(f[:1]==b'D' for f in self.hidden_send.w.frames),before+1)
 async def test_write_provider_cannot_suppress_body_failure(self):
  from cluster_p1.envelope import decode
  @contextmanager
  def suppress():
   try:yield None
   except OSError:pass
  def fail(raw):raise OSError('write failed')
  with patch.object(self.hidden_send.w,'write',fail):
   with self.assertRaises(OSError):await self.hidden_send.send(decode(self.hidden_bytes()),write_fence=suppress)
  self.assertTrue(self.hidden_send._closed)
 async def test_boolean_write_permission_denied(self):
  from cluster_p1.envelope import decode
  @contextmanager
  def bad():yield True
  with self.assertRaises(Exception):await self.hidden_send.send(decode(self.hidden_bytes()),write_fence=bad)
  self.assertFalse(any(f[:1]==b'D' for f in self.hidden_send.w.frames))
 async def test_async_write_permission_not_executed(self):
  from cluster_p1.envelope import decode
  called=[]
  async def bad():called.append(1)
  with self.assertRaises(Exception):await self.hidden_send.send(decode(self.hidden_bytes()),write_fence=bad)
  self.assertEqual(called,[]);self.assertFalse(any(f[:1]==b'D' for f in self.hidden_send.w.frames))
 async def test_receipt_fence_exit_failure_retains_committed_record(self):
  old=self.authority.hold
  @contextmanager
  def uncertain(principal,binding,action):
   with old(principal,binding,action):yield None
   if action=='hidden-commit':raise OSError('receipt fence exit unknown')
  with patch.object(self.authority,'hold',uncertain):
   results=await asyncio.gather(self.hs.send(self.hidden_bytes()),self.hr.receive(),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in results))
  with self.receiver._store._tx() as c:self.assertEqual(c.execute('SELECT count(*) FROM p1_received_frames').fetchone(),(1,))
  with self.assertRaises(Exception):await self.hr.receive()
 async def test_receipt_provider_cannot_suppress_transaction_error(self):
  old=self.authority.hold
  @contextmanager
  def suppress(principal,binding,action):
   with old(principal,binding,action):
    try:yield None
    except OSError:pass
  with patch.object(self.authority,'hold',suppress),patch.object(self.receiver,'_retain_frame',side_effect=OSError('transaction failure')):
   results=await asyncio.gather(self.hs.send(self.hidden_bytes()),self.hr.receive(),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in results))
  with self.receiver._store._tx() as c:self.assertEqual(c.execute('SELECT count(*) FROM p1_received_frames').fetchone(),(0,))
 async def test_C_denies_its_own_publication_retains_files_and_charge(self):
  spool,digest=self.spool();old=self.recipient.hold_receiver
  @contextmanager
  def deny(context,action):
   if action=='result-commit':raise PermissionError('C no longer allows publication')
   with old(context,action):yield None
  with patch.object(self.recipient,'hold_receiver',deny):
   results=await asyncio.gather(self.rs.send(self.spec,spool,digest,lambda:None),self.ri.receive(self.spec,self.artifacts,lambda:None,self.fence),return_exceptions=True)
  self.assertTrue(all(isinstance(x,BaseException) for x in results));self.assertEqual(sum(f[:1]==b'D' for f in self.result_send.w.frames),19)
  self.assertEqual(self.db.execute('SELECT state FROM artifact_tickets').fetchone(),('WRITING',));self.assertEqual((self.source.used(),self.destination.used()),(640512,640512))
 async def test_C_cannot_reuse_B_transport_provider(self):
  from cluster_p1.split_role_transport import ResultIngress
  from validation.role_bridge.test_transport import config
  with self.assertRaises(Exception):ResultIngress(self.result_reader,self.result_writer,config('b'),self.mr,self.binding,recipient_node='c',receive_authorize=self.authority,**self.kw)
 async def test_C_policy_and_write_fences_never_span_network_waits(self):
  from validation.role_bridge.test_transport import Writer
  old=Writer.drain;actions=[];original=self.recipient.hold_receiver
  @contextmanager
  def receiver(context,action):
   actions.append((context,action))
   with original(context,action):yield None
  async def drain(w):
   self.assertEqual((self.authority.active,self.recipient.active),(0,0));await old(w)
  with patch.object(Writer,'drain',drain),patch.object(self.recipient,'hold_receiver',receiver):await self.test_full_hidden_and_result()
  self.assertTrue({'result-reserve','result-store','result-commit'}<={a for c,a in actions})
  self.assertTrue(all(c['recipient_node']=='c' and c['purpose']=='result-store' for c,a in actions))
if __name__=='__main__':
 names=[n for n in Tests.__dict__ if n.startswith('test_')]
 r=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(Tests(n) for n in names));raise SystemExit(not r.wasSuccessful())
