"""Real Channel/coordinator/SQLite, in-memory streams and fake TLS metadata only."""
import asyncio,hashlib,sqlite3,ssl,struct,tempfile,unittest,sys
from pathlib import Path
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from cluster_p1.channel import Channel,TLSConfig,Grant,ChannelError,accept,connect
from cluster_p1.nonce_coordinator import NonceCoordinator
from cluster_p1.store import Conflict
from cluster_p1.envelope import Envelope
from test_p1_tile_contract_v2 import fixture
try:from cluster_p1.prepared_receipt import PreparedReceiptConnection
except ImportError:PreparedReceiptConnection=None

def forbid_external(event,args):
 if event in ('socket.bind','socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'):
  raise AssertionError('offline test prohibits '+event)
sys.addaudithook(forbid_external)

@contextmanager
def owner():yield 'owner'
class Reader:
 def __init__(self):self.stream=asyncio.StreamReader();self.reads=[];self.entered=asyncio.Event()
 def feed(self,raw):self.stream.feed_data(raw)
 async def readexactly(self,n):self.reads.append(n);self.entered.set();return await self.stream.readexactly(n)
class Writer:
 def __init__(self,peer=None):
  self.peer=peer;self.frames=[];self.closed=False;self.cert=b'fake-cert';self.closed_event=asyncio.Event()
  self.context=SimpleNamespace(verify_mode=ssl.CERT_REQUIRED,check_hostname=True)
 def get_extra_info(self,name):return self if name=='ssl_object' else None
 @property
 def server_side(self):return False
 def version(self):return 'TLSv1.2'
 def getpeercert(self,binary_form=False):return self.cert
 def write(self,raw):
  self.frames.append(raw)
  if self.peer is not None:self.peer.feed(raw)
 async def drain(self):pass
 def close(self):self.closed=True;self.closed_event.set()
 def is_closing(self):return self.closed
 async def wait_closed(self):pass
class Tests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.assertIsNotNone(PreparedReceiptConnection,'prepared-receipt assembly missing')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.channels=[];self.prepared=[];self.coordinators=[]
  self.sid='1'*32;self.fp=hashlib.sha256(b'fake-cert').hexdigest()
  self.cfg=TLSConfig('unused','unused','unused',{'a':self.fp})
  self.grants={'a':{Grant('t',self.sid,1,0,(0,5),'a'*64,2)}}
 async def asyncTearDown(self):
  for p in self.prepared:
   if p.journal is not None:p.journal.close()
  for ch in self.channels:await ch.close()
  for co in self.coordinators:co.close()
 def coordinator(self,name='one'):
  co=NonceCoordinator.initialize(self.root/('registry-'+name),authorize=owner);self.coordinators.append(co)
  t,s,st,tiles=fixture();s=s.model_copy(update={'session_id':self.sid});st=tuple(x.model_copy(update={'session_id':self.sid}) for x in st);tiles=tuple(dict(x,session_id=self.sid) for x in tiles)
  r=co.register(t,s,st,tiles,hidden_size=24,vocab_size=32,config_sha256='c'*64)
  return co,r
 async def assembly(self,r=None,w=None):
  r=r or Reader();w=w or Writer();p=await PreparedReceiptConnection.create(r,w,self.cfg,self.grants,capacity_bytes=4096,capacity_tiles=1,timeout=2)
  self.prepared.append(p);self.channels.append(p.channel);return p,r,w
 def envelope(self):return Envelope('t',self.sid,1,0,0,(0,2),'a'*64,2,(1,2,24),'float32',struct.pack('<f',1.0)*48)
 async def test_ready_factory_emits_one_H_receipt_not_execution(self):
  p,r,w=await self.assembly();co,res=self.coordinator()
  self.assertIs(type(p.channel),Channel);self.assertEqual(w.frames,[])
  j=await p.prepare(co,res,self.root/'recv')
  self.assertIs(j,p.journal);self.assertEqual(p.state,'ready');self.assertEqual([x[:1] for x in w.frames],[b'H'])
  with self.assertRaises(PermissionError):
   with j._execution(None):pass
  self.assertEqual(co.query(self.sid)['claimed_stages'],(1,))
 async def test_blocked_preparation_H0_and_D_rejected_before_payload(self):
  p,r,w=await self.assembly();co,res=self.coordinator();entered=asyncio.Event();release=asyncio.Event()
  async def barrier():entered.set();await release.wait()
  task=asyncio.create_task(p.prepare(co,res,self.root/'recv',prepare_fence=barrier));await entered.wait()
  self.assertEqual(p.state,'preparing');self.assertEqual(w.frames,[])
  self.assertEqual(co.query(self.sid)['claimed_stages'],(1,))
  wire=self.envelope().encode();r.feed(b'D'+struct.pack('!I',len(wire))+wire)
  await asyncio.wait_for(w.closed_event.wait(),1)
  self.assertEqual(r.reads,[5]);self.assertTrue(p.channel._queue.empty());self.assertEqual(w.frames,[])
  release.set()
  with self.assertRaises((ChannelError,PermissionError)):await task
  with sqlite3.connect(self.root/'recv') as db:self.assertEqual(db.execute('SELECT count(*) FROM p1_v2_receipts').fetchone()[0],0)
 async def test_failure_burns_slot_and_closes_no_H(self):
  p,r,w=await self.assembly();co,res=self.coordinator()
  async def fail():raise RuntimeError('offline preparation failure')
  with self.assertRaises(RuntimeError):await p.prepare(co,res,self.root/'recv',prepare_fence=fail)
  self.assertTrue(w.closed);self.assertEqual(w.frames,[]);self.assertEqual(p.state,'closed')
  other,_,ow=await self.assembly()
  with self.assertRaises(Conflict):await other.prepare(co,res,self.root/'retry')
  self.assertTrue(ow.closed);self.assertEqual(ow.frames,[])
 async def test_cancellation_burns_slot_closes(self):
  p,r,w=await self.assembly();co,res=self.coordinator();entered=asyncio.Event()
  async def barrier():entered.set();await asyncio.Future()
  task=asyncio.create_task(p.prepare(co,res,self.root/'recv',prepare_fence=barrier));await entered.wait();task.cancel()
  with self.assertRaises(asyncio.CancelledError):await task
  self.assertTrue(w.closed);self.assertEqual(w.frames,[]);self.assertEqual(co.query(self.sid)['claimed_stages'],(1,))
 async def test_repeat_prepare_and_start_do_not_reissue_H(self):
  p,r,w=await self.assembly();co,res=self.coordinator()
  with self.assertRaises(ChannelError):await p.channel.start()
  await p.prepare(co,res,self.root/'recv')
  with self.assertRaises(ValueError):await p.prepare(co,res,self.root/'other')
  with self.assertRaises(ChannelError):await p.channel.start()
  self.assertEqual([x[:1] for x in w.frames],[b'H'])
 async def test_concurrent_prepare_one_slot_winner(self):
  co,res=self.coordinator();p,_,w=await self.assembly();q,_,z=await self.assembly();entered=asyncio.Event();release=asyncio.Event()
  async def barrier():entered.set();await release.wait()
  a=asyncio.create_task(p.prepare(co,res,self.root/'first',prepare_fence=barrier));await entered.wait()
  with self.assertRaises(Conflict):await q.prepare(co,res,self.root/'second')
  release.set();await a
  self.assertEqual([x[:1] for x in w.frames],[b'H']);self.assertEqual(z.frames,[])
 async def test_concurrent_same_assembly_only_first_runs(self):
  p,_,w=await self.assembly();co,res=self.coordinator();entered=asyncio.Event();release=asyncio.Event()
  async def barrier():entered.set();await release.wait()
  a=asyncio.create_task(p.prepare(co,res,self.root/'first',prepare_fence=barrier));await entered.wait()
  with self.assertRaises(ValueError):await p.prepare(co,res,self.root/'second')
  self.assertFalse(w.closed);release.set();await a;self.assertEqual(len(w.frames),1)
 async def test_bidirectional_prepare_never_waits_for_peer_data(self):
  ra,rb=Reader(),Reader();wa,wb=Writer(rb),Writer(ra)
  a,_,_=await self.assembly(ra,wa);b,_,_=await self.assembly(rb,wb);ca,r1=self.coordinator('a');cb,r2=self.coordinator('b')
  entered_a,entered_b,release=asyncio.Event(),asyncio.Event(),asyncio.Event()
  async def ba():entered_a.set();await release.wait()
  async def bb():entered_b.set();await release.wait()
  ta=asyncio.create_task(a.prepare(ca,r1,self.root/'a',prepare_fence=ba));tb=asyncio.create_task(b.prepare(cb,r2,self.root/'b',prepare_fence=bb))
  await asyncio.wait_for(asyncio.gather(entered_a.wait(),entered_b.wait()),1);self.assertEqual(wa.frames+wb.frames,[])
  release.set();await asyncio.wait_for(asyncio.gather(ta,tb),1)
  self.assertEqual([x[:1] for x in wa.frames],[b'H']);self.assertEqual([x[:1] for x in wb.frames],[b'H'])
 async def test_fence_return_not_authority_registry_closed(self):
  p,_,w=await self.assembly();co,res=self.coordinator()
  async def barrier():co.close();return True
  with self.assertRaises(Conflict):await p.prepare(co,res,self.root/'recv',prepare_fence=barrier)
  self.assertEqual(w.frames,[]);self.assertTrue(w.closed)
 async def test_context_tamper_after_factory_no_H(self):
  p,_,w=await self.assembly();co,res=self.coordinator()
  async def barrier():
   with sqlite3.connect(self.root/'recv') as db:db.execute("UPDATE p1_receipt_context SET body='{}'")
  with self.assertRaises(Conflict):await p.prepare(co,res,self.root/'recv',prepare_fence=barrier)
  self.assertTrue(w.closed);self.assertEqual(w.frames,[])
 async def test_D_header_latches_refusal_before_withheld_body(self):
  p,r,w=await self.assembly();co,res=self.coordinator();entered=asyncio.Event();release=asyncio.Event()
  async def barrier():entered.set();await release.wait()
  task=asyncio.create_task(p.prepare(co,res,self.root/'recv',prepare_fence=barrier));await entered.wait()
  wire=self.envelope().encode();r.feed(b'D'+struct.pack('!I',len(wire)))
  try:
   try:await asyncio.wait_for(w.closed_event.wait(),0.2)
   except TimeoutError:
    self.assertEqual(r.reads,[5],'D gate improperly entered payload read while unprepared')
    raise
   release.set()
   with self.assertRaises((ChannelError,PermissionError)):await task
   r.feed(wire)
   self.assertEqual(r.reads,[5]);self.assertEqual(w.frames,[]);self.assertTrue(p.channel._queue.empty())
  finally:
   release.set()
   if not task.done():task.cancel()
   await asyncio.gather(task,return_exceptions=True)
 async def test_concurrent_start_while_H_write_lock_pending(self):
  p,_,w=await self.assembly();co,res=self.coordinator();blocked=asyncio.Event()
  lock=asyncio.Lock();await lock.acquire()
  class ObservedLock:
   async def __aenter__(self):blocked.set();await lock.acquire()
   async def __aexit__(self,*args):lock.release()
  p.channel._write=ObservedLock()
  task=asyncio.create_task(p.prepare(co,res,self.root/'recv'))
  await blocked.wait()
  with self.assertRaises(ChannelError):await p.channel.start()
  lock.release();await task;self.assertEqual([x[:1] for x in w.frames],[b'H'])
 async def test_cancel_start_waiting_write_lock_retires(self):
  p,_,w=await self.assembly();co,res=self.coordinator();blocked=asyncio.Event()
  lock=asyncio.Lock();await lock.acquire()
  class ObservedLock:
   async def __aenter__(self):blocked.set();await lock.acquire()
   async def __aexit__(self,*args):lock.release()
  p.channel._write=ObservedLock()
  task=asyncio.create_task(p.prepare(co,res,self.root/'recv'));await blocked.wait();task.cancel()
  try:
   with self.assertRaises(asyncio.CancelledError):await task
  finally:lock.release()
  self.assertEqual(p.state,'closed');self.assertEqual(w.frames,[]);self.assertEqual(co.query(self.sid)['claimed_stages'],(1,))
 async def test_H_drain_failure_retires_without_retry(self):
  p,_,w=await self.assembly();co,res=self.coordinator()
  async def fail():raise ConnectionResetError('fake drain')
  w.drain=fail
  with self.assertRaises(ConnectionResetError):await p.prepare(co,res,self.root/'recv')
  self.assertEqual(p.state,'closed');self.assertEqual([x[:1] for x in w.frames],[b'H'])
  with self.assertRaises(ChannelError):await p.channel.start()
  self.assertEqual(co.query(self.sid)['claimed_stages'],(1,))
 async def test_immediate_D_on_H_write_is_receipt_ready(self):
  p,r,w=await self.assembly();co,res=self.coordinator();wire=self.envelope().encode();old=w.write;states=[]
  def write(data):
   old(data)
   if data[:1]==b'H':states.append(p.state);r.feed(b'D'+struct.pack('!I',len(wire))+wire)
  w.write=write
  j=await p.prepare(co,res,self.root/'recv')
  result=await p.bound.receive_once(j)
  self.assertEqual(states,['ready']);self.assertFalse(result['execution_allowed'])
  self.assertEqual([x[:1] for x in w.frames],[b'H',b'C'])
  with sqlite3.connect(j._path) as db:
   self.assertEqual(db.execute('SELECT count(*) FROM p1_v2_receipts').fetchone()[0],1)
   self.assertEqual(db.execute('SELECT count(*) FROM p1_local_attempts').fetchone()[0],0)
 async def test_EOF_during_preparation_retires_before_H(self):
  p,r,w=await self.assembly();co,res=self.coordinator();entered=asyncio.Event();release=asyncio.Event()
  async def barrier():entered.set();await release.wait()
  task=asyncio.create_task(p.prepare(co,res,self.root/'recv',prepare_fence=barrier));await entered.wait();r.stream.feed_eof()
  await w.closed_event.wait();release.set()
  with self.assertRaises((ChannelError,PermissionError)):await task
  self.assertEqual(w.frames,[]);self.assertEqual(p.state,'closed')
 async def test_timeout_during_preparation_retires_before_H(self):
  p,r,w=await self.assembly();co,res=self.coordinator();entered=asyncio.Event();release=asyncio.Event();timeout_trigger=asyncio.Event()
  async def timed_read(n):await timeout_trigger.wait();raise TimeoutError('fake scheduled timeout')
  r.readexactly=timed_read
  async def barrier():entered.set();await release.wait()
  task=asyncio.create_task(p.prepare(co,res,self.root/'recv',prepare_fence=barrier));await entered.wait();timeout_trigger.set()
  await w.closed_event.wait();release.set()
  with self.assertRaises((ChannelError,PermissionError)):await task
  self.assertEqual(w.frames,[]);self.assertEqual(p.state,'closed')
 async def test_binding_failure_closes_new_channel(self):
  r,w=Reader(),Writer();bad=TLSConfig('unused','unused','unused',{'invalid peer':self.fp});baseline=asyncio.all_tasks()
  try:
   with self.assertRaises(ValueError):await PreparedReceiptConnection.create(r,w,bad,{})
   self.assertTrue(w.closed,'binding failure leaked authenticated writer')
   await asyncio.sleep(0)
   self.assertFalse([t for t in asyncio.all_tasks()-baseline if not t.done()])
  finally:
   tasks=asyncio.all_tasks()-baseline
   for t in tasks:t.cancel()
   await asyncio.gather(*tasks,return_exceptions=True)
 async def test_private_capacity_defaults_and_oversize_preflight(self):
  r,w=Reader(),Writer();p=await PreparedReceiptConnection.create(r,w,self.cfg,self.grants)
  self.prepared.append(p);self.channels.append(p.channel)
  self.assertEqual((p.channel.capacity_bytes,p.channel.capacity_tiles),(65536,1))
  co,res=self.coordinator();await p.prepare(co,res,self.root/'recv')
  r.feed(b'D'+struct.pack('!I',65537))
  await w.closed_event.wait();self.assertEqual(r.reads,[5]);self.assertTrue(p.channel._queue.empty())
 async def test_private_capacity_overrides_cannot_expand(self):
  for opts in ({'capacity_bytes':65537},{'capacity_tiles':2},{'capacity_bytes':True}):
   w=Writer()
   with self.assertRaises(ValueError):await PreparedReceiptConnection.create(Reader(),w,self.cfg,self.grants,**opts)
   self.assertTrue(w.closed);self.assertEqual(w.frames,[])
 async def test_H_write_failure_retires(self):
  p,_,w=await self.assembly();co,res=self.coordinator()
  def fail(data):raise BrokenPipeError('fake write')
  w.write=fail
  with self.assertRaises(BrokenPipeError):await p.prepare(co,res,self.root/'recv')
  self.assertEqual(p.state,'closed');self.assertEqual(w.frames,[]);self.assertEqual(co.query(self.sid)['claimed_stages'],(1,))
 async def test_cancel_H_drain_unknown_advertisement_not_retried(self):
  p,_,w=await self.assembly();co,res=self.coordinator();entered=asyncio.Event()
  async def drain():entered.set();await asyncio.Future()
  w.drain=drain;task=asyncio.create_task(p.prepare(co,res,self.root/'recv'));await entered.wait();task.cancel()
  with self.assertRaises(asyncio.CancelledError):await task
  self.assertEqual(p.state,'closed');self.assertEqual([x[:1] for x in w.frames],[b'H'])
  with self.assertRaises(ChannelError):await p.channel.start()
 async def test_real_factory_failure_after_claim_no_H(self):
  from cluster_p1.bound_connection import BoundReceiptConnection
  p,_,w=await self.assembly();co,res=self.coordinator()
  with patch.object(BoundReceiptConnection,'create_journal',side_effect=RuntimeError('fake storage failure')):
   with self.assertRaises(RuntimeError):await p.prepare(co,res,self.root/'recv')
  self.assertEqual(co.query(self.sid)['claimed_stages'],(1,));self.assertEqual(w.frames,[]);self.assertTrue(w.closed)
 async def test_bad_TLS_factory_closes_owned_writer(self):
  w=Writer();w.context.verify_mode=ssl.CERT_NONE
  with self.assertRaises(ChannelError):await PreparedReceiptConnection.create(Reader(),w,self.cfg,self.grants)
  self.assertTrue(w.closed);self.assertEqual(w.frames,[])
 async def test_default_accept_and_repeat_start_unchanged(self):
  r,w=Reader(),Writer();c=await accept(r,w,self.cfg,self.grants);self.channels.append(c);await c.start()
  self.assertEqual([x[:1] for x in w.frames],[b'H',b'H'])
 async def test_default_connect_unchanged_without_socket(self):
  r,w=Reader(),Writer()
  with patch('cluster_p1.channel.asyncio.open_connection',return_value=(r,w)),patch.object(TLSConfig,'context',return_value=object()):
   c=await connect('unused',0,self.cfg,self.grants,server_hostname='unused');self.channels.append(c)
  self.assertEqual([x[:1] for x in w.frames],[b'H'])
if __name__=='__main__':unittest.main(verbosity=2)
