"""SSL-object stand-ins only, no certificates, sockets or actual handshake."""
import asyncio,hashlib,ssl,tempfile,unittest,struct
from pathlib import Path
from contextlib import contextmanager
from types import SimpleNamespace
from cluster_p1.channel import Channel,TLSConfig,Grant,ChannelError,Delivery
from cluster_p1.handoff import EdgeBinding
from cluster_p1.envelope import Envelope
from test_p1_tile_contract_v2 import fixture
try:from cluster_p1.bound_connection import BoundReceiptConnection
except ImportError:BoundReceiptConnection=None
@contextmanager
def auth(*a):yield 'owner'
class Reader:
 async def readexactly(self,n):await asyncio.Future()
class Writer:
 def __init__(self,cert=b'cert-A',mode=ssl.CERT_REQUIRED,hostname=True):
  self.closed=False;self.cert=cert;self.context=SimpleNamespace(verify_mode=mode,check_hostname=hostname)
 def get_extra_info(self,k):return self
 @property
 def server_side(self):return False
 def version(self):return 'TLSv1.3'
 def getpeercert(self,binary_form=False):return self.cert
 def is_closing(self):return self.closed
 def close(self):self.closed=True
 async def wait_closed(self):pass
 def write(self,b):pass
 async def drain(self):pass
class Tests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.assertIsNotNone(BoundReceiptConnection,'connection-bound receipt seam missing')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.sid='1'*32;self.fp=hashlib.sha256(b'cert-A').hexdigest();self.channels=[]
  t,s,st,tiles=fixture();s=s.model_copy(update={'session_id':self.sid})
  st=tuple(x.model_copy(update={'session_id':self.sid}) for x in st)
  self.args=(t,s,st,tuple(dict(x,session_id=self.sid) for x in tiles))
 async def asyncTearDown(self):
  for c in getattr(self,'channels',[]):await c.close()
 def channel(self,**kwargs):
  w=Writer(**kwargs);cfg=TLSConfig('not-read','not-read','not-read',{'a':self.fp})
  c=Channel(Reader(),w,cfg,{'a':{Grant('t',self.sid,1,0,(0,5),'a'*64,2)}})
  self.channels.append(c);return c
 def frame(self,k=0):
  s,n=((0,2),(2,2),(4,1))[k]
  return Envelope('t',self.sid,1,0,k,(s,s+n),'a'*64,2,(1,n,24),'float32',struct.pack('<f',1.)*(n*24))
 def setup_journal(self,c,name='b'):
  from cluster_p1.local_attempt import LocalAttemptJournal
  t,s,st,tiles=self.args
  a=LocalAttemptJournal.create(Path(self.tmp.name)/(name+'-plan'),t,s,st,authorize=auth,execution_authorize=auth)
  self.addCleanup(a.close);p=a.register(tiles,hidden_size=24,vocab_size=32,event_id='plan')
  edge=EdgeBinding('t',self.sid,1,p['plan_digest'],'a'*64,2,'a','b')
  bound=BoundReceiptConnection(c)
  j=bound.create_journal(Path(self.tmp.name)/name,t,s,st,authorize=auth,execution_authorize=auth,
       receipt_binding=edge,local_node='b',expected_config_sha256='c'*64)
  self.addCleanup(j.close);j.register(tiles,hidden_size=24,vocab_size=32,event_id='plan')
  return bound,j
 def queue(self,c,k=0):
  e=self.frame(k);c._used_bytes+=len(e.payload);c._used_tiles+=1
  c._queue.put_nowait(Delivery(c,e,len(e.payload)))
 async def test_same_connection_real_receipt_and_release(self):
  c=self.channel();b,j=self.setup_journal(c);self.queue(c)
  result=await b.receive_once(j)
  self.assertFalse(result['execution_allowed']);self.assertEqual(c._used_tiles,0)
  self.assertEqual(j._receipt_peer.certificate_sha256,self.fp)
 async def test_mutable_peer_node_not_identity(self):
  c=self.channel();c.peer_node='spoof'
  b=BoundReceiptConnection(c);self.assertEqual(b.expected_peer.node_id,'a')
 async def test_revoke_before_receive_rejects_queued(self):
  c=self.channel();b,j=self.setup_journal(c);self.queue(c);c.revoke_identity()
  with self.assertRaises((PermissionError,ChannelError)):await b.receive_once(j)
 async def test_closed_before_receive_rejects_queued(self):
  c=self.channel();b,j=self.setup_journal(c);self.queue(c);await c.close()
  with self.assertRaises((PermissionError,ChannelError)):await b.receive_once(j)
 async def test_other_connection_journal_refused(self):
  c=self.channel();b,j=self.setup_journal(c);other=BoundReceiptConnection(self.channel())
  self.assertNotEqual(other.expected_peer.connection_generation,b.expected_peer.connection_generation)
  with self.assertRaises(ValueError):await other.receive_once(j)
 async def test_foreign_delivery_refused(self):
  c=self.channel();b,j=self.setup_journal(c);other=self.channel();e=self.frame()
  c._queue.put_nowait(Delivery(other,e,len(e.payload)))
  with self.assertRaises(ValueError):await b.receive_once(j)
 async def test_writer_close_observed_by_fence(self):
  c=self.channel();b,j=self.setup_journal(c);c.w.close()
  with self.assertRaises(PermissionError):
   with b.peer_authorize(b.expected_peer):pass
 async def test_bad_tls_metadata_rejected(self):
  for kw in ({'cert':None},{'cert':b'other'},{'mode':ssl.CERT_NONE},{'hostname':False}):
   with self.assertRaises(ChannelError):self.channel(**kw)
 async def test_same_connection_cannot_rebind_adapter(self):
  c=self.channel();BoundReceiptConnection(c)
  with self.assertRaises(ValueError):BoundReceiptConnection(c)
 async def test_journal_creation_attempt_is_one_shot(self):
  c=self.channel();b,j=self.setup_journal(c)
  with self.assertRaises(ValueError):b.create_journal(Path(self.tmp.name)/'second',*self.args[:3],authorize=auth,
   execution_authorize=auth,receipt_binding=j._receipt_edge,local_node='b',expected_config_sha256='c'*64)
 async def test_close_between_dequeue_and_receipt_blocks_write(self):
  import sqlite3
  c=self.channel();b,j=self.setup_journal(c);self.queue(c);original=c.receive
  async def receive():
   d=await original();c.revoke_identity();return d
  c.receive=receive
  with self.assertRaises(BaseException):await b.receive_once(j)
  with sqlite3.connect(j._path) as db:self.assertEqual(db.execute('select count(*) from p1_v2_receipts').fetchone()[0],0)
 async def test_cancel_pending_receive_retires_connection(self):
  c=self.channel();b,j=self.setup_journal(c);started=asyncio.Event()
  async def receive():started.set();await asyncio.Future()
  c.receive=receive;t=asyncio.create_task(b.receive_once(j));await started.wait();t.cancel()
  with self.assertRaises(asyncio.CancelledError):await t
  self.assertTrue(c._closed)
  with self.assertRaises(PermissionError):
   with b.peer_authorize(b.expected_peer):pass
 async def test_oversize_delivery_constructor_retires_stream(self):
  c=self.channel();b,j=self.setup_journal(c)
  e=Envelope('t',self.sid,1,0,0,(0,1),'a'*64,2,(65536,),'uint8',b'x'*65536)
  c._used_bytes=65536;c._used_tiles=1;c._queue.put_nowait(Delivery(c,e,65536))
  with self.assertRaises(ValueError):await b.receive_once(j)
  self.assertTrue(c._closed)
 async def test_revocation_after_owner_bind_refuses_operation(self):
  c=self.channel();b,j=self.setup_journal(c)
  for k in range(3):self.queue(c,k);await b.receive_once(j)
  calls=[];o=j.make_stage1_owner(lambda v,**kw:calls.append(v) or b'\0'*256,instance_id='down')
  c.revoke_identity()
  try:
   with self.assertRaises(PermissionError):await o.call(self.frame().payload,token_start=0,token_count=2)
  finally:await o.close()
  self.assertEqual(calls,[])
 async def test_receive_and_owner_binding_share_lock_order(self):
  # Real journal/receiver/owner code, real two-thread locks. A nonblocking
  # diagnostic at the second edge exposes the cycle instead of hanging tests.
  import threading
  from cluster_p1.receipt_attempt import ReceiptAttemptJournal,PeerIdentity
  c=self.channel();bound,base=self.setup_journal(c)
  peer=PeerIdentity('a',self.fp,'thread-test');plock=threading.RLock()
  held=threading.Event();attempt=threading.Event();proceed=threading.Event();errors=[];cycle=[]
  @contextmanager
  def provider(expected):
   if threading.current_thread().name=='receiver-thread':
    if not plock.acquire(blocking=False):
     cycle.append('journal->peer while peer->journal');raise RuntimeError('lock inversion observed')
   else:plock.acquire()
   try:yield peer
   finally:plock.release()
  j=ReceiptAttemptJournal.create(Path(self.tmp.name)/'lock-test',*self.args[:3],authorize=auth,
    execution_authorize=auth,receipt_binding=base._receipt_edge,expected_peer=peer,peer_authorize=provider,
    local_node='b',expected_config_sha256='c'*64,enable_receipt_stage1=True)
  self.addCleanup(j.close);j.register(self.args[3],hidden_size=24,vocab_size=32,event_id='plan')
  receiver=j.make_receiver()
  for k in range(3):receiver.receive_bytes(self.frame(k).encode())
  real=j._lock
  class Gate:
   def __enter__(self):
    name=threading.current_thread().name
    if name=='owner-thread':attempt.set()
    real.acquire()
    if name=='receiver-thread':held.set();proceed.wait()
    return self
   def __exit__(self,*exc):real.release()
  j._lock=Gate()
  def receive():
   try:receiver.receive_bytes(self.frame().encode())
   except BaseException as e:errors.append(e)
  def owner():
   async def run():
    o=j.make_stage1_owner(lambda v,**kw:b'\0'*256,instance_id='ordinary');await o.close()
   try:asyncio.run(run())
   except BaseException as e:errors.append(e)
  a=threading.Thread(target=receive,name='receiver-thread',daemon=True)
  b=threading.Thread(target=owner,name='owner-thread',daemon=True)
  a.start();self.assertTrue(held.wait(2));b.start()
  try:self.assertTrue(attempt.wait(2))
  finally:proceed.set()
  a.join(2);b.join(2)
  self.assertFalse(a.is_alive() or b.is_alive(),'test safety join, not a lock-order remedy')
  self.assertEqual(cycle,[]);self.assertEqual(errors,[])
 async def test_bootstrap_never_acquires_owner_under_peer(self):
  from cluster_p1.receipt_attempt import ReceiptAttemptJournal,PeerIdentity
  c=self.channel();bound,base=self.setup_journal(c);peer=PeerIdentity('a',self.fp,'init-order')
  state={'peer':False};seen=[]
  @contextmanager
  def owner(*args):
   self.assertFalse(state['peer'],'owner acquired under peer');seen.append('owner');yield 'owner'
  @contextmanager
  def provider(expected):
   state['peer']=True;seen.append('peer')
   try:yield peer
   finally:state['peer']=False
  j=ReceiptAttemptJournal.create(Path(self.tmp.name)/'init-order',*self.args[:3],authorize=owner,
    execution_authorize=owner,receipt_binding=base._receipt_edge,expected_peer=peer,peer_authorize=provider,
    local_node='b',expected_config_sha256='c'*64,enable_receipt_stage1=True)
  self.addCleanup(j.close);self.assertIn('owner',seen);self.assertIn('peer',seen)
 async def test_cross_thread_revocation_is_scheduled_not_inverse_wait(self):
  import threading
  c=self.channel();b=BoundReceiptConnection(c);loop=asyncio.get_running_loop();scheduled=threading.Event()
  def revoke():loop.call_soon_threadsafe(c.revoke_identity);scheduled.set()
  with b.peer_authorize(b.expected_peer):
   t=threading.Thread(target=revoke);t.start();self.assertTrue(scheduled.wait(2));t.join()
   self.assertFalse(c._closed)
  await asyncio.sleep(0)
  self.assertTrue(c._closed)
  with self.assertRaises(PermissionError):
   with b.peer_authorize(b.expected_peer):pass
