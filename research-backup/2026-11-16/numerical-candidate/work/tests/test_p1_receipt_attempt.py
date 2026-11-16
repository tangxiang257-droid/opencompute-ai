"""Ordinary-data contract tests; test peer contexts are NOT TLS authentication."""
import hashlib,sqlite3,struct,tempfile,unittest
from pathlib import Path
from contextlib import contextmanager
from dataclasses import replace
from unittest.mock import patch
from test_p1_tile_contract_v2 import fixture
from cluster_p1.local_attempt import LocalAttemptJournal
from cluster_p1.handoff import EdgeBinding
from cluster_p1.envelope import Envelope
from cluster_p1.store import Conflict
try:
 from cluster_p1.receipt_attempt import ReceiptAttemptJournal,PeerIdentity
except ImportError:
 ReceiptAttemptJournal=PeerIdentity=None
@contextmanager
def auth(*args):yield 'owner'
class Tests(unittest.IsolatedAsyncioTestCase):
 def setUp(self):
  self.assertIsNotNone(ReceiptAttemptJournal,'receipt-bound execution adapter missing')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);p=Path(self.tmp.name)
  t,s,st,tiles=fixture();sid='1'*32;s=s.model_copy(update={'session_id':sid})
  st=tuple(x.model_copy(update={'session_id':sid}) for x in st)
  tiles=tuple(dict(x,session_id=sid) for x in tiles);self.args=(t,s,st,tiles)
  self.a=LocalAttemptJournal.create(p/'a',t,s,st,authorize=auth,execution_authorize=auth)
  self.addCleanup(self.a.close);r=self.a.register(tiles,hidden_size=24,vocab_size=32,event_id='plan')
  self.edge=EdgeBinding('t',sid,1,r['plan_digest'],'a'*64,2,'a','b')
  self.peer=PeerIdentity('a','b'*64,'connection-1');self.revoked=False
  @contextmanager
  def peer(expected):
   if self.revoked:raise PermissionError('revoked')
   yield self.peer
  self.path=p/'b';self.b=ReceiptAttemptJournal.create(self.path,t,s,st,authorize=auth,execution_authorize=auth,
    receipt_binding=self.edge,expected_peer=self.peer,peer_authorize=peer,local_node='b',
    expected_config_sha256='c'*64,enable_receipt_stage1=True)
  self.addCleanup(self.b.close);self.b.register(tiles,hidden_size=24,vocab_size=32,event_id='plan')
  self.r=self.b.make_receiver();self.calls=[]
 async def upstream(self):
  inputs=(struct.pack('<2q',1,2),struct.pack('<2q',3,4),struct.pack('<q',5))
  a=self.a.make_owner(lambda v,**kw:struct.pack('<f',1.)*(len(v)//8*24),inputs,instance_id='up')
  try:
   for v,s,n in zip(inputs,(0,2,4),(2,2,1)):await a.call(v,token_start=s,token_count=n)
  finally:await a.close()
  return [Envelope('t',self.edge.session_id,1,0,k,(s,s+n),'a'*64,2,(1,n,24),'float32',self.a.query(k)['output']).encode() for k,(s,n) in enumerate(((0,2),(2,2),(4,1)))]
 def owner(self):
  def op(v,**kw):self.calls.append(v);return b'\0'*(len(v)//24*32)
  return self.b.make_stage1_owner(op,instance_id='down')
 async def test_two_ledgers_complete_serial_source(self):
  frames=await self.upstream()
  for f in frames:self.assertFalse(self.r.receive_bytes(f)['execution_allowed'])
  o=self.owner()
  try:
   for k,(s,n) in enumerate(((0,2),(2,2),(4,1))):await o.call(self.a.query(k)['output'],token_start=s,token_count=n)
  finally:await o.close()
  self.assertEqual(len(self.calls),3)
  with sqlite3.connect(self.path) as c:self.assertEqual(c.execute('select count(*) from p1_local_attempts').fetchone()[0],0)
  self.assertEqual(self.b.query(2,stage_id=1)['state'],'OUTPUT_COMMITTED')
 async def test_duplicate_receipt_no_extra_storage_or_permission(self):
  f=(await self.upstream())[0];self.r.receive_bytes(f)
  with sqlite3.connect(self.path) as c:before=c.execute('select count(*) from p1_received_frames').fetchone()
  self.assertTrue(self.r.receive_bytes(f)['duplicate'])
  with sqlite3.connect(self.path) as c:self.assertEqual(c.execute('select count(*) from p1_received_frames').fetchone(),before)
 async def test_missing_source_blocks_owner(self):
  with self.assertRaises(Conflict):self.owner()
 async def test_payload_insert_failure_rolls_back_receipt(self):
  f=(await self.upstream())[0]
  with patch.object(self.b,'_retain_frame',side_effect=RuntimeError('retention fail')):
   with self.assertRaises(RuntimeError):self.r.receive_bytes(f)
  with sqlite3.connect(self.path) as c:
   self.assertEqual(c.execute('select count(*) from p1_v2_receipts').fetchone()[0],0)
   self.assertEqual(c.execute('select count(*) from p1_received_frames').fetchone()[0],0)
 async def test_wrong_peer_and_live_revocation(self):
  f=(await self.upstream())[0];self.peer=replace(self.peer,certificate_sha256='d'*64)
  with self.assertRaises(PermissionError):self.r.receive_bytes(f)
 async def test_revoked_after_binding_blocks_execution(self):
  for f in await self.upstream():self.r.receive_bytes(f)
  o=self.owner();self.revoked=True
  try:
   with self.assertRaises(PermissionError):await o.call(self.a.query(0)['output'],token_start=0,token_count=2)
  finally:await o.close()
  self.assertEqual(self.calls,[]);self.assertIsNone(self.b.query(0,stage_id=1))
 async def test_modified_frame_after_binding_blocks_execution(self):
  for f in await self.upstream():self.r.receive_bytes(f)
  o=self.owner()
  with sqlite3.connect(self.path) as c:c.execute("update p1_received_frames set frame=? where tile=0",(b'bad',))
  try:
   with self.assertRaises(ValueError):await o.call(self.a.query(0)['output'],token_start=0,token_count=2)
  finally:await o.close()
  self.assertEqual(self.calls,[])
 async def test_stage0_execution_disabled_on_receiver(self):
  with self.assertRaises(Conflict):self.b.make_owner(lambda x,**kw:x,(),instance_id='x')
 async def test_duplicate_missing_payload_cannot_repair(self):
  f=(await self.upstream())[0];self.r.receive_bytes(f)
  with sqlite3.connect(self.path) as c:c.execute('delete from p1_received_frames')
  with self.assertRaises(Conflict):self.r.receive_bytes(f)
 async def test_changed_content_same_tile_rejected(self):
  from cluster_p1.envelope import decode
  f=(await self.upstream())[0];self.r.receive_bytes(f);e=decode(f)
  with self.assertRaises(Conflict):self.r.receive_bytes(replace(e,payload=b'\0'*len(e.payload)).encode())
 async def test_old_epoch_and_other_nonce_rejected(self):
  from cluster_p1.envelope import decode
  e=decode((await self.upstream())[0])
  for changes in ({'execution_epoch':2},{'session_id':'2'*32}):
   with self.assertRaises(ValueError):self.r.receive_bytes(replace(e,**changes).encode())
 async def test_receipt_budget_failure_rolls_back_both(self):
  f=(await self.upstream())[0]
  with patch.object(self.b,'_used',return_value=262144):
   with self.assertRaises(Conflict):self.r.receive_bytes(f)
  with sqlite3.connect(self.path) as c:
   self.assertEqual(c.execute('select count(*) from p1_v2_receipts').fetchone()[0],0)
   self.assertEqual(c.execute('select count(*) from p1_received_frames').fetchone()[0],0)
 async def test_context_mutation_blocks_binding(self):
  for f in await self.upstream():self.r.receive_bytes(f)
  with sqlite3.connect(self.path) as c:c.execute("update p1_receipt_context set body='{}'")
  with self.assertRaises(Conflict):self.owner()
 async def test_execution_revoked_during_body_blocks_publication(self):
  for f in await self.upstream():self.r.receive_bytes(f)
  def op(v,**kw):self.revoked=True;return b'\0'*256
  o=self.b.make_stage1_owner(op,instance_id='down')
  try:
   with self.assertRaises(PermissionError):await o.call(self.a.query(0)['output'],token_start=0,token_count=2)
  finally:await o.close()
  self.assertEqual(self.b.query(0,stage_id=1)['state'],'ADMITTED')
  self.revoked=False
  with self.assertRaises(Conflict):self.owner()
 async def test_plain_string_is_not_peer_context(self):
  f=(await self.upstream())[0];self.peer='a'
  with self.assertRaises(PermissionError):self.r.receive_bytes(f)
 async def test_opt_in_required_before_file_creation(self):
  p=self.path.parent/'disabled'
  with self.assertRaises(Conflict):ReceiptAttemptJournal.create(p,*self.args[:3],authorize=auth,execution_authorize=auth,
    receipt_binding=self.edge,expected_peer=self.peer,peer_authorize=auth,local_node='b',expected_config_sha256='c'*64)
  self.assertFalse(p.exists())
 async def test_receiver_default_remains_receipt_only(self):
  from cluster_p1.handoff import Receiver
  @contextmanager
  def peer():yield 'a'
  r=Receiver(self.b,self.edge,authenticate_peer=peer,input_layout_version=2,input_layout_wire_version=2)
  r.receive_bytes((await self.upstream())[0])
  with sqlite3.connect(self.path) as c:self.assertEqual(c.execute('select count(*) from p1_received_frames').fetchone()[0],0)
  with self.assertRaises(Conflict):self.owner()
 async def test_delivery_receipt_then_separate_stage_owner(self):
  from cluster_p1.channel import Delivery
  from cluster_p1.envelope import decode
  from cluster_p1.delivery_receipt import DeliveryAttempt
  class Sink:
   def __init__(self):self.released=[]
   async def _release(self,n):self.released.append(n)
  sink=Sink()
  for f in await self.upstream():
   e=decode(f);a=DeliveryAttempt(self.r,Delivery(sink,e,len(e.payload)))
   await a.run();self.assertEqual(a.state.release,'completed');self.assertFalse(a.state.execution_allowed)
  self.assertEqual(sink.released,[192,192,96])
  o=self.owner()
  try:await o.call(self.a.query(0)['output'],token_start=0,token_count=2)
  finally:await o.close()
  self.assertEqual(len(self.calls),1)
 async def test_suppressed_peer_body_error_still_rejected(self):
  from cluster_p1.receipt_attempt import ReceiptAttemptJournal
  p=self.path.parent/'suppress'
  @contextmanager
  def peer(expected):
   try:yield expected
   except BaseException:pass
  j=ReceiptAttemptJournal.create(p,*self.args[:3],authorize=auth,execution_authorize=auth,
    receipt_binding=self.edge,expected_peer=self.peer,peer_authorize=peer,local_node='b',
    expected_config_sha256='c'*64,enable_receipt_stage1=True)
  self.addCleanup(j.close);j.register(self.args[3],hidden_size=24,vocab_size=32,event_id='plan')
  r=j.make_receiver();f=(await self.upstream())[0]
  with patch.object(j,'_retain_frame',side_effect=RuntimeError('must propagate')):
   with self.assertRaises(RuntimeError):r.receive_bytes(f)
  with sqlite3.connect(p) as c:self.assertEqual(c.execute('select count(*) from p1_v2_receipts').fetchone()[0],0)
 async def test_endpoint_swap_after_receipt_rejected(self):
  import json
  for f in await self.upstream():self.r.receive_bytes(f)
  with sqlite3.connect(self.path) as c:
   rows=c.execute('select stage_id,body from p1_stages').fetchall()
   for sid,body in rows:
    d=json.loads(body);d['assigned_node']='b' if sid==0 else 'a'
    c.execute('update p1_stages set body=? where stage_id=?',(json.dumps(d),sid))
  with self.assertRaises(Conflict):self.owner()
 async def test_layer_split_changed_after_owner_binding_refused(self):
  import json
  for f in await self.upstream():self.r.receive_bytes(f)
  o=self.owner()
  with sqlite3.connect(self.path) as c:
   for sid,body in c.execute('select stage_id,body from p1_stages').fetchall():
    d=json.loads(body)
    if sid==0:d['layer_end']=2
    else:d['layer_start']=2
    c.execute('update p1_stages set body=? where stage_id=?',(json.dumps(d),sid))
  try:
   with self.assertRaises(Conflict):await o.call(self.a.query(0)['output'],token_start=0,token_count=2)
  finally:await o.close()
  self.assertEqual(self.calls,[])
 async def test_orphan_frame_cannot_rebuild_receipt(self):
  f=(await self.upstream())[0];self.r.receive_bytes(f)
  with sqlite3.connect(self.path) as c:
   c.execute('delete from p1_v2_receipts')
   c.execute("delete from p1_events where id like 'v2:receipt:%'")
  with self.assertRaises(Conflict):self.r.receive_bytes(f)
  with sqlite3.connect(self.path) as c:
   self.assertEqual(c.execute('select count(*) from p1_v2_receipts').fetchone()[0],0)
   self.assertEqual(c.execute("select count(*) from p1_events where id like 'v2:receipt:%'").fetchone()[0],0)
 async def test_retention_receiver_cannot_substitute_peer_callback(self):
  from cluster_p1.handoff import Receiver
  @contextmanager
  def weak_peer():yield 'a'
  self.revoked=True
  with self.assertRaises(TypeError):Receiver(self.b,self.edge,authenticate_peer=weak_peer,
      input_layout_version=2,input_layout_wire_version=2,retain_payload=True)
