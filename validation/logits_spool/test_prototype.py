"""Only ordinary bytes, local files and SQLite. No model or network runtime."""
import dataclasses, hashlib, json, sqlite3, struct, tempfile, threading, unittest
from pathlib import Path
from unittest.mock import patch
try:
 from validation.logits_spool.prototype import Binding, Plan, Book, Spool, Rejected, CHUNK, MANIFEST
except ImportError:
 Binding=Plan=Book=Spool=None
 Rejected=ValueError;CHUNK=32768;MANIFEST=32768

class Tests(unittest.TestCase):
 def setUp(self):
  self.assertIsNotNone(Spool,'prototype missing')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
 def plan(self,counts=(2,),vocab=3,**changes):
  fields=dict(owner='owner',task='task',session_nonce='nonce',attempt='attempt',epoch=1,connection='generation',model='a'*64,layout='b'*64,input_snapshot='c'*64,token_digest='d'*64,preprocessing='e'*64,plan_digest='f'*64)
  fields.update(changes);return Plan(Binding(**fields),counts,vocab)
 def setup(self,p=None,cap=2*1024*1024):
  p=p or self.plan();s=Book(self.root/'source.db',cap);d=Book(self.root/'destination.db',cap)
  self.addCleanup(s.close);self.addCleanup(d.close)
  s.reserve('r',p,'source');d.reserve('r',p,'destination')
  return Spool(self.root/'spool',s,d,'r',p),s,d,p
 def body(self,p):
  def produce(write):
   for token in range(sum(p.counts)):
    for start in range(0,p.vocab,CHUNK//4):
     n=min(CHUNK//4,p.vocab-start)
     write(token,start,struct.pack('<'+'f'*n,*[float(token*10+i) for i in range(start,start+n)]))
  return produce
 def test_token_major_golden_and_no_column_major_permutation(self):
  x,s,d,p=self.setup();x.produce(self.body(p))
  frames=list(x.frames());self.assertEqual(b''.join(f.block for f in frames),struct.pack('<6f',0,1,2,10,11,12))
  self.assertNotEqual(struct.pack('<6f',0,1,10,11,2,12),b''.join(f.block for f in frames))
  self.assertEqual(s.record('r')['state'],'READING');self.assertEqual(d.record('r')['state'],'RESERVED')
 def test_reject_out_of_order_gap_overlap_and_wrong_tail(self):
  for bad in (lambda w:w(1,0,b'\0'*4),lambda w:w(0,1,b'\0'*4),lambda w:(w(0,0,b'\0'*4),w(0,0,b'\0'*4)),lambda w:w(0,0,b'\0'*16)):
   with self.subTest(bad=bad),tempfile.TemporaryDirectory() as tmp:
    old=self.root;self.root=Path(tmp)
    x,s,d,p=self.setup()
    with self.assertRaises(Rejected):x.produce(bad)
    self.assertEqual(s.record('r')['state'],'ISOLATED');self.assertEqual(s.used(),p.charge)
    self.root=old
 def test_producer_block_boundary_and_exact_bytes_only(self):
  for size in (32764,32768,32772):
   with self.subTest(size=size),tempfile.TemporaryDirectory() as tmp:
    old=self.root;self.root=Path(tmp);x,s,d,p=self.setup(self.plan((1,),size//4))
    if size<=32768:x.produce(lambda w:w(0,0,bytes(size)))
    else:
     with self.assertRaises(Rejected):x.produce(lambda w:w(0,0,bytes(size)))
    self.root=old
  x,s,d,p=self.setup()
  with self.assertRaises(Rejected):x.produce(lambda w:w(0,0,memoryview(bytes(12))))
 def test_nonfinite_and_incomplete_rejected(self):
  for raw in (struct.pack('<I',0x7f800000),struct.pack('<I',0xff800000),struct.pack('<I',0x7fc00001),b'\0'):
   with tempfile.TemporaryDirectory() as tmp:
    old=self.root;self.root=Path(tmp);x,s,d,p=self.setup()
    with self.assertRaises(Rejected):x.produce(lambda w:w(0,0,raw))
    self.root=old
  x,s,d,p=self.setup()
  with self.assertRaises(Rejected):x.produce(lambda w:None)
 def test_object_boundaries_three_tokens_56_vs_57_frames(self):
  for counts,expected in (((3,),56),((1,1,1),57)):
   with tempfile.TemporaryDirectory() as tmp:
    old=self.root;self.root=Path(tmp);x,s,d,p=self.setup(self.plan(counts,151936));x.produce(self.body(p));frames=list(x.frames())
    self.assertEqual(len(frames),expected);self.assertEqual(sum(len(f.block) for f in frames),1823232)
    for index in range(len(counts)):
     sizes=[len(f.block) for f in frames if f.object_index==index]
     self.assertTrue(all(n==32768 for n in sizes[:-1]))
    self.assertEqual(s.used(),1823232+MANIFEST);self.assertEqual(d.used(),1823232+MANIFEST)
    self.root=old
 def test_real_validator_rejects_token_tail_as_nonfinal_frame(self):
  from framed_result_transport import ObjectReceiver,ObjectContext,payload
  from isolated_tls_delivery import Denied
  from cluster_p1.envelope import Envelope
  from types import SimpleNamespace
  @dataclasses.dataclass(frozen=True)
  class Scope:
   task:str='task';session:str='nonce';epoch:int=1;peer_node:str='peer'
  @dataclasses.dataclass(frozen=True)
  class B:
   scope:Scope=Scope();stage_id:int=1;model_hash:str='a'*64;layout_version:int=2;tiles:tuple=((0,3,(1,3,151936)),)
  c=ObjectContext(B(),'b'*64,1823232,'c'*64,'generation')
  r=object.__new__(ObjectReceiver);r.context=c;r.written=18*32768;r.frames=18;r.check=lambda:None
  def delivery(block):
   return SimpleNamespace(envelope=Envelope('task','nonce',1,1,18,(0,3),'a'*64,2,(len(block)+48,),'uint8',payload(c,18,r.written,block)))
  with self.assertRaises(Denied):r.validate(delivery(bytes(17920)))
  self.assertEqual(len(r.validate(delivery(bytes(32768)))),32768)
  x,source,destination,plan=self.setup(self.plan((3,),151936));x.produce(self.body(plan))
  manifest=json.loads(source.record('r')['manifest'])
  c=dataclasses.replace(c,sha256=manifest['objects'][0]['sha256'])
  r.context=c;r.written=0;r.frames=0;seen=0
  for frame in x.frames():
   raw=payload(c,frame.sequence,frame.offset,frame.block)
   e=Envelope('task','nonce',1,1,frame.sequence,(0,3),'a'*64,2,(len(raw),),'uint8',raw)
   self.assertEqual(r.validate(SimpleNamespace(envelope=e)),frame.block)
   r.written+=len(frame.block);r.frames+=1;seen+=1
  self.assertEqual((seen,r.written),(56,1823232))

 def test_four_tokens_and_request_splitting_do_not_evade_cap(self):
  with self.assertRaises(Rejected):self.plan((1,1,1,1),151936)
  x,s,d,p=self.setup(self.plan((1,),151936),cap=650000)
  with self.assertRaises(Rejected):s.reserve('r2',p,'source')
  with self.assertRaises(Rejected):s.reserve('r',self.plan((1,),151936,attempt='other'),'source')
  self.assertEqual(s.used(),p.charge)
 def test_two_requests_concurrent_shared_book_budget(self):
  p=self.plan((1,),151936);book=Book(self.root/'shared.db',p.charge);self.addCleanup(book.close)
  ready=threading.Barrier(3);out=[]
  def reserve(i):
   b=Book(self.root/'shared.db',p.charge)
   try:
    ready.wait();b.reserve(str(i),self.plan((1,),151936,task='t'+str(i)),'source');out.append('ok')
   except Rejected:out.append('denied')
   finally:b.close()
  ts=[threading.Thread(target=reserve,args=(i,)) for i in range(2)]
  for t in ts:t.start()
  ready.wait()
  for t in ts:t.join(5);self.assertFalse(t.is_alive())
  self.assertCountEqual(out,['ok','denied']);self.assertEqual(book.used(),p.charge)
 def test_destination_unknown_keeps_source_reserved_and_body_uncalled(self):
  x,s,d,p=self.setup();d.isolate('r');calls=[]
  with self.assertRaises(Rejected):x.produce(lambda w:calls.append(1))
  self.assertEqual(calls,[]);self.assertEqual(s.used(),p.charge);self.assertEqual(d.used(),p.charge)
 def test_binding_changes_and_source_replacement_rejected(self):
  x,s,d,p=self.setup()
  for field,value in [('owner','other'),('epoch',2),('connection','other'),('input_snapshot','0'*64),('model','0'*64),('session_nonce','other'),('attempt','other')]:
   changed=dataclasses.replace(p,binding=dataclasses.replace(p.binding,**{field:value}))
   with self.assertRaises(Rejected):s.confirm('r',changed,'source')
  x.produce(self.body(p));data=x.path.read_bytes();x.path.unlink();x.path.write_bytes(data)
  with self.assertRaises(Rejected):list(x.frames())
 def test_body_once_after_write_fsync_and_commit_response_failure(self):
  for phase in ('written','before_fsync','before_commit','after_commit'):
   with tempfile.TemporaryDirectory() as tmp:
    old=self.root;self.root=Path(tmp);x,s,d,p=self.setup();calls=[]
    def hook(at):
     if at==phase:raise RuntimeError('injected')
    x.hook=hook
    def body(w):calls.append(1);self.body(p)(w)
    with self.assertRaises(RuntimeError):x.produce(body)
    with self.assertRaises(Rejected):x.produce(body)
    self.assertEqual(calls,[1]);self.assertEqual(s.used(),p.charge)
    self.assertEqual(s.record('r')['state'],'SEALED' if phase=='after_commit' else 'ISOLATED')
    self.root=old
 def test_sink_holds_write_and_cancel_fences_remaining_body(self):
  x,s,d,p=self.setup();entered=threading.Event();release=threading.Event();done=threading.Event();errors=[];progress=[]
  def hook(phase):
   if phase=='written':entered.set();self.assertTrue(release.wait(5))
  x.hook=hook
  def body(w):w(0,0,struct.pack('<3f',0,1,2));progress.append('next');w(1,0,struct.pack('<3f',10,11,12))
  def job():
   try:x.produce(body)
   except Rejected:errors.append('cancelled')
   finally:done.set()
  t=threading.Thread(target=job);t.start()
  try:
   self.assertTrue(entered.wait(5));self.assertEqual(progress,[]);self.assertFalse(done.is_set());s.isolate('r');release.set();t.join(5);self.assertFalse(t.is_alive())
  finally:release.set();t.join(5)
  self.assertEqual(errors,['cancelled']);self.assertEqual(progress,[]);self.assertEqual(s.used(),p.charge)
 def test_foreign_thread_and_escaped_write_callback_rejected(self):
  x,s,d,p=self.setup();captured=[];errors=[]
  def body(w):
   captured.append(w)
   def foreign():
    try:w(0,0,struct.pack('<3f',0,1,2))
    except Rejected:errors.append('denied')
   t=threading.Thread(target=foreign);t.start();t.join(5);self.assertFalse(t.is_alive())
   self.assertEqual(errors,['denied']);self.body(p)(w)
  x.produce(body)
  with self.assertRaises(Rejected):captured[0](2,0,bytes(12))
 def test_actual_fsync_failure_no_reexecution_or_quota_release(self):
  x,s,d,p=self.setup();calls=[]
  def body(w):calls.append(1);self.body(p)(w)
  with patch('validation.logits_spool.prototype.os.fsync',side_effect=OSError('fsync failed')):
   with self.assertRaises(OSError):x.produce(body)
  with self.assertRaises(Rejected):x.produce(body)
  self.assertEqual(calls,[1]);self.assertEqual(s.record('r')['state'],'ISOLATED')
  self.assertEqual((s.used(),d.used()),(p.charge,p.charge))
 def test_reserve_committed_response_lost_idempotent_query_and_cross_mode(self):
  p=self.plan((1,),151936);s=Book(self.root/'quota.db',p.charge);self.addCleanup(s.close)
  def uncertain():s.reserve('r',p,'source');raise OSError('lost response')
  with self.assertRaises(OSError):uncertain()
  self.assertEqual(s.record('r')['state'],'RESERVED');s.reserve('r',p,'source')
  self.assertEqual(s.used(),p.charge)
  with self.assertRaises(Rejected):s.reserve('other',self.plan((1,),151936,task='other'),'destination')
  with self.assertRaises(Rejected):s.reserve('r',p,'destination')
 def test_sealed_revoke_and_unknown_commit_stay_retired_across_instances(self):
  for unknown in (False,True):
   with tempfile.TemporaryDirectory() as tmp:
    old=self.root;self.root=Path(tmp);x,s,d,p=self.setup()
    if unknown:
     def hook(phase):
      if phase=='after_commit':raise OSError('response lost')
     x.hook=hook
     with self.assertRaises(OSError):x.produce(self.body(p))
    else:x.produce(self.body(p));s.isolate('r')
    clone=Spool(self.root/'spool',s,d,'r',p)
    with self.assertRaises(Rejected):list(clone.frames())
    with self.assertRaises(Rejected):clone.produce(self.body(p))
    self.assertEqual(s.used(),p.charge);self.root=old
 def test_cross_thread_frame_continuation_rejected(self):
  x,s,d,p=self.setup();x.produce(self.body(p));frames=x.frames();next(frames);errors=[]
  def other():
   try:next(frames)
   except Rejected:errors.append('denied')
   except StopIteration:errors.append('ended')
  t=threading.Thread(target=other);t.start();t.join(5);self.assertFalse(t.is_alive())
  self.assertEqual(errors,['denied']);frames.close()
 def test_source_replacement_before_seal_rejected(self):
  x,s,d,p=self.setup()
  def hook(phase):
   if phase=='before_commit':
    data=x.path.read_bytes();x.path.rename(x.path.with_suffix('.old'));x.path.write_bytes(data)
  x.hook=hook
  with self.assertRaises(Rejected):x.produce(self.body(p))
  self.assertEqual(s.used(),p.charge)
 def test_transport_boundary_reframes_32772_as_full_then_tail(self):
  x,s,d,p=self.setup(self.plan((1,),8193));x.produce(self.body(p))
  self.assertEqual([len(f.block) for f in x.frames()],[32768,4])
 def test_inplace_spool_change_between_reads_rejects_next_frame(self):
  x,s,d,p=self.setup(self.plan((1,),8193));x.produce(self.body(p));frames=x.frames();next(frames)
  x.path.chmod(0o600)
  with x.path.open('r+b') as f:f.seek(32768);f.write(struct.pack('<f',123))
  with self.assertRaises(Rejected):next(frames)
  self.assertEqual(s.record('r')['retired'],1);self.assertEqual(s.used(),p.charge)
 def test_reopen_book_cannot_regain_consumed_or_retired_qualification(self):
  x,s,d,p=self.setup();x.produce(self.body(p));s.isolate('r')
  other=Book(self.root/'source.db',2*1024*1024);self.addCleanup(other.close)
  other.reserve('r',p,'source')
  with self.assertRaises(Rejected):Spool(self.root/'spool',other,d,'r',p).produce(self.body(p))
  with self.assertRaises(Rejected):list(Spool(self.root/'spool',other,d,'r',p).frames())
  self.assertEqual(other.used(),p.charge)
 def test_no_ack_delete_or_replay_permission(self):
  x,s,d,p=self.setup();x.produce(self.body(p));list(x.frames())
  with self.assertRaises(Rejected):list(x.frames())
  with self.assertRaises(Rejected):x.produce(self.body(p))
  self.assertFalse(hasattr(x,'release'));self.assertEqual(s.used(),p.charge);self.assertEqual(d.used(),p.charge)

if __name__=='__main__':unittest.main()
