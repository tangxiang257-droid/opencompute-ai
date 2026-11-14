import dataclasses,hashlib,struct,tempfile,threading,unittest
from contextlib import contextmanager
from pathlib import Path
from validation.logits_spool.prototype import Binding,Plan,Book,Spool,Rejected
try:
 from validation.private_producer.contract import Spec,Permit,PrivateProducer
except ImportError:Spec=Permit=PrivateProducer=None

class Tests(unittest.TestCase):
 def setUp(self):
  self.assertIsNotNone(PrivateProducer,'private contract missing')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.value=struct.pack('<6f',1,2,3,4,5,6)
  self.plan=Plan(Binding('owner','task','nonce','attempt',1,'generation',*['a'*64]*6),(2,),3)
  self.source=Book(self.root/'s.db',2*1024*1024);self.dest=Book(self.root/'d.db',2*1024*1024)
  self.addCleanup(self.source.close);self.addCleanup(self.dest.close)
  self.source.reserve('r',self.plan,'source');self.dest.reserve('r',self.plan,'destination')
  self.spec=Spec(self.plan,3,hashlib.sha256(self.value).hexdigest())
  self.spool=Spool(self.root/'spool',self.source,self.dest,'r',self.plan)
  self.calls=[]
 def body(self,value,write):
  self.calls.append(value)
  for token in range(2):write(token,0,struct.pack('<3f',token*3,token*3+1,token*3+2))
 @contextmanager
 def auth(self,spec):yield Permit('owner',self.spec.digest)
 def adapter(self,body=None,auth=None):return PrivateProducer(self.spec,self.spool,body or self.body,authorize=auth)
 def test_default_denial_permanently_retires_without_body(self):
  x=self.adapter()
  with self.assertRaises(Rejected):x.run(self.value)
  self.assertEqual(self.calls,[]);self.assertEqual(self.source.record('r')['retired'],1)
  with self.assertRaises(Rejected):self.adapter(auth=self.auth).run(self.value)
 def test_explicit_synthetic_context_and_budgets_not_reference_length(self):
  x=self.adapter(auth=self.auth);result=x.run(self.value)
  self.assertFalse(result.execution_allowed);self.assertEqual(result.output_bytes,24)
  self.assertEqual(result.source_charge,self.plan.charge);self.assertEqual(result.destination_charge,self.plan.charge)
  self.assertEqual(self.calls,[self.value]);self.assertGreater(result.working_bytes,len(self.value))
  self.assertFalse(hasattr(result,'payload'))
  with self.assertRaises(Rejected):x.run(self.value)
 def test_wrong_actual_input_or_reference_rejected_before_body(self):
  x=self.adapter(auth=self.auth)
  with self.assertRaises(Rejected):x.run(b'/tmp/large-object')
  self.assertEqual(self.calls,[])
 def test_same_length_changed_input_digest_denied(self):
  x=self.adapter(auth=self.auth)
  with self.assertRaises(Rejected):x.run(struct.pack('<6f',6,5,4,3,2,1))
  self.assertEqual(self.calls,[])
 def test_spec_rebind_cannot_replace_persisted_input_or_plan(self):
  self.adapter(auth=self.auth)
  changed=dataclasses.replace(self.spec,input_digest='0'*64)
  with self.assertRaises(Rejected):PrivateProducer(changed,self.spool,self.body,authorize=self.auth)
  changed=dataclasses.replace(self.spec,plan=dataclasses.replace(self.plan,binding=dataclasses.replace(self.plan.binding,epoch=2)))
  with self.assertRaises(Rejected):PrivateProducer(changed,self.spool,self.body,authorize=self.auth)
 def test_wrong_owner_and_permission_boolean_fail_closed(self):
  @contextmanager
  def bad(spec):yield True
  with self.assertRaises(Rejected):self.adapter(auth=bad).run(self.value)
  self.assertEqual(self.calls,[])
 def test_context_suppression_cannot_hide_failure(self):
  @contextmanager
  def suppress(spec):
   try:yield Permit('owner',self.spec.digest)
   except BaseException:pass
  def bad(value,write):self.calls.append(value);raise RuntimeError('body failed')
  x=self.adapter(body=bad,auth=suppress)
  with self.assertRaises(RuntimeError):x.run(self.value)
  self.assertEqual(self.source.record('r')['retired'],1)
  with self.assertRaises(Rejected):self.adapter(auth=self.auth).run(self.value)
 def test_context_exit_failure_after_seal_retires_persistently(self):
  @contextmanager
  def fail(spec):yield Permit('owner',self.spec.digest);raise RuntimeError('exit failed')
  with self.assertRaises(RuntimeError):self.adapter(auth=fail).run(self.value)
  self.assertEqual(self.source.record('r')['state'],'SEALED');self.assertEqual(self.source.record('r')['retired'],1)
  with self.assertRaises(Rejected):list(self.spool.frames())
 def test_body_must_return_none_not_reference_or_bytes(self):
  def bad(value,write):self.body(value,write);return 'file:already-here'
  with self.assertRaises(Rejected):self.adapter(body=bad,auth=self.auth).run(self.value)
  self.assertEqual(self.source.record('r')['retired'],1)
 def test_running_cancel_barrier_stops_write_and_no_replay(self):
  entered=threading.Event();release=threading.Event();errors=[]
  def body(value,write):
   self.calls.append(value);entered.set();self.assertTrue(release.wait(5));write(0,0,bytes(12))
  x=self.adapter(body=body,auth=self.auth)
  def run():
   try:x.run(self.value)
   except Rejected:errors.append('retired')
  t=threading.Thread(target=run);t.start()
  try:self.assertTrue(entered.wait(5));x.retire();release.set();t.join(5);self.assertFalse(t.is_alive())
  finally:release.set();t.join(5)
  self.assertEqual(errors,['retired']);self.assertEqual(self.calls,[self.value])
  with self.assertRaises(Rejected):self.adapter(auth=self.auth).run(self.value)
 def test_large_logits_require_full_spool_quota_and_explicit_new_path(self):
  p=dataclasses.replace(self.plan,counts=(1,),vocab=151936)
  self.assertEqual(p.total,607744)
  spec=Spec(p,3,hashlib.sha256(bytes(12)).hexdigest())
  self.assertEqual(spec.output_bytes,607744);self.assertGreater(spec.source_charge,607744)
  with self.assertRaises(Rejected):Spec(p,20000,'a'*64)
 def test_nonfinite_input_rejected_even_when_digest_matches(self):
  bad=struct.pack('<I',0x7fc00000)*6
  spec=dataclasses.replace(self.spec,input_digest=hashlib.sha256(bad).hexdigest())
  @contextmanager
  def auth(s):yield Permit('owner',spec.digest)
  x=PrivateProducer(spec,self.spool,self.body,authorize=auth)
  with self.assertRaises(Rejected):x.run(bad)
  self.assertEqual(self.calls,[])

 def test_working_window_aggregate_not_per_instance(self):
  self.adapter(auth=self.auth).run(self.value)
  p=dataclasses.replace(self.plan,binding=dataclasses.replace(self.plan.binding,task='other'))
  self.source.reserve('r2',p,'source');self.dest.reserve('r2',p,'destination')
  spec=Spec(p,3,hashlib.sha256(self.value).hexdigest());spool=Spool(self.root/'spool2',self.source,self.dest,'r2',p)
  @contextmanager
  def auth(s):yield Permit('owner',spec.digest)
  other=PrivateProducer(spec,spool,self.body,authorize=auth)
  with self.assertRaises(Rejected):other.run(self.value)
  self.assertEqual(self.calls,[self.value])
  with self.source.lock:used=self.source.db.execute('SELECT sum(working) FROM private_producer_v1').fetchone()[0]
  self.assertEqual(used,self.spec.working_bytes)
 def test_full_output_reservation_cannot_be_small_reference_charge(self):
  with self.source.tx() as c:c.execute("UPDATE reservation SET charge=1 WHERE id='r'")
  with self.assertRaises(Rejected):self.adapter(auth=self.auth).run(self.value)
  self.assertEqual(self.calls,[])
 def test_wrong_permit_owner_or_spec_digest_rejected(self):
  @contextmanager
  def bad(spec):yield Permit('intruder','0'*64)
  with self.assertRaises(Rejected):self.adapter(auth=bad).run(self.value)
  self.assertEqual(self.calls,[])
 def test_legacy_owner_still_rejects_large_bytes_and_references(self):
  import asyncio
  from cluster_p1.stage_ownership import StageOwner,StageBinding,OwnershipError
  from cluster_p1.local_attempt import MAX_BYTES,MAX_RESERVED
  self.assertEqual((MAX_BYTES,MAX_RESERVED),(65536,262144))
  async def check():
   for result in (bytes(65537),'file:spool'):
    owner=StageOwner(StageBinding('task','session',1,1,'a'*64,'b'*64),lambda *a,**k:result,authorize=lambda spec:True,commit=lambda *a:None)
    try:
     with self.assertRaises(OwnershipError):await owner.call(bytes(4),token_start=0,token_count=1)
    finally:await owner.close()
  asyncio.run(check())

 def test_authorization_exit_barrier_blocks_export_until_done(self):
  entered=threading.Event();release=threading.Event();errors=[]
  @contextmanager
  def auth(spec):
   yield Permit('owner',self.spec.digest)
   entered.set();self.assertTrue(release.wait(5));raise RuntimeError('exit failure')
  x=self.adapter(auth=auth)
  def run():
   try:x.run(self.value)
   except RuntimeError:errors.append('exit')
  t=threading.Thread(target=run);t.start()
  try:
   self.assertTrue(entered.wait(5))
   clone=Spool(self.root/'spool',self.source,self.dest,'r',self.plan)
   with self.assertRaises(Rejected):next(clone.frames())
  finally:release.set();t.join(5)
  self.assertFalse(t.is_alive());self.assertEqual(errors,['exit'])
  with self.assertRaises(Rejected):list(clone.frames())
 def test_retirement_updates_both_records_without_second_transaction(self):
  from unittest.mock import patch
  x=self.adapter(auth=self.auth);x.run(self.value)
  with patch.object(self.source,'isolate',side_effect=AssertionError('separate retirement transaction')):x.retire()
  self.assertEqual(self.source.record('r')['retired'],1)
  with self.source.lock:retired=self.source.db.execute("SELECT retired FROM private_producer_v1 WHERE id='r'").fetchone()[0]
  self.assertEqual(retired,1)
 def test_concurrent_duplicate_retires_without_second_body(self):
  entered=threading.Event();release=threading.Event();errors=[]
  def body(value,write):
   self.calls.append(value);entered.set();self.assertTrue(release.wait(5));write(0,0,bytes(12))
  first=self.adapter(body=body,auth=self.auth)
  def run():
   try:first.run(self.value)
   except Rejected:errors.append('first')
  t=threading.Thread(target=run);t.start()
  try:
   self.assertTrue(entered.wait(5))
   with self.assertRaises(Rejected):self.adapter(auth=self.auth).run(self.value)
  finally:release.set();t.join(5)
  self.assertFalse(t.is_alive());self.assertEqual(self.calls,[self.value]);self.assertEqual(errors,['first'])
 def test_authorization_enter_failure_never_calls_body(self):
  @contextmanager
  def fail(spec):raise RuntimeError('entry denied');yield
  with self.assertRaises(RuntimeError):self.adapter(auth=fail).run(self.value)
  self.assertEqual(self.calls,[]);self.assertEqual(self.source.record('r')['retired'],1)
 def test_large_synthetic_output_fully_charged_and_streamed(self):
  p=dataclasses.replace(self.plan,counts=(1,),vocab=151936,binding=dataclasses.replace(self.plan.binding,task='large'))
  self.source.reserve('large',p,'source');self.dest.reserve('large',p,'destination')
  value=bytes(12);spec=Spec(p,3,hashlib.sha256(value).hexdigest())
  spool=Spool(self.root/'large-spool',self.source,self.dest,'large',p)
  @contextmanager
  def auth(s):yield Permit('owner',spec.digest)
  def body(v,write):
   for column in range(0,p.vocab,8192):write(0,column,bytes(min(8192,p.vocab-column)*4))
  result=PrivateProducer(spec,spool,body,authorize=auth).run(value)
  self.assertEqual((result.output_bytes,result.source_charge,result.destination_charge),(607744,640512,640512))
  self.assertEqual(sum(len(frame.block) for frame in spool.frames()),607744)
  self.assertFalse(result.execution_allowed)

 def test_retirement_storage_failure_cannot_export_unregistered_result(self):
  import sqlite3
  @contextmanager
  def auth(spec):
   yield Permit('owner',self.spec.digest)
   def deny_update(op,table,*args):
    return sqlite3.SQLITE_DENY if op==sqlite3.SQLITE_UPDATE and table=='reservation' else sqlite3.SQLITE_OK
   self.source.db.set_authorizer(deny_update)
   raise RuntimeError('exit failed')
  try:
   with self.assertRaises(sqlite3.DatabaseError):self.adapter(auth=auth).run(self.value)
  finally:self.source.db.set_authorizer(None)
  with self.source.lock:row=self.source.db.execute("SELECT state,retired FROM private_producer_v1 WHERE id='r'").fetchone()
  self.assertEqual(row,('CONSUMED',0))  # retirement transaction rolled back fully
  self.assertEqual(self.source.record('r')['state'],'SEALED')
  with self.assertRaises(Rejected):list(Spool(self.root/'spool',self.source,self.dest,'r',self.plan).frames())
  self.assertEqual(len(self.calls),1)
  with self.assertRaises(Rejected):self.adapter(auth=self.auth).run(self.value)
  self.assertEqual(len(self.calls),1)

if __name__=='__main__':unittest.main()
