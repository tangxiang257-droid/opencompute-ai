"""Private single-coordinator, ordinary SQLite/callback tests; no network/model."""
import asyncio,sqlite3,threading,unittest
from pathlib import Path
from unittest.mock import patch
from test_p1_bound_connection import Tests as Helper,auth
from cluster_p1.store import Conflict
try:from cluster_p1.nonce_coordinator import NonceCoordinator
except ImportError:NonceCoordinator=None
_helper_setup=Helper.asyncSetUp
class Tests(unittest.IsolatedAsyncioTestCase):
 channel=Helper.channel;frame=Helper.frame;queue=Helper.queue
 async def asyncSetUp(self):
  await _helper_setup(self)
  self.assertIsNotNone(NonceCoordinator,'durable nonce coordinator missing')
  self.path=Path(self.tmp.name)/'coordinator';self.co=NonceCoordinator.initialize(self.path,authorize=auth)
  self.addCleanup(self.co.close)
 asyncTearDown=Helper.asyncTearDown
 def reserve(self,co=None,args=None):
  return (co or self.co).register(*(args or self.args),hidden_size=24,vocab_size=32,config_sha256='c'*64)
 def receiver(self,r,name='recv',co=None):
  from cluster_p1.bound_connection import BoundReceiptConnection
  c=self.channel();b=BoundReceiptConnection(c)
  j=(co or self.co).create_receiver(r,b,Path(self.tmp.name)/name,execution_authorize=auth)
  self.addCleanup(j.close);return c,b,j
 async def test_new_connection_database_same_nonce_no_second_execution(self):
  r=self.reserve();c,b,j=self.receiver(r);calls=[]
  for k in range(3):self.queue(c,k);await b.receive_once(j)
  o=j.make_stage1_owner(lambda v,**kw:calls.append(v) or b'\0'*256,instance_id='once')
  try:await o.call(self.frame().payload,token_start=0,token_count=2)
  finally:await o.close()
  with self.assertRaises(Conflict):self.receiver(r,'other')
  with self.assertRaises(Conflict):self.reserve()
  self.assertEqual(len(calls),1);self.assertFalse((Path(self.tmp.name)/'other').exists())
 async def test_sender_receiver_slots_independent_once(self):
  r=self.reserve();a=self.co.create_sender(r,Path(self.tmp.name)/'sender',execution_authorize=auth);self.addCleanup(a.close)
  self.receiver(r)
  with self.assertRaises(Conflict):self.co.create_sender(r,Path(self.tmp.name)/'sender2',execution_authorize=auth)
 async def test_nonce_not_reusable_with_changed_epoch(self):
  self.reserve();a=list(self.args);a[1]=a[1].model_copy(update={'epoch':2});a[3]=tuple(dict(x,epoch=2) for x in a[3])
  with self.assertRaises(Conflict):self.reserve(args=tuple(a))
 async def test_failure_after_slot_commit_burns_slot(self):
  r=self.reserve()
  with patch('cluster_p1.bound_connection.BoundReceiptConnection.create_journal',side_effect=RuntimeError('creation fails')):
   with self.assertRaises(RuntimeError):self.receiver(r)
  with self.assertRaises(Conflict):self.receiver(r,'retry')
 async def test_reopen_only_history_no_resume(self):
  r=self.reserve();self.co.close();co=NonceCoordinator.open_existing(self.path,authorize=auth);self.addCleanup(co.close)
  self.assertFalse(co.query(self.sid)['execution_allowed'])
  with self.assertRaises(Conflict):self.reserve(co)
  with self.assertRaises(Conflict):self.receiver(r,'new',co)
 async def test_old_live_journal_fenced_on_coordinator_close(self):
  r=self.reserve();c,b,j=self.receiver(r);self.co.close();self.queue(c)
  with self.assertRaises(Conflict):await b.receive_once(j)
  with sqlite3.connect(j._path) as db:self.assertEqual(db.execute('select count(*) from p1_v2_receipts').fetchone()[0],0)
 async def test_missing_existing_not_initialized(self):
  p=Path(self.tmp.name)/'missing'
  with self.assertRaises(FileNotFoundError):NonceCoordinator.open_existing(p,authorize=auth)
  self.assertFalse(p.exists())
 async def test_exclusive_existing_open_refused(self):
  with self.assertRaises((Conflict,BlockingIOError)):NonceCoordinator.open_existing(self.path,authorize=auth)
 async def test_changed_file_identity_fences(self):
  r=self.reserve();self.path.rename(self.path.with_suffix('.old'));self.path.write_bytes(b'bad')
  with self.assertRaises(Conflict):self.receiver(r)
 async def test_bad_database_does_not_auto_repair(self):
  self.co.close();self.path.write_bytes(b'not sqlite')
  with self.assertRaises((Conflict,sqlite3.DatabaseError)):NonceCoordinator.open_existing(self.path,authorize=auth)
  self.assertEqual(self.path.read_bytes(),b'not sqlite')
 async def test_parallel_slot_claim_one_winner(self):
  r=self.reserve();barrier=threading.Barrier(2);results=[]
  def claim(n):
   barrier.wait()
   try:results.append(self.co.create_sender(r,Path(self.tmp.name)/n,execution_authorize=auth))
   except BaseException as e:results.append(e)
  a=threading.Thread(target=claim,args=('a',));b=threading.Thread(target=claim,args=('b',));a.start();b.start();a.join(3);b.join(3)
  self.assertFalse(a.is_alive() or b.is_alive());wins=[x for x in results if not isinstance(x,BaseException)]
  self.assertEqual(len(wins),1);wins[0].close();self.assertEqual(sum(isinstance(x,Conflict) for x in results),1)
 async def test_committed_but_response_lost_no_reissue(self):
  original=self.co._commit
  def commit(c):original(c);raise RuntimeError('lost commit response')
  with patch.object(self.co,'_commit',side_effect=commit):
   with self.assertRaises(RuntimeError):self.reserve()
  self.co.close();co=NonceCoordinator.open_existing(self.path,authorize=auth);self.addCleanup(co.close)
  with self.assertRaises(Conflict):self.reserve(co)
 async def test_claim_commit_unknown_retires_coordinator(self):
  r=self.reserve();original=self.co._commit
  def commit(c):original(c);raise RuntimeError('lost commit response')
  with patch.object(self.co,'_commit',side_effect=commit):
   with self.assertRaises(RuntimeError):self.receiver(r)
  with self.assertRaises(Conflict):self.receiver(r,'again')
  self.assertFalse((Path(self.tmp.name)/'recv').exists())
 async def test_wrong_owner_registration_no_row(self):
  from contextlib import contextmanager
  @contextmanager
  def wrong():yield 'wrong'
  with patch.object(self.co,'_authorize',wrong):
   with self.assertRaises(PermissionError):self.reserve()
  with sqlite3.connect(self.path) as db:self.assertEqual(db.execute('select count(*) from sessions').fetchone()[0],0)
 async def test_mutated_reservation_not_authority(self):
  r=self.reserve()
  with self.assertRaises(Conflict):self.receiver(object())
  self.assertFalse((Path(self.tmp.name)/'recv').exists())
 async def test_initializer_authorization_exit_failure_releases_lock(self):
  from contextlib import contextmanager
  @contextmanager
  def fail_exit():
   yield 'owner'
   raise RuntimeError('authority exit')
  p=Path(self.tmp.name)/'init-exit'
  with self.assertRaises(RuntimeError):NonceCoordinator.initialize(p,authorize=fail_exit)
  co=NonceCoordinator.open_existing(p,authorize=auth);co.close()
 async def test_receiver_factory_failure_does_not_hold_registry_lock(self):
  r=self.reserve()
  with patch('cluster_p1.bound_connection.BoundReceiptConnection.create_journal',side_effect=RuntimeError('fail')):
   with self.assertRaises(RuntimeError):self.receiver(r)
  self.assertEqual(self.co.query(self.sid)['claimed_stages'],(1,))
 async def test_declared_plan_changes_before_registration_rejected(self):
  a=list(self.args);a[2]=(a[2][0].model_copy(update={'layer_end':2}),a[2][1])
  with self.assertRaises((ValueError,Conflict)):self.reserve(args=tuple(a))
 async def test_postcommit_owner_exit_failure_no_reservation_returned(self):
  from contextlib import contextmanager
  @contextmanager
  def fail_exit():
   yield 'owner'
   raise RuntimeError('authority exit')
  with patch.object(self.co,'_authorize',fail_exit):
   with self.assertRaises(RuntimeError):self.reserve()
  self.co.close();co=NonceCoordinator.open_existing(self.path,authorize=auth);self.addCleanup(co.close)
  with self.assertRaises(Conflict):self.reserve(co)
 async def test_sender_complete_stage_declarations_fixed(self):
  import json,struct
  r=self.reserve();j=self.co.create_sender(r,Path(self.tmp.name)/'sender',execution_authorize=auth);self.addCleanup(j.close)
  with sqlite3.connect(j._path) as c:
   for sid,body in c.execute('select stage_id,body from p1_stages').fetchall():
    d=json.loads(body)
    if sid==0:d['layer_end']=2
    else:d['layer_start']=2
    c.execute('update p1_stages set body=? where stage_id=?',(json.dumps(d),sid))
  values=(struct.pack('<2q',1,2),struct.pack('<2q',3,4),struct.pack('<q',5));calls=[];o=None
  try:
   with self.assertRaises(Conflict):
    o=j.make_owner(lambda v,**kw:calls.append(v) or b'\0'*192,values,instance_id='sender')
    await o.call(values[0],token_start=0,token_count=2)
  finally:
   if o is not None:await o.close()
  self.assertEqual(calls,[])
 async def test_sender_fixed_plan_executes_once(self):
  import struct
  r=self.reserve();j=self.co.create_sender(r,Path(self.tmp.name)/'sender',execution_authorize=auth);self.addCleanup(j.close)
  values=(struct.pack('<2q',1,2),struct.pack('<2q',3,4),struct.pack('<q',5));calls=[]
  o=j.make_owner(lambda v,**kw:calls.append(v) or b'\0'*192,values,instance_id='sender')
  try:await o.call(values[0],token_start=0,token_count=2)
  finally:await o.close()
  self.assertEqual(len(calls),1);self.assertEqual(j.query(0)['state'],'OUTPUT_COMMITTED')
 def change_task(self,j,**changes):
  import json
  with sqlite3.connect(j._path) as db:
   d=json.loads(db.execute('select body from p1_tasks').fetchone()[0]);d.update(changes)
   db.execute('update p1_tasks set body=?',(json.dumps(d),))
 async def test_receiver_privacy_change_before_receipt_rolls_back(self):
  r=self.reserve();c,b,j=self.receiver(r);self.change_task(j,privacy_level='L3');self.queue(c)
  with self.assertRaises(Conflict):await b.receive_once(j)
  with sqlite3.connect(j._path) as db:
   self.assertEqual(db.execute('select count(*) from p1_v2_receipts').fetchone()[0],0)
   self.assertEqual(db.execute('select count(*) from p1_received_frames').fetchone()[0],0)
 async def test_receiver_task_change_before_owner_rejected(self):
  r=self.reserve();c,b,j=self.receiver(r)
  for k in range(3):self.queue(c,k);await b.receive_once(j)
  self.change_task(j,privacy_level='L3',cost_cap=123,deadline=456,payload_ref='other')
  calls=[];o=None
  try:
   with self.assertRaises(Conflict):
    o=j.make_stage1_owner(lambda v,**kw:calls.append(v) or b'\0'*256,instance_id='x')
    await o.call(self.frame().payload,token_start=0,token_count=2)
  finally:
   if o is not None:await o.close()
  self.assertEqual(calls,[])
 async def test_receiver_task_change_after_owner_before_admit(self):
  r=self.reserve();c,b,j=self.receiver(r)
  for k in range(3):self.queue(c,k);await b.receive_once(j)
  calls=[];o=j.make_stage1_owner(lambda v,**kw:calls.append(v) or b'\0'*256,instance_id='x')
  self.change_task(j,privacy_level='L3')
  try:
   with self.assertRaises(Conflict):await o.call(self.frame().payload,token_start=0,token_count=2)
  finally:await o.close()
  self.assertEqual(calls,[])
  with sqlite3.connect(j._path) as db:self.assertEqual(db.execute('select count(*) from p1_local_stage1_attempts').fetchone()[0],0)
 async def test_receiver_task_change_during_body_refuses_commit(self):
  r=self.reserve();c,b,j=self.receiver(r)
  for k in range(3):self.queue(c,k);await b.receive_once(j)
  def op(v,**kw):self.change_task(j,privacy_level='L3');return b'\0'*256
  o=j.make_stage1_owner(op,instance_id='x')
  try:
   with self.assertRaises(Conflict):await o.call(self.frame().payload,token_start=0,token_count=2)
  finally:await o.close()
  with sqlite3.connect(j._path) as db:self.assertEqual(db.execute('select state,output from p1_local_stage1_attempts').fetchone(),('ADMITTED',None))
 async def test_receiver_session_declaration_change_rejected(self):
  import json
  r=self.reserve();c,b,j=self.receiver(r)
  for k in range(3):self.queue(c,k);await b.receive_once(j)
  with sqlite3.connect(j._path) as db:
   d=json.loads(db.execute('select body from p1_sessions').fetchone()[0]);d['selected_nodes'].append('extra')
   db.execute('update p1_sessions set body=?',(json.dumps(d),))
  with self.assertRaises(Conflict):j.make_stage1_owner(lambda v,**kw:b'\0'*256,instance_id='x')
del Helper
