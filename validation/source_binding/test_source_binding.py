"""Finite synthetic declarations, actual local SQLite; never execution/network."""
import dataclasses,hashlib,importlib.abc,json,os,sqlite3,sys,tempfile,threading,unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name.split('.')[0] in {'torch','transformers','safetensors','tensorflow','public_pool'} or name.endswith('.worker'):raise AssertionError('offline import denied')
def guard(event,args):
 if event in ('socket.bind','socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'):raise AssertionError('offline event denied')
sys.meta_path.insert(0,Block());sys.addaudithook(guard)
from input_declaration import InputDeclaration,DeclaredModel
from pair_plan_contract import _digest
from cluster_p1.nonce_coordinator import NonceCoordinator
from cluster_p1.store import Conflict
MODEL=dict(kind='ModelIdentity',version=1,config_sha256='1'*64,weights_sha256='2'*64,semantics='stock-qwen2-full-causal-v1',weight_format='safetensors-single',compute_dtype='float32',kv_dtype='float32',activation_dtype='float32',logits_dtype='float32')
D=InputDeclaration('a'*32,'owner','request','draft',1,'3'*64,'4'*64,'5'*64,'6'*64,3,100,'prep-v1','7'*64,_digest(MODEL),DeclaredModel(**{k:v for k,v in MODEL.items() if k not in ('kind','version')}))
NS='f'*64;NONCE='b'*32
@contextmanager
def auth():yield 'owner'
class Checks(unittest.TestCase):
 def setUp(self):
  self.assertTrue(hasattr(NonceCoordinator,'bind_source_declaration'),'source-only binding API missing')
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.path=Path(self.tmp.name)/'coordinator'
  self.co=NonceCoordinator.initialize(self.path,authorize=auth,source_namespace=NS);self.addCleanup(self.co.close)
 def bind(self,d=D,n=NONCE,co=None):return (co or self.co).bind_source_declaration(d,n)
 def counts(self):
  with sqlite3.connect(self.path) as c:return tuple(c.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in ('sessions','source_claims','slots'))
 def reopen(self,path=None):
  self.co.close();co=NonceCoordinator.open_existing(path or self.path,authorize=auth,source_namespace=NS);self.addCleanup(co.close);return co
 def test_atomic_inert_claim_and_exact_history(self):
  result=self.bind();self.assertFalse(result.execution_allowed);self.assertEqual(result.nonce,NONCE);self.assertEqual(result.task_id,D.task_id);self.assertEqual(self.counts(),(1,1,0));self.assertEqual(self.co._reservations,{})
  with self.assertRaises(dataclasses.FrozenInstanceError):result.execution_allowed=True
  q=self.co.query_source(D.task_id);self.assertTrue(q['exists']);self.assertEqual(q['nonce'],NONCE);self.assertFalse(q['execution_allowed']);self.assertEqual(q['declaration_sha256'],result.declaration_sha256)
  self.assertEqual(self.co.query(NONCE)['claimed_stages'],())
 def test_same_source_new_nonce_or_changed_snapshot_refused(self):
  self.bind()
  for d,n in ((D,NONCE),(D,'c'*32),(dataclasses.replace(D,snapshot_digest='e'*64),'d'*32)):
   with self.assertRaises(Conflict):self.bind(d,n)
  self.assertEqual(self.counts(),(1,1,0));self.assertFalse(self.co._retired)
 def test_different_tasks_equal_token_payload_not_deduplicated(self):
  self.bind();self.bind(dataclasses.replace(D,task_id='c'*32,request_id='other'),'d'*32);self.assertEqual(self.counts(),(2,2,0))
 def test_nonce_unique_across_sources(self):
  self.bind()
  with self.assertRaises(Conflict):self.bind(dataclasses.replace(D,task_id='c'*32))
  self.assertEqual(self.counts(),(1,1,0))
 def test_parallel_same_source_different_nonce_exactly_one_winner(self):
  barrier=threading.Barrier(2);results=[]
  def call(n):
   barrier.wait()
   try:results.append(self.bind(n=n))
   except BaseException as e:results.append(e)
  ts=[threading.Thread(target=call,args=(n*32,)) for n in ('c','d')]
  for t in ts:t.start()
  for t in ts:t.join(3)
  self.assertFalse(any(t.is_alive() for t in ts));self.assertEqual(sum(not isinstance(x,BaseException) for x in results),1);self.assertEqual(sum(isinstance(x,Conflict) for x in results),1);self.assertEqual(self.counts(),(1,1,0))
 def test_live_source_mode_denies_all_legacy_execution_entrypoints(self):
  receipt=self.bind()
  for f in (lambda:self.co.register(None,None,(),(),hidden_size=1,vocab_size=1,config_sha256='1'*64),lambda:self.co._reservation(receipt),lambda:self.co._claim(receipt,0,self.path.parent/'sender'),lambda:self.co.create_sender(receipt,self.path.parent/'sender',execution_authorize=auth),lambda:self.co.create_receiver(receipt,None,self.path.parent/'receiver',execution_authorize=auth)):
   with self.assertRaises(Conflict):f()
  self.assertFalse((self.path.parent/'sender').exists());self.assertEqual(self.counts(),(1,1,0))
 def test_existing_reopen_is_history_only_even_for_new_source(self):
  self.bind();co=self.reopen();self.assertTrue(co.query_source(D.task_id)['exists'])
  for d,n in ((D,'c'*32),(dataclasses.replace(D,task_id='d'*32),'e'*32)):
   with self.assertRaises(Conflict):self.bind(d,n,co)
  self.assertEqual(self.counts(),(1,1,0))
 def test_commit_before_effect_failure_retires_and_reopen_cannot_retry(self):
  with patch.object(self.co,'_commit',side_effect=RuntimeError('unknown commit')):
   with self.assertRaises(RuntimeError):self.bind()
  self.assertTrue(self.co._retired);self.assertEqual(self.counts(),(0,0,0));co=self.reopen();self.assertFalse(co.query_source(D.task_id)['exists'])
  with self.assertRaises(Conflict):self.bind(n='c'*32,co=co)
 def test_commit_after_effect_failure_retains_both_rows(self):
  original=self.co._commit
  def commit(c):original(c);raise RuntimeError('lost response')
  with patch.object(self.co,'_commit',side_effect=commit):
   with self.assertRaises(RuntimeError):self.bind()
  self.assertTrue(self.co._retired);self.assertEqual(self.counts(),(1,1,0));co=self.reopen();self.assertTrue(co.query_source(D.task_id)['exists'])
  with self.assertRaises(Conflict):self.bind(n='c'*32,co=co)
 def test_authorization_exit_failure_no_success_retains_history(self):
  @contextmanager
  def fails():yield 'owner';raise RuntimeError('exit failed')
  with patch.object(self.co,'_authorize',fails):
   with self.assertRaises(RuntimeError):self.bind()
  self.assertTrue(self.co._retired);self.assertEqual(self.counts(),(1,1,0))
 def test_authorization_cannot_suppress_duplicate(self):
  self.bind()
  @contextmanager
  def suppress():
   try:yield 'owner'
   except BaseException:pass
  with patch.object(self.co,'_authorize',suppress):
   with self.assertRaises(Conflict):self.bind(n='c'*32)
  self.assertEqual(self.counts(),(1,1,0))
 def test_source_insert_failure_rolls_back_nonce_and_retires(self):
  # Fault injection after sessions insert: same connection still owns transaction.
  original=self.co._insert_source_claim
  def fails(c,*a):raise RuntimeError('source write failed')
  with patch.object(self.co,'_insert_source_claim',side_effect=fails):
   with self.assertRaises(RuntimeError):self.bind()
  self.assertTrue(self.co._retired);self.assertEqual(self.counts(),(0,0,0));co=self.reopen()
  with self.assertRaises(Conflict):self.bind(co=co)
 def test_namespace_mismatch_and_legacy_open_do_not_write(self):
  self.bind();self.co.close();before=self.path.read_bytes()
  for kwargs in ({},{'source_namespace':'e'*64}):
   with self.assertRaises(Conflict):NonceCoordinator.open_existing(self.path,authorize=auth,**kwargs)
   self.assertEqual(before,self.path.read_bytes())
 def test_v1_not_upgraded_or_source_enabled(self):
  path=self.path.parent/'v1';v1=NonceCoordinator.initialize(path,authorize=auth);self.addCleanup(v1.close)
  with self.assertRaises(Conflict):self.bind(co=v1)
  v1.close();before=path.read_bytes()
  with self.assertRaises(Conflict):NonceCoordinator.open_existing(path,authorize=auth,source_namespace=NS)
  self.assertEqual(before,path.read_bytes())
 def test_path_rename_live_fences_and_reopen_remains_history_only(self):
  self.bind();other=self.path.parent/'renamed';self.path.rename(other)
  with self.assertRaises(Conflict):self.co.query_source(D.task_id)
  co=self.reopen(other);self.assertTrue(co.query_source(D.task_id)['exists'])
  with self.assertRaises(Conflict):self.bind(n='c'*32,co=co)
 def test_wrong_owner_and_malformed_declaration_never_insert(self):
  bad=(dataclasses.replace(D,owner_id='other'),dataclasses.replace(D,execution_allowed=True),dataclasses.replace(D,computation_verified=0),dataclasses.replace(D,token_count=True),dataclasses.replace(D,model_record_digest=MODEL['weights_sha256']),dataclasses.replace(D,tokenizer_status='verified'),dataclasses.replace(D,request_id='x'*129),dataclasses.replace(D,model=dataclasses.replace(D.model,weights_sha256='e'*64)))
  for d in bad:
   with self.assertRaises((ValueError,TypeError,PermissionError,Conflict)):self.bind(d)
  self.assertEqual(self.counts(),(0,0,0))
 def test_wrong_authorizer_denied(self):
  @contextmanager
  def other():yield 'other'
  with patch.object(self.co,'_authorize',other):
   with self.assertRaises(PermissionError):self.bind()
  self.assertEqual(self.counts(),(0,0,0))
 def test_immutable_schema_blocks_update_and_delete(self):
  self.bind()
  with sqlite3.connect(self.path) as c:
   for table in ('source_claims','sessions'):
    with self.assertRaises(sqlite3.IntegrityError):c.execute('DELETE FROM '+table)
  self.assertEqual(self.counts(),(1,1,0))
 def test_bad_namespace_or_nonce_refused(self):
  for n in ('', 'c'*31, 'Z'*32, b'c'*32):
   with self.assertRaises((TypeError,ValueError)):self.bind(n=n)
  p=self.path.parent/'bad'
  with self.assertRaises((ValueError,TypeError)):NonceCoordinator.initialize(p,authorize=auth,source_namespace='mutable-path')
  self.assertFalse(p.exists());self.assertEqual(self.counts(),(0,0,0))
 def test_reject_model_object_before_deepcopy_callback(self):
  called=[]
  class Malicious:
   def __deepcopy__(self,memo):called.append('ran');return 'float32'
  bad=dataclasses.replace(D,model=dataclasses.replace(D.model,compute_dtype=Malicious()))
  try:self.bind(bad)
  except (TypeError,ValueError,Conflict):pass
  self.assertEqual(called,[],'strict rejection must precede arbitrary deepcopy')
  self.assertEqual(self.counts(),(0,0,0))
if __name__=='__main__':unittest.main(verbosity=2)
