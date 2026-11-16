"""Synthetic USER/fence integration only. Real pinned reader, Authority and SQLite.

No implementation here is production identity verification or a real IdP.
"""
import asyncio, dataclasses, importlib.abc, inspect, os, sqlite3, sys, threading, unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
class Block(importlib.abc.MetaPathFinder):
 def find_spec(self,name,*args):
  if name.split('.')[0] in {'torch','transformers','safetensors','tensorflow','public_pool'} or 'worker' in name.lower():raise AssertionError('offline import denied')
def guard(event,args):
 if event in ('socket.bind','socket.connect','socket.getaddrinfo','socket.gethostbyname','socket.gethostbyaddr','socket.getnameinfo','subprocess.Popen','os.system','os.fork','os.posix_spawn','os.exec'):raise AssertionError('offline event denied')
sys.meta_path.insert(0,Block());sys.addaudithook(guard)
from validation.input_declaration.test_declaration import Checks as Fixture, MODEL, CONTRACT
from input_declaration import DeclarationRejected
from token_input_snapshot import publish_input
from cluster_p1.nonce_coordinator import NonceCoordinator
from cluster_p1.store import Conflict
try:import private_source_bridge as bridge
except ImportError:bridge=None

class Checks(Fixture):
 async def asyncSetUp(self):
  self.assertIsNotNone(bridge,'private bridge missing on fixed baseline')
  await super().asyncSetUp();self.opened=[];self.path=self.root/'source-coordinator'
  base=self.users
  class SyntheticFence(bridge.LocalUserFence):
   """TEST SUBSTITUTE ONLY: no genuine credentials/issuer/expiry verification."""
   def __init__(self):self.lock=threading.RLock();self.depth=0;self.enter_fail=False;self.exit_fail=False;self.suppress=False;self.expired=False;self.on_enter=None;self.on_exit=None
   def verify(self,credential):return None if self.expired else base.verify(credential)
   @contextmanager
   def hold(self,credential):
    with self.lock:
     if self.enter_fail:raise RuntimeError('synthetic enter fault')
     self.depth+=1
     try:
      if self.on_enter:self.on_enter()
      try:yield
      except BaseException:
       if not self.suppress:raise
     finally:
      self.depth-=1
      if self.on_exit:self.on_exit()
      if self.exit_fail:raise RuntimeError('synthetic exit fault')
  self.fence=SyntheticFence();self.service._verifier=self.fence
 async def asyncTearDown(self):
  for b in getattr(self,'opened',[]):
   if b._cleanup_failed:
    with self.assertRaises(bridge.SourceBridgeRejected):await b.close()
   else:await b.close()
  await super().asyncTearDown()
 async def open(self,**kwargs):
  args=dict(service=self.service,model=MODEL,preprocessing_version='declared-test-v1',contract_sha256=CONTRACT,source_namespace='f'*64,coordinator_path=self.path,identity_fence=self.fence,credential='alice-token');args.update(kwargs)
  b=await bridge._open_source_bridge(**args);self.opened.append(b);return b
 async def register(self,b,**kwargs):
  args=dict(credential='alice-token',task_id=self.task.task_id);args.update(kwargs)
  return await b.register_current_source(**args)
 def counts(self):
  with sqlite3.connect(self.path) as c:return tuple(c.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in ('sessions','source_claims','slots'))
 async def test_default_disabled_and_missing_contract_does_not_create_file(self):
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(bridge._CurrentSourceBridge())
  for provider in (None,self.users):
   with self.assertRaises(bridge.SourceBridgeRejected):await self.open(identity_fence=provider)
  self.assertFalse(self.path.exists())
 async def test_namespace_required_never_silently_initializes_v1(self):
  for namespace in (None,'','bad'):
   with self.assertRaises(bridge.SourceBridgeRejected):await self.open(source_namespace=namespace)
   self.assertFalse(self.path.exists())
 async def test_success_current_source_inert_same_authorizer(self):
  b=await self.open();original=b._co._authorize;changes=self.ledger._db.total_changes
  r=await self.register(b);self.assertFalse(r.execution_allowed);self.assertEqual(r.task_id,self.task.task_id)
  q=await b.query_history('alice-token',self.task.task_id)
  self.assertTrue(q['exists']);self.assertFalse(q['execution_allowed']);self.assertEqual(q['nonce'],r.nonce)
  self.assertEqual(self.counts(),(1,1,0));self.assertEqual(b._co._reservations,{})
  self.assertIs(b._co._authorize,original);self.assertEqual(self.ledger._db.total_changes,changes)
 async def test_input_cannot_supply_owner_declaration_nonce(self):
  b=await self.open()
  for extra in ({'owner':'alice'},{'declaration':await self.prepare()},{'nonce':'a'*32}):
   with self.assertRaises(TypeError):await b.register_current_source('alice-token',self.task.task_id,**extra)
  self.assertEqual(self.counts(),(0,0,0));await self.register(b)
 async def test_foreign_and_missing_source_reject_without_disabling_other_work(self):
  b=await self.open()
  for args in ({'credential':'bob-token'},{'task_id':'0'*32}):
   with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b,**args)
  self.assertEqual(self.counts(),(0,0,0));await self.register(b)
 async def test_cancelled_source_rejected_before_write(self):
  b=await self.open();await self.a.cancel(self.owner,self.task.task_id)
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(0,0,0));self.assertFalse(b._co._closed)
 async def test_snapshot_corruption_rejects_then_other_work_is_possible(self):
  b=await self.open();p=self.root/'inputs'/(self.ref.digest+'.tokens');raw=p.read_bytes();os.chmod(p,0o600);p.write_bytes(raw[:-1]+bytes([raw[-1]^1]))
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(0,0,0));p.write_bytes(raw);os.chmod(p,0o400);await self.register(b)
 async def test_cancel_waiting_authority_has_no_write_and_does_not_retire(self):
  b=await self.open();await self.a._lock.acquire()
  job=asyncio.create_task(self.register(b));await asyncio.sleep(0);job.cancel()
  with self.assertRaises(asyncio.CancelledError):await job
  self.a._lock.release();self.assertEqual(self.counts(),(0,0,0));await self.register(b)
 async def test_revoke_while_waiting_authority_rejected(self):
  b=await self.open();await self.a._lock.acquire();job=asyncio.create_task(self.register(b));await asyncio.sleep(0)
  self.users.revoked=True;self.a._lock.release()
  with self.assertRaises(bridge.SourceBridgeRejected):await job
  self.assertEqual(self.counts(),(0,0,0))
 async def test_fence_entry_failure_no_write(self):
  b=await self.open();self.fence.enter_fail=True
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(0,0,0))
 async def test_fence_exit_failure_preserves_commit_stops_bridge(self):
  b=await self.open();self.fence.exit_fail=True
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(1,1,0));self.fence.exit_fail=False
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertTrue(b._co._closed)
 async def test_suppressing_fence_cannot_convert_write_failure_to_success(self):
  b=await self.open();self.fence.suppress=True
  with patch.object(b._co,'_commit',side_effect=RuntimeError('injected before commit')):
   with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(0,0,0));self.assertTrue(b._co._closed)
 async def test_commit_before_and_after_effect_are_not_retried(self):
  for after in (False,True):
   with self.subTest(after=after):
    b=await self.open(coordinator_path=self.root/('co-'+str(after)));co=b._co;original=co._commit
    def fault(c):
     if after:original(c)
     raise RuntimeError('injected commit uncertainty')
    with patch.object(co,'_commit',side_effect=fault):
     with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
    with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
    self.assertTrue(co._closed)
    with sqlite3.connect(self.root/('co-'+str(after))) as c:
     self.assertEqual(c.execute('SELECT count(*) FROM source_claims').fetchone()[0],int(after))
 async def test_final_expiry_after_commit_discards_success(self):
  b=await self.open();original=b._co._commit
  def commit(c):original(c);self.fence.expired=True
  with patch.object(b._co,'_commit',side_effect=commit):
   with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(1,1,0));self.assertTrue(b._co._closed)
 async def test_identity_changes_during_coordinator_lock_wait(self):
  b=await self.open();held=threading.Event();checked=threading.Event();original=self.fence.verify;binding_started=False
  original_bind=b._co.bind_source_declaration
  def bind(*args):
   nonlocal binding_started
   binding_started=True
   return original_bind(*args)
  def verify(c):
   value=original(c)
   if binding_started:checked.set()
   return value
  self.fence.verify=verify
  def contender():
   with b._co._lock:
    held.set()
    if checked.wait(2):self.fence.expired=True
  t=threading.Thread(target=contender);t.start();self.assertTrue(held.wait(1))
  try:
   with patch.object(b._co,'bind_source_declaration',side_effect=bind):
    with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  finally:t.join(3)
  self.assertFalse(t.is_alive());self.assertTrue(b._co._closed);self.assertEqual(self.counts(),(1,1,0))
 async def test_source_transaction_fully_exited_before_bind(self):
  b=await self.open();original=b._co.bind_source_declaration
  def bind(*args):
   self.assertFalse(self.ledger._db.in_transaction);self.assertTrue(self.a._lock.locked());self.assertEqual(self.fence.depth,1)
   return original(*args)
  with patch.object(b._co,'bind_source_declaration',side_effect=bind):await self.register(b)
 async def test_source_transaction_exit_wrapper_failure_never_binds(self):
  b=await self.open();old=self.ledger._transaction
  @contextmanager
  def fault(*a,**kw):
   with old(*a,**kw):yield
   raise RuntimeError('wrapper after real normal transaction exit')
  with patch.object(self.ledger,'_transaction',fault):
   with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(0,0,0));self.assertTrue(b._co._closed)
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
 async def test_authority_closed_or_replaced_rejected(self):
  b=await self.open();await self.a.close()
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(0,0,0))
 async def test_verifier_replacement_refused(self):
  b=await self.open();self.service._verifier=self.users
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(0,0,0))
 async def test_duplicate_does_not_grant_or_disable_other_tasks(self):
  b=await self.open();await self.register(b)
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertFalse(b._co._closed);self.assertEqual(self.counts(),(1,1,0))
  binding=dataclasses.replace(self.binding,request_id='different-request')
  reference=publish_input(self.fds[1],binding,(1,2,3))
  async with self.a._lock:
   with self.ledger._transaction():
    other=self.ledger.commit_text(self.ledger._db,self.owner,'different-key',self.draft,1,b'ordinary text',CONTRACT,'d'*64,binding,reference)
  self.assertNotEqual(other.task_id,self.task.task_id)
  await self.register(b,task_id=other.task_id);self.assertEqual(self.counts(),(2,2,0))
 async def test_close_bridge_rejects_and_reopen_is_history_only(self):
  b=await self.open();d=await self.prepare();await self.register(b);await b.close()
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  @contextmanager
  def owner():yield self.owner
  co=NonceCoordinator.open_existing(self.path,authorize=owner,source_namespace='f'*64)
  try:
   self.assertTrue(co.query_source(self.task.task_id)['exists'])
   with self.assertRaises(Conflict):co.bind_source_declaration(d,'a'*32)
  finally:co.close()
 async def test_direct_coordinator_call_outside_identity_context_refused(self):
  b=await self.open();d=await self.prepare()
  with self.assertRaises(Exception):b._co.bind_source_declaration(d,'a'*32)
  self.assertEqual(self.counts(),(0,0,0))
 async def test_shared_helper_used_and_without_lock_rejected(self):
  b=await self.open();method=self.reader._read_locked
  with self.assertRaises(DeclarationRejected):method('alice-token',self.task.task_id,self.owner)
  # The two actual readers share the same class helper, not copied SQL checks.
  with patch.object(type(self.reader),'_read_locked',side_effect=DeclarationRejected()):
   with self.assertRaises(DeclarationRejected):await self.prepare()
   with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(0,0,0));await self.register(b)
 async def test_expiry_on_fence_exit_no_success_after_commit(self):
  b=await self.open();self.fence.on_exit=lambda:setattr(self.fence,'expired',True)
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(1,1,0));self.assertTrue(b._co._closed)
 async def test_authority_invalidated_after_commit_no_success(self):
  b=await self.open();original=b._co._commit
  def commit(c):original(c);self.a._poison()
  with patch.object(b._co,'_commit',side_effect=commit):
   with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(self.counts(),(1,1,0));self.assertTrue(b._co._closed)
 async def test_injected_cancellation_after_commit_propagates_and_stops(self):
  b=await self.open();original=b._co._commit
  def commit(c):original(c);raise asyncio.CancelledError()
  with patch.object(b._co,'_commit',side_effect=commit):
   with self.assertRaises(asyncio.CancelledError):await self.register(b)
  self.assertEqual(self.counts(),(1,1,0));self.assertTrue(b._co._closed)
 async def test_cleanup_failure_never_auto_retries_close(self):
  b=await self.open();original=b._co.close;calls=[];self.fence.exit_fail=True
  def close():calls.append('close');original();raise OSError('wrapper after real close')
  with patch.object(b._co,'close',side_effect=close):
   with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
   with self.assertRaises(bridge.SourceBridgeRejected):await b.close()
   self.assertEqual(calls,['close'])
  self.assertEqual(self.counts(),(1,1,0))
 async def test_suppressing_fence_cannot_hide_cancellation(self):
  b=await self.open();self.fence.suppress=True
  with patch.object(b._co,'_commit',side_effect=asyncio.CancelledError()):
   with self.assertRaises(asyncio.CancelledError):await self.register(b)
  self.assertEqual(self.counts(),(0,0,0));self.assertTrue(b._co._closed)
 async def test_initialization_exit_failure_never_overwrites_file(self):
  self.fence.exit_fail=True
  with self.assertRaises(bridge.SourceBridgeRejected):await self.open()
  self.assertTrue(self.path.exists());self.assertEqual(self.counts(),(0,0,0));before=self.path.read_bytes();self.fence.exit_fail=False
  with self.assertRaises(bridge.SourceBridgeRejected):await self.open()
  self.assertEqual(self.path.read_bytes(),before)
 async def test_nested_public_reader_not_used_by_bridge(self):
  b=await self.open()
  with patch.object(type(self.reader),'prepare_input_declaration',side_effect=AssertionError('must use synchronous shared core')):
   await self.register(b)
 async def test_reader_waiting_old_lock_rejects_replacement_authority(self):
  # Actual second Authority and ledger; its lock is owned by this test task.
  other=Fixture();await other.asyncSetUp()
  try:
   b=await self.open();await self.a._lock.acquire();await other.a._lock.acquire()
   job=asyncio.create_task(self.reader.prepare_input_declaration('alice-token',other.task.task_id))
   await asyncio.sleep(0);self.service._authority=other.a;self.a._lock.release()
   try:
    with self.assertRaises(DeclarationRejected):await job
    with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b,task_id=other.task.task_id)
    self.assertEqual(self.counts(),(0,0,0))
   finally:
    self.service._authority=self.a;other.a._lock.release()
  finally:
   await other.asyncTearDown()
   while other._cleanups:
    function,args,kwargs=other._cleanups.pop();function(*args,**kwargs)
 async def test_duck_typed_capability_not_a_fence_integration(self):
  class Duck:
   capability=True
   def verify(inner,c):return self.users.verify(c)
   @contextmanager
   def hold(inner,c):yield self.owner
  fake=Duck();self.service._verifier=fake
  with self.assertRaises(bridge.SourceBridgeRejected):await self.open(identity_fence=fake)
  self.assertFalse(self.path.exists())
 async def test_unstarted_hold_coroutine_closed_without_entering_body(self):
  b=await self.open();events=[]
  async def invalid_hold():
   try:events.append('body')
   finally:events.append('finally')
  context=invalid_hold();self.assertEqual(inspect.getcoroutinestate(context),inspect.CORO_CREATED)
  self.fence.hold=lambda credential:context
  try:
   with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
   self.assertEqual(events,[]);self.assertEqual(inspect.getcoroutinestate(context),inspect.CORO_CLOSED)
   self.assertEqual(self.counts(),(0,0,0))
  finally:context.close()
 async def test_suspended_hold_coroutine_rejected_without_finally_callback(self):
  b=await self.open();events=[]
  class Pause:
   def __await__(self):yield None
  async def invalid_hold():
   try:await Pause()
   finally:events.append('finally')
  context=invalid_hold();context.send(None)  # provider/test owns starting it
  self.assertEqual(inspect.getcoroutinestate(context),inspect.CORO_SUSPENDED)
  self.fence.hold=lambda credential:context
  try:
   with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
   self.assertEqual(events,[],'bridge must not invoke suspended coroutine finally')
   self.assertEqual(inspect.getcoroutinestate(context),inspect.CORO_SUSPENDED)
   self.assertEqual(self.counts(),(0,0,0))
  finally:context.close()  # provider cleanup, deliberately outside bridge
  self.assertEqual(events,['finally'])
 async def test_running_hold_coroutine_rejected_without_close(self):
  b=await self.open();events=[]
  async def provider_owned():
   self.assertEqual(inspect.getcoroutinestate(context),inspect.CORO_RUNNING)
   async with self.a._lock:
    # Use the actual identity boundary to distinguish its intended rejection
    # from coroutine.close's "already executing" ValueError.
    with self.assertRaises(bridge.SourceBridgeRejected):
     with b._identity('alice-token'):self.fail('async fence entered')
   events.append('provider continued')
  context=provider_owned();self.fence.hold=lambda credential:context
  await context
  self.assertEqual(events,['provider continued']);self.assertEqual(self.counts(),(0,0,0))
 async def test_custom_hold_awaitable_rejected_without_drive_or_close(self):
  b=await self.open();events=[]
  class Awaitable:
   def __await__(self):
    events.append('await');yield None
   def close(self):events.append('close')
  context=Awaitable();self.fence.hold=lambda credential:context
  with self.assertRaises(bridge.SourceBridgeRejected):await self.register(b)
  self.assertEqual(events,[]);self.assertEqual(self.counts(),(0,0,0))

if __name__=='__main__':
 names=[n for n in Checks.__dict__ if n.startswith('test_')]
 result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(Checks(n) for n in names))
 raise SystemExit(not result.wasSuccessful())
