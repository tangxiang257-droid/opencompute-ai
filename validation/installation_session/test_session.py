"""Real original endpoints, ordinary bytes/SQLite, LAB identity/providers only."""
import unittest,importlib.util
class Entry(unittest.TestCase):
 def test_session_protocol_exists(self):
  self.assertIsNotNone(importlib.util.find_spec('cluster_p1.installation_session'))

import asyncio
from unittest.mock import patch
from validation.online_issuance.test_transport import Tests as Base
class SessionTests(Base):
 async def open(self,**kw):
  from cluster_p1.installation_session import SessionPolicy
  p=SessionPolicy(**kw)
  results=await asyncio.gather(self.client.install(session_policy=p),self.service.serve_once(session_policy=p),return_exceptions=True)
  for r in results:
   if isinstance(r,BaseException):raise r
  return self.client._session,self.service._session
 async def test_live_session_cannot_install_twice(self):
  a,b=await self.open(interval=.02,silence=.3)
  self.assertTrue(a.active and b.active)
  with self.assertRaises(Exception):await self.client.install()
 async def test_missing_policy_keeps_legacy_reader_behavior(self):
  await self.exchange();self.assertIsNone(getattr(self.cc,'_session_idle_owner',None))
 async def test_policy_mismatch_closes_and_never_retries(self):
  from cluster_p1.installation_session import SessionPolicy
  r=await asyncio.gather(self.client.install(session_policy=SessionPolicy(interval=.02,silence=.3)),self.service.serve_once(session_policy=SessionPolicy(interval=.03,silence=.3)),return_exceptions=True)
  self.assertTrue(any(isinstance(v,BaseException) for v in r));self.assertTrue(self.cc._closed or self.sc._closed)
  self.assertEqual(self.authority.db.execute('select count(*) from slots').fetchone(),(1,))
 async def test_revocation_with_live_transport_still_denies(self):
  a,b=await self.open(interval=.02,silence=.3);self.permission.revoked=True
  with self.assertRaises(Exception):self.client.live_guard()
  self.assertEqual(self.authority.db.execute('select count(*) from slots').fetchone(),(1,))
 async def test_reader_cancel_seals_original_connection(self):
  a,b=await self.open(interval=.02,silence=.3);self.cc._task.cancel();await asyncio.gather(self.cc._task,return_exceptions=True)
  with self.assertRaises(Exception):self.client.live_guard()
  await self.client.close();self.assertTrue(a.task.done())
 async def test_close_joins_both_owned_monitor_and_reader(self):
  a,b=await self.open(interval=.02,silence=.3)
  await asyncio.gather(self.client.close(),self.service.close());self.assertTrue(a.task.done() and b.task.done());self.assertTrue(self.cc._task.done() and self.sc._task.done())

 async def test_idle_boundary_outlives_old_header_deadline(self):
  self.cc.timeout=.1;self.sc.timeout=.1
  a,b=await self.open(interval=.02,silence=.6)
  entered=asyncio.Event();release=asyncio.Event();old=a.send
  async def paused(body):
   if body['kind']=='PING':entered.set();await release.wait()
   return await old(body)
  with patch.object(a,'send',paused):
   await asyncio.wait_for(entered.wait(),1)
   # Explicit idle-duration experiment; event barrier decides admission order.
   timer=asyncio.get_running_loop().call_later(.2,release.set)
   try:
    await release.wait();self.client.live_guard();self.assertFalse(self.cc._task.done());self.assertFalse(self.sc._task.done())
   finally:timer.cancel();release.set()
 async def test_missing_pong_is_terminal_unknown_not_reissue(self):
  a,b=await self.open(interval=.02,silence=.3)
  entered=asyncio.Event();release=asyncio.Event();old=b.send
  async def drop(body):
   if body['kind']=='PONG':entered.set();await release.wait()
   return await old(body)
  with patch.object(b,'send',drop):
   await asyncio.wait_for(entered.wait(),1)
   await asyncio.wait_for(asyncio.shield(a.task),2)
   self.assertFalse(a.active);self.assertTrue(self.cc._closed)
   self.assertEqual(self.authority.db.execute('select count(*) from slots').fetchone(),(1,))
   with self.assertRaises(Exception):await self.client.install()
   release.set()
 async def test_wrong_pong_challenge_retires_original(self):
  a,b=await self.open(interval=.02,silence=.3);old=b.send
  async def corrupt(body):
   if body['kind']=='PONG':body=dict(body,nonce='0'*64)
   return await old(body)
  with patch.object(b,'send',corrupt):await asyncio.wait_for(asyncio.shield(a.task),2)
  self.assertFalse(a.active);self.assertTrue(self.cc._closed)
 async def test_expired_freshness_denies_synchronously_before_scheduled_task(self):
  a,b=await self.open(interval=.02,silence=.3);a.deadline=a.loop.time()-1
  with self.assertRaises(Exception):self.client.live_guard()
  self.assertFalse(a.active);self.assertTrue(self.cc._closed)
 async def test_close_repeated_cancellation_keeps_join(self):
  a,b=await self.open(interval=.02,silence=.6)
  entered=asyncio.Event();cancelled=asyncio.Event();release=asyncio.Event();old=a.send
  async def pause(body):
   if body['kind']=='PING':
    entered.set()
    try:await release.wait()
    except asyncio.CancelledError:
     cancelled.set();await release.wait();raise
   return await old(body)
  with patch.object(a,'send',pause):
   await asyncio.wait_for(entered.wait(),1)
   closing=asyncio.create_task(self.client.close());await asyncio.wait_for(cancelled.wait(),1)
   closing.cancel();checkpoint=asyncio.Event();asyncio.get_running_loop().call_soon(checkpoint.set);await checkpoint.wait()
   closing.cancel();self.assertFalse(closing.done());self.assertFalse(a.task.done())
   release.set();await asyncio.gather(closing,return_exceptions=True)
  self.assertTrue(a.task.done());self.assertTrue(self.cc._task.done())

 async def test_partial_header_still_has_frame_deadline(self):
  self.cc.timeout=.1;self.sc.timeout=.1
  a,b=await self.open(interval=.02,silence=.6)
  entered=asyncio.Event();release=asyncio.Event();old=a.send
  async def pause(body):
   if body['kind']=='PING':entered.set();await release.wait()
   return await old(body)
  with patch.object(a,'send',pause):
   await asyncio.wait_for(entered.wait(),1)
   self.cc.w.write(b'D')
   await asyncio.wait_for(asyncio.shield(self.sc._task),1)
   self.assertIsInstance(self.sc._error,TimeoutError)
   release.set()
 async def test_other_task_cannot_use_opening_internal_context(self):
  from cluster_p1.installation_session import _internal
  entered=asyncio.Event();release=asyncio.Event();old=self.service.send
  # Successful install still requires explicit session protocol; internal context
  # cannot be inherited as a qualification by a provider-created task.
  a,b=await self.open(interval=.02,silence=.3)
  a.phase='OPENING';token=_internal.set((a,asyncio.current_task()))
  try:
   async def child():
    with self.assertRaises(Exception):self.client.current()
   await asyncio.create_task(child())
  finally:_internal.reset(token)
  self.assertFalse(a.active)

 async def test_monotonic_rollback_is_terminal(self):
  a,b=await self.open(interval=.02,silence=.6)
  self.client.live_guard()
  now=a.loop.time()
  with patch.object(a.loop,'time',return_value=now-1):
   with self.assertRaises(Exception):self.client.live_guard()
  self.assertFalse(a.active)
 async def test_writer_close_failure_still_joins_monitor(self):
  a,b=await self.open(interval=.02,silence=.6)
  entered=asyncio.Event();release=asyncio.Event();old=a.send;calls=[]
  async def paused(body):
   if body['kind']=='PING':entered.set();await release.wait()
   return await old(body)
  def broken():calls.append(True);raise OSError('injected writer close failure')
  try:
   with patch.object(a,'send',paused):
    await asyncio.wait_for(entered.wait(),1)
    with patch.object(self.cc.w,'close',broken):
     try:await self.client.close()
     except BaseException:pass
    self.assertTrue(calls,'fault not injected')
    self.assertTrue(a.task.done(),'close returned while owned monitor remains alive')
  finally:
   release.set();await asyncio.gather(self.client.close(),return_exceptions=True)
 async def test_boolean_protocol_version_is_rejected(self):
  a,b=await self.open(interval=.02,silence=.6)
  accepted=asyncio.Event();injected=asyncio.Event();old_send=b.send;old_refresh=a.refresh
  async def corrupt(body):
   if body['kind']=='PONG':body=dict(body,version=True);injected.set()
   return await old_send(body)
  def refresh():
   if injected.is_set():accepted.set()
   return old_refresh()
  with patch.object(b,'send',corrupt),patch.object(a,'refresh',refresh):
   wait=asyncio.create_task(accepted.wait())
   try:
    await asyncio.wait_for(asyncio.wait((wait,a.task),return_when=asyncio.FIRST_COMPLETED),1)
    self.assertTrue(injected.is_set(),'fault not injected')
    self.assertFalse(accepted.is_set(),'boolean version accepted and freshness renewed')
   finally:
    wait.cancel();await asyncio.gather(wait,return_exceptions=True)
 async def test_natural_idle_crosses_original_ten_second_boundary(self):
  a,b=await self.open();done=asyncio.Event();count=0;old=a.refresh
  before=self.authority.db.execute('select * from slots').fetchall();start=a.loop.time()
  def refresh():
   nonlocal count
   old();count+=1
   if count==7:done.set()
  with patch.object(a,'refresh',refresh):await asyncio.wait_for(done.wait(),16)
  self.assertGreater(a.loop.time()-start,self.cc.timeout)
  self.client.live_guard();self.service.current()
  self.assertEqual(self.authority.db.execute('select * from slots').fetchall(),before)

 async def test_legacy_header_already_waiting_before_endpoint_construction(self):
  from validation.online_issuance.test_transport import streams,config,Channel,IssuanceClient,IssuanceService
  grants=set(self.cc.grants);await self.cleanup()
  cr,cw,sr,sw=streams('a','issuer');entered={cr:asyncio.Event(),sr:asyncio.Event()};old=asyncio.StreamReader.readexactly
  async def observed(reader,n):
   if reader in entered and n==5:entered[reader].set()
   return await old(reader,n)
  with patch.object(asyncio.StreamReader,'readexactly',observed):
   self.cc=Channel(cr,cw,config('issuer'),{'issuer':grants},capacity_bytes=32768,capacity_tiles=1)
   self.sc=Channel(sr,sw,config('a'),{'a':grants},capacity_bytes=32768,capacity_tiles=1)
   await asyncio.wait_for(asyncio.gather(*(e.wait() for e in entered.values())),1)
   self.client=IssuanceClient(self.cc,self.store,self.descriptor,completion=self.completion,**self.kw)
   self.service=IssuanceService(self.sc,self.authority,self.registration,self.descriptor,**self.kw)
   a,b=await self.open(interval=.02,silence=.6)
   self.assertTrue(a.active and b.active);self.client.live_guard()

 async def test_cancel_before_monitor_first_step_closes_partial_header(self):
  original=asyncio.create_task;hit=[]
  def spawn(coro,*args,**kwargs):
   frame=getattr(coro,'cr_frame',None);session=None if frame is None else frame.f_locals.get('self')
   target=getattr(coro,'__qualname__','')=='_Session.run' and getattr(session,'endpoint',None) is self.client
   if target:self.cc.r.feed_data(b'D')
   task=original(coro,*args,**kwargs)
   if target:hit.append(task);task.cancel()
   return task
  try:
   with patch.object(asyncio,'create_task',spawn):await self.open(interval=.02,silence=.6)
  except BaseException:
   self.assertTrue(hit);self.assertTrue(self.cc._closed);return
  self.assertTrue(hit);await asyncio.gather(hit[0],return_exceptions=True)
  self.assertTrue(self.cc._closed)
  with self.assertRaises(Exception):self.client.live_guard()

def load_tests(loader,tests,pattern):
 return unittest.TestSuite([Entry('test_session_protocol_exists')]+[SessionTests(n) for n in SessionTests.__dict__ if n.startswith('test_')])
if __name__=='__main__':unittest.main()
