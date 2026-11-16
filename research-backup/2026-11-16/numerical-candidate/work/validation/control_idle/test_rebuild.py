import asyncio,time,unittest
from cluster_p1.channel import Channel,Delivery
from cluster_p1.data_session import DataSession,DataSessionPolicy
from validation.role_bridge.test_transport import streams,config
from validation.data_session.test_core import Provider
class Rebuild(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):self.channels=[];self.p=Provider()
 async def asyncTearDown(self):
  for c in self.channels:await c.close()
 def channel(self,**kw):
  r,w,_,_=streams('a','b');c=Channel(r,w,config('b'),{},timeout=.05,**kw);self.channels.append(c);return c
 def session(self,c):
  return DataSession._attach(c,{'test':'ordinary'},DataSessionPolicy(.01,.1,.3),identity=self.p,authorize=self.p,check=lambda:None,purpose='control-session',dormant=True)
 async def test_optin(self):
  c=self.channel(_idle_aware=True);self.assertTrue(c._idle_aware)
  with self.assertRaises(AttributeError):c._idle_aware=False
  s=self.session(c);self.assertEqual(s.binding['purpose'],'control-session')
 async def test_noopt_rejected(self):
  c=self.channel()
  with self.assertRaises(Exception):self.session(c)
 async def test_late_attach(self):
  c=self.channel(_idle_aware=True);await asyncio.sleep(.01);time.sleep(.06)
  with self.assertRaises(Exception):self.session(c)
 async def test_finite_idle(self):
  c=self.channel(_idle_aware=True);s=self.session(c);s.phase='ACTIVE'
  await asyncio.wait_for(c._task,.4);self.assertTrue(c._closed)
 async def test_postpop_revoke(self):
  c=self.channel(_idle_aware=True);s=self.session(c);s.phase='ACTIVE';d=Delivery(c,object(),4);c._queue.put_nowait(d);c._used_bytes=4;c._used_tiles=1
  pop=c._queue.get_nowait
  def revoked():
   x=pop();s._fail(RuntimeError('test revoke'));return x
  c._queue.get_nowait=revoked
  with self.assertRaises(Exception):await c.receive(_session_owner=s)
  self.assertEqual(c._undelivered,[d]);self.assertEqual(c._used_bytes,4);self.assertFalse(d._released)
 async def test_legacy_queued(self):
  c=self.channel();d=Delivery(c,object(),4);c._queue.put_nowait(d);c._abort()
  self.assertIs(await c.receive(),d);self.assertEqual(c._undelivered,[]);self.assertFalse(d._released)

 async def test_same_turn_revoke(self):
  c=self.channel(_idle_aware=True);s=self.session(c);s.phase='ACTIVE';entered=asyncio.Event();wait=c._condition.wait
  async def observed():entered.set();return await wait()
  c._condition.wait=observed;task=asyncio.create_task(c.receive(_session_owner=s));await entered.wait()
  d=Delivery(c,object(),4)
  async with c._condition:
   c._queue.put_nowait(d);c._used_bytes=4;c._used_tiles=1;s._fail(RuntimeError('same turn'));c._condition.notify_all()
  with self.assertRaises(Exception):await task
  self.assertTrue(d in c._undelivered or d in list(c._queue._queue));self.assertEqual(c._used_bytes,4);self.assertFalse(d._released)
 async def test_cancel_after_get(self):
  from unittest.mock import patch
  import cluster_p1.channel as module
  c=self.channel();d=Delivery(c,object(),4);c._queue.put_nowait(d);c._used_bytes=4;c._used_tiles=1
  obtained=asyncio.Event();release=asyncio.Event();original=asyncio.wait_for
  async def barrier(awaitable,*a,**kw):
   target=getattr(getattr(awaitable,'cr_code',None),'co_qualname','')=='Channel.receive.<locals>.get'
   value=await original(awaitable,*a,**kw)
   if target:obtained.set();await release.wait()
   return value
  with patch.object(module.asyncio,'wait_for',barrier):
   task=asyncio.create_task(c.receive());await obtained.wait();task.cancel()
   with self.assertRaises(asyncio.CancelledError):await task
  self.assertEqual(c._undelivered,[d]);self.assertTrue(c._queue.empty());self.assertEqual(c._used_bytes,4);self.assertFalse(d._released)
 async def test_attach_after_reader_started(self):
  c=self.channel(_idle_aware=True);await asyncio.sleep(.005);s=self.session(c);s.phase='ACTIVE'
  await asyncio.sleep(.06);self.assertFalse(c._closed);s.check()
 async def test_flag_exact_bool(self):
  for value in (0,1,None,'yes'):
   with self.assertRaises(TypeError):self.channel(_idle_aware=value)
