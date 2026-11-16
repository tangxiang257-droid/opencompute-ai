"""Ordinary-memory fixture; no operations dispatched and no network listeners."""
import asyncio,unittest
from validation.action_session.test_pipeline import Flow
from cluster_p1.data_session import DataSessionPolicy
from cluster_p1.control_session import _Transport
class Control(Flow):
 async def host_inputs(self,stage,spec):
  out=await super().host_inputs(stage,spec)
  self.host_kw['control_session_policy']=DataSessionPolicy()
  self.dial_kw['control_session_policy']=DataSessionPolicy()
  return out
 async def asyncTearDown(self):
  self.assertEqual(self.consent.service._download_owner(self.token),self.owner)
  await self._cleanup_runtime()
  self.assertTrue(all(t.done() for t in self.jobs));self.assertTrue(all(c._task.done() for c in self.channels))
  self.assertEqual(self.consent.service._download_owner(self.token),self.owner)
  await super().asyncTearDown()
 async def test_control_chain_no_operation(self):
  await self.prepare_a();h=self.control_hosts[0];l=self.launchers[0]
  for e in (h.control._client,l.installed.role._service):self.assertTrue(e._control_session.active)
  _Transport(h,True).check();_Transport(l,False).check()
  self.assertEqual(self.calls,[[],[]])
 async def test_control_idle_beyond_original_timeout(self):
  await self.prepare_a();h=self.control_hosts[0];c=h.control._client.channel;start=asyncio.get_running_loop().time();event=asyncio.Event();write=c.w.write
  def observe(raw):
   write(raw)
   if raw[:1]==b'P' and asyncio.get_running_loop().time()-start>c.timeout:event.set()
  c.w.write=observe
  try:await asyncio.wait_for(event.wait(),c.timeout+8)
  finally:c.w.write=write
  self.assertTrue(h.control._client._control_session.active);self.assertTrue(self.launchers[0].installed.role._service._control_session.active);self.assertEqual(self.calls,[[],[]])
def load_tests(*args):return unittest.TestSuite(Control(n) for n in Control.__dict__ if n.startswith('test_'))
