import asyncio,unittest
from unittest.mock import patch
from validation.action_session.test_pipeline import Flow
from private_experiment_actions import ActionGate
class Authority(Flow):
 async def test_actual_task_revoke_before_start(self):
  _,control=await self.prepare_a();session=self.action_servers[0]._action_session
  await self.adm.revoke(self.token,self.task.task_id)
  await asyncio.wait_for(session.closed_event.wait(),10)
  try:
   receipt=await control.dispatch();self.assertEqual(receipt['state'],'UNKNOWN')
  except (PermissionError,ValueError) as error:
   self.assertTrue(self.launchers[0]._closing or self.launchers[0].actions.channel._closed)
  self.assertEqual(self.calls,[[],[]]);self.assertNotIn((0,'start'),self.action_rows())
  await self.settle_expected_role_failure()
 async def test_actual_task_revoke_during_thread(self):
  self.assertTrue(self.a_session.session.active and self.hidden_host.data_session.active)
  await super().test_revoke_while_ordinary_body_running_blocks_commit_not_stop()
 async def test_matched_pulse_cannot_refresh_consumed_short_permit(self):
  _,control=await self.prepare_a();gate=self.launchers[0].actions;session=gate._action_session
  old=ActionGate.prepare;activate=session._activate;matched=asyncio.Event();seen=[]
  async def prepare(g,action,*args):
   result=await old(g,action,*args)
   if g is gate and action=='start':
    before=session.sequence;self.clock.advance(4)
    def observed():
     activate()
     if session.sequence>before and session.pending is None:matched.set()
    with patch.object(session,'_activate',observed):await asyncio.wait_for(matched.wait(),5)
    session.check();seen.append(session.active)
   return result
  with patch.object(ActionGate,'prepare',prepare):receipt=await control.dispatch()
  self.assertEqual(seen,[True]);self.assertEqual(receipt['state'],'UNKNOWN');self.assertEqual(self.calls,[[],[]])
  self.assertNotIn((0,'commit'),self.action_rows());await self.settle_expected_role_failure()
def load_tests(*args):return unittest.TestSuite(Authority(n) for n in Authority.__dict__ if n.startswith('test_'))
if __name__=='__main__':unittest.main()
