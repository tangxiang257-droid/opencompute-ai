import asyncio,unittest
from unittest.mock import patch
from validation.hidden_host.test_pipeline import Flow
from private_experiment_actions import ActionGate
class Authority(Flow):
 async def test_actual_task_revoke_before_start(self):
  self.assertTrue(self.a_session.session.active and self.hidden_host.data_session.active)
  await super().test_revoke_before_start_blocks_real_control_body()
  await self.settle_expected_role_failure()
 async def test_actual_task_revoke_during_thread(self):
  self.assertTrue(self.a_session.session.active and self.hidden_host.data_session.active)
  await super().test_revoke_while_ordinary_body_running_blocks_commit_not_stop()
 async def test_matched_pulse_cannot_refresh_consumed_short_permit(self):
  _,control=await self.prepare_a();gate=self.launchers[0].actions;session=self.a_session.session
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
