from validation.hidden_host.test_pipeline import *
from validation.hidden_host.test_pipeline import Flow as Base
from private_b_hidden_host import BHiddenHost
from cluster_p1.control_exchange import _json

class Faults(Base):
 async def test_replaced_receiver_never_called(self):
  calls=[]
  class Replacement:
   async def receive(self):calls.append('called');return {}
  self.hidden_host.receiver=Replacement()
  with self.assertRaises(Exception):await self.hidden_host.receive_once(enabled=True)
  self.assertEqual(calls,[])
 async def test_missing_provider_constructor(self):
  for field in ('identity','authorize','resources'):
   kw=dict(self.hidden_kw);kw[field]=None
   with self.assertRaises(Exception):BHiddenHost(*self.hidden_args,**kw)
 async def test_wrong_declared_A_rejected_before_socket(self):
  args=list(self.hidden_args);c=json.loads(args[0]);c['peer']='impostor';c['peer_certificate']=hashlib.sha256(b'impostor').hexdigest()
  args[0]=_json(c);args[1]=hashlib.sha256(args[0]).hexdigest();args[4]=wire.config('impostor')
  with self.assertRaises(Exception):BHiddenHost(*args,**self.hidden_kw)
 async def test_no_reinstall_or_second_host(self):
  with self.assertRaises(Exception):await self.hidden_host.install(enabled=True)
  other=BHiddenHost(*self.hidden_args,**self.hidden_kw)
  with patch.object(self.hidden_module.socket,'socket',side_effect=AssertionError('must not bind')):
   with self.assertRaises(ValueError):await other.start(enabled=True)
  self.assertFalse(other.claimed);self.assertFalse(self.launchers[1]._closing)
 async def test_removed_original_issuance_rejects_receive(self):
  h=self.hidden_host;before=h.resources.query(h.ticket.reservation_id)
  h.startup.store.issued.pop(id(h.startup.journal))
  with self.assertRaises(Exception):await h.receive_once(enabled=True)
  after=h.resources.query(h.ticket.reservation_id)
  self.assertEqual(after['state'],'UNKNOWN');self.assertEqual(before['charge'],after['charge']);self.assertEqual(self.calls,[[],[]])
 async def test_changed_identity_provider_rejects_receive(self):
  self.hidden_host.identity=Identity()
  with self.assertRaises(Exception):await self.hidden_host.receive_once(enabled=True)
  self.assertEqual(self.calls,[[],[]]);self.assertEqual(self.hidden_host.resources.query(self.hidden_host.ticket.reservation_id)['state'],'UNKNOWN')
 async def test_repeated_receive_cannot_commit_again(self):
  await self.execute_a()
  with self.assertRaises(Exception):await self.hidden_host.receive_once(enabled=True)
  self.assertEqual(self.hidden_host.state,'RECEIVED')
 async def test_external_close_quarantines_before_blocked_writer(self):
  h=self.hidden_host;writer=h.slot.writer;entered=asyncio.Event();release=asyncio.Event();old=writer.wait_closed
  async def blocked():entered.set();await release.wait()
  writer.wait_closed=blocked
  task=asyncio.create_task(self.launchers[1].close())
  try:
   await asyncio.wait_for(entered.wait(),3)
   self.assertEqual(h.resources.query(h.ticket.reservation_id)['state'],'UNKNOWN');self.assertFalse(task.done())
  finally:release.set();writer.wait_closed=old
  await asyncio.wait_for(task,5)

def load_tests(*args):return unittest.TestSuite(Faults(x) for x in Faults.__dict__ if x.startswith('test_'))
if __name__=='__main__':unittest.main()
