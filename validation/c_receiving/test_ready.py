"""Actual installed B reads committed receipt; compute and outer permissions synthetic."""
import asyncio,unittest,hashlib
from validation.role_assembly.test_pipeline import Tests as Base
from validation.split_roles.test_pipeline import as_role
from cluster_p1.c_receiving_input import CommittedBInput
class Tests(Base):
 async def dispatch(self,role,coordinator):
  if role.startup.stage==1:
   s=role.operation.spec;binding=coordinator.descriptor.binding
   d=dict(owner=s.owner,task=s.request,session=s.session,epoch=s.epoch,model=s.model,layout=s.layout,plan_digest=s.plan_digest,input_snapshot=s.input_snapshot,token_digest=s.token_digest,preprocessing=s.preprocessing,b_node='b',b_placement={k:role.startup.descriptor[k] for k in ('device','instance','resource_id')},declaration_digest=binding['declaration_digest'])
   async def serve():
    for _ in range(2):await role.serve_once()
   before=len(self.role_loaders[1].calls)
   results=await asyncio.wait_for(asyncio.gather(as_role('b',serve()),as_role('coordinator',CommittedBInput(coordinator).read_committed(d))),5)
   self.assertEqual(len(self.role_loaders[1].calls),before)
   self.assertEqual(results[1]['input_digest'],hashlib.sha256(role.operation.received_bytes()).hexdigest())
   self.proof_seen=True
  return await super().dispatch(role,coordinator)
 async def test_actual_B_receipt_ready_before_projection_then_existing_chain(self):
  self.proof_seen=False;await self.online_bootstrap();await self.run_split();self.assertTrue(self.proof_seen)
if __name__=='__main__':
 r=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([Tests('test_actual_B_receipt_ready_before_projection_then_existing_chain')]));raise SystemExit(not r.wasSuccessful())
