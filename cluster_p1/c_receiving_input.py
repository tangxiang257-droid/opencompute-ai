"""Authenticated B prepare-only control evidence; never caller-supplied hashes.

B role bootstrap must already bind its actual committed receipt before issuing
its RoleDescriptor. The input-ready action requires the exact InstalledStageRole and reads its
actual StageBOperation receipt; a generic RegisteredRole cannot supply it. The control reply proves an authenticated role statement, not model
correctness or a global anti-replay guarantee.
"""
import hashlib
from .role_control import CoordinatorControl
from .control_exchange import _need,_json
class CommittedBInput:
 def __init__(self,control):_need(type(control)is CoordinatorControl);self.control=control;self.descriptor=control.descriptor
 async def read_committed(self,d):
  c=self.control;_need(c.descriptor is self.descriptor and c.descriptor.role_node==d['b_node'])
  b=c.descriptor.binding
  expected=dict(owner=d['owner'],task=d['task'],session=d['session'],epoch=d['epoch'],stage=1,model=d['model'],layout=d['layout'],plan=d['plan_digest'],input_snapshot=d['input_snapshot'],token_digest=d['token_digest'],preprocessing=d['preprocessing'],device=d['b_placement']['device'],instance=d['b_placement']['instance'],declaration_digest=d['declaration_digest'])
  _need(all(b.get(k)==v for k,v in expected.items()))
  reply=await c.prepare();_need(reply['state']=='RESERVED' and reply['retired'] is False)
  reply=await c.input_ready();_need(reply['state']=='RESERVED' and reply['retired'] is False)
  proof=reply['input_ready'];_need(type(proof)is dict and set(proof)=={'input_digest','receipt_digest','resource_id'} and proof['resource_id']==d['b_placement']['resource_id'] and proof['input_digest']==b['input_digest'])
  return dict(input_digest=b['input_digest'],receipt_digest=proof['receipt_digest'],session=d['session'],epoch=d['epoch'],plan_digest=d['plan_digest'],tile=0)
