"""Descriptor/ordinary-byte limits, no Tensor/model execution."""
import unittest,json
from dataclasses import FrozenInstanceError
from test_p1_tile_contract_v2 import fixture
from cluster_p1.envelope import Envelope
from cluster_p1.tile_contract_v2 import TileV2
from cluster_p1.store import Conflict
try:
 from cluster_p1.payload_budget import assess_payload_budget
except ImportError:
 assess_payload_budget=None

class Tests(unittest.TestCase):
 def setUp(self):self.assertIsNotNone(assess_payload_budget,'payload preflight missing')
 def plan(self,hidden=24,vocab=32,counts=(2,2,1),task_id=None):
  t,s,st,tiles=fixture();tiles=tuple(TileV2.model_validate(x) for x in tiles);s=s.model_copy(update={'expected_tokens':sum(counts)})
  pairs=[];start=0
  for k,n in enumerate(counts):
   for stage in (0,1):
    old=tiles[stage];inp=old.input.model_copy(update={'shape':(1,n) if stage==0 else (1,n,hidden)})
    out=old.output.model_copy(update={'shape':(1,n,hidden if stage==0 else vocab)})
    pairs.append(old.model_copy(update={'tile_id':k,'token_start':start,'token_count':n,'input':inp,'output':out}))
   start+=n
  if task_id:
   t=t.model_copy(update={'task_id':task_id});s=s.model_copy(update={'task_id':task_id})
   pairs=[x.model_copy(update={'task_id':task_id}) for x in pairs]
  return t,s,st,tuple(pairs),hidden,vocab
 def report(self,args=None,wire=2):
  t,s,st,tiles,h,v=args or self.plan()
  return assess_payload_budget(t,s,st,tiles,hidden_size=h,vocab_size=v,wire_layout_version=wire)
 def test_full_logits_tail_and_retained_accounting(self):
  r=self.report();self.assertEqual(r.local_retained_bytes,1640)
  self.assertEqual(r.logits_bytes,(256,256,128));self.assertEqual(r.hidden_bytes,(192,192,96))
  self.assertEqual(r.result_set_bytes,640);self.assertEqual(r.result_set_frames,3)
  self.assertFalse(r.execution_allowed);self.assertFalse(r.payload_verified)
  self.assertEqual(r.output_semantics,'full-logits-f32-le-c-batch1-v1')
  r.require_local();r.require_handoff()
 def test_frame_estimate_matches_real_encoded_synthetic_bytes(self):
  args=self.plan(task_id='x'*128);r=self.report(args);tiles=args[3]
  sizes=[]
  for t in tiles[::2]:
   e=Envelope(t.task_id,t.session_id,t.epoch,0,t.tile_id,(t.token_start,t.token_start+t.token_count),t.model_hash,2,t.output.shape,'float32',bytes(t.token_count*24*4))
   sizes.append(len(e.encode()))
  self.assertEqual(r.hidden_frame_bytes,tuple(sizes))
  self.assertEqual(r.receiver_retained_bytes,sum(sizes)+480+640)
 def test_model_full_logits_not_truncated_or_implicitly_chunk_enabled(self):
  r=self.report(self.plan(hidden=896,vocab=151936,counts=(1,)))
  self.assertEqual(r.logits_bytes,(607744,));self.assertEqual(r.result_set_frames,19)
  self.assertTrue(r.framed_results_fit)
  with self.assertRaises(Conflict):r.require_local()
  with self.assertRaises(Conflict):r.require_handoff()
 def test_framed_result_total_three_vs_four_token_limit(self):
  a=self.report(self.plan(hidden=896,vocab=151936,counts=(1,1,1)))
  b=self.report(self.plan(hidden=896,vocab=151936,counts=(1,1,1,1)))
  self.assertEqual(a.result_set_bytes,1823232);self.assertTrue(a.framed_results_fit)
  self.assertEqual(b.result_set_bytes,2430976);self.assertFalse(b.framed_results_fit)
 def test_local_exact_object_cap_does_not_imply_frame_fits(self):
  r=self.report(self.plan(hidden=4096,vocab=32,counts=(4,)))
  self.assertEqual(r.hidden_bytes,(65536,));r.require_local()
  self.assertGreater(r.hidden_frame_bytes[0],65536)
  with self.assertRaises(Conflict):r.require_handoff()
 def test_local_cumulative_cap_and_no_budget_override(self):
  r=self.report();r.require_local(max_reserved=1640)
  with self.assertRaises(Conflict):r.require_local(max_reserved=1639)
  for kw in ({'max_reserved':262145},{'max_bytes':65537},{'max_bytes':True}):
   with self.assertRaises(Conflict):r.require_local(**kw)
 def test_receiver_full_frames_are_additional_retained_bytes(self):
  r=self.report(self.plan(hidden=4096,vocab=32,counts=(3,3)))
  r.require_local()
  self.assertTrue(all(x<65536 for x in r.hidden_frame_bytes))
  # Receiver budget is separately accounted, not assumed equal to local total.
  self.assertEqual(r.receiver_retained_bytes,sum(r.hidden_frame_bytes)+98304+768)
 def test_identity_mismatch_and_bad_wire_version_fail(self):
  args=list(self.plan());tiles=list(args[3]);tiles[1]=tiles[1].model_copy(update={'epoch':2});args[3]=tuple(tiles)
  with self.assertRaises(ValueError):self.report(args)
  for wire in (True,0,1,-1,2**31):
   with self.assertRaises(ValueError):self.report(wire=wire)
 def test_receiver_cumulative_cap_separate_from_local(self):
  r=self.report(self.plan(hidden=480,counts=(1,)*64));r.require_local()
  self.assertGreater(r.receiver_retained_bytes,262144)
  with self.assertRaises(Conflict):r.require_handoff()
 def test_report_immutable(self):
  r=self.report()
  with self.assertRaises(FrozenInstanceError):r.local_retained_bytes=0

if __name__=='__main__':unittest.main()
