"""v0.4.8 bounded multi-request Qwen2 pipeline and local Gloo tests.

No cloud, pretrained download, or remote Worker. All model weights synthesized
on localhost and independently checked against the dense forward pass.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import torch

from benchmarks.qwen2_multi_request_v048 import (
    PROMPTS, launch_group, run_comparison)
from compute.qwen2_chunked import export_chunked
from compute.qwen2_pipeline_batch_v048 import (
    MAX_REQUESTS, kv_bytes, load_requests, projected_kv_bytes,
    validate_requests)
from compute.qwen2_pipeline_v047 import partition_one_rank_v2
from compute.qwen2_reference import generate_dense
from compute.qwen2_shards import synthetic_state
from compute.qwen2_validation import validate_generation_parity


class BoundedBatchContracts(unittest.TestCase):
    def setUp(self):
        self.meta = {"vocab": 64, "positions": 64,
                     "layers": 4, "head_dim": 4, "kv_heads": 2}
        self.small = [{"id": "one", "prompt_ids": [1, 2, 3]},
                      {"id": "two", "prompt_ids": [4, 5]}]

    def test_valid_requests_have_stable_unique_ids_and_different_lengths(self):
        self.assertEqual(validate_requests(self.small, self.meta, 3), self.small)
        self.assertEqual(len(validate_requests([self.small[0]], self.meta, 3)), 1)
        self.assertEqual(MAX_REQUESTS, 4)

    def test_reject_unbounded_invalid_and_duplicate_requests(self):
        invalid = [
            [], [{}], [{"id": "", "prompt_ids": [1]}],
            [{"id": "../evil", "prompt_ids": [1]}],
            self.small + [self.small[0]],
            [{"id": "bad", "prompt_ids": [-1]}],
            [{"id": "bad", "prompt_ids": [64]}],
            [{"id": "bad", "prompt_ids": [True]}],
            [{"id": "bad", "prompt_ids": []}],
            [{"id": "bad", "prompt_ids": [1], "other": "no"}],
            [{"id": f"r{i}", "prompt_ids": [1]} for i in range(5)],
            [{"id": "bad", "prompt_ids": [1] * 129}],
        ]
        for request in invalid:
            with self.subTest(request=request):
                with self.assertRaises(ValueError):
                    validate_requests(request, self.meta, 3)
        with self.assertRaises(ValueError):
            validate_requests(self.small, self.meta, 0)
        with self.assertRaises(ValueError):
            validate_requests(self.small, self.meta, 9)
        with self.assertRaises(ValueError):
            validate_requests(
                [{"id": "large", "prompt_ids": [1] * 63}],
                self.meta, 3)

    def test_kv_projection_scales_with_request_context_and_layers(self):
        base = projected_kv_bytes(
            self.meta, [3], 3, stage_index=0)
        twice = projected_kv_bytes(
            self.meta, [3, 3], 3, stage_index=0)
        self.assertEqual(twice, 2 * base)
        self.assertEqual(
            projected_kv_bytes(self.meta, [3, 3], 3, stage_index=1),
            twice)
        # Synthetic 12-layer/large-head case rejects a 1 MiB per-rank
        # envelope *before* any expensive tensor creation.
        heavy = dict(self.meta, layers=12, head_dim=128, kv_heads=16)
        self.assertGreater(
            projected_kv_bytes(heavy, [128] * 4, 8, stage_index=0),
            1048576)
        self.assertEqual(kv_bytes([[None, None], [None]]), 0)
        example = torch.zeros((1, 2, 3, 4), dtype=torch.float32)
        self.assertEqual(kv_bytes([[(example, example)]]), 192)

    def test_requests_json_must_be_small_regular_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            valid = root / "prompts.json"
            valid.write_text(json.dumps({"requests": self.small}), encoding="utf-8")
            self.assertEqual(load_requests(valid), self.small)
            valid.write_text('{"unexpected":[]}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "expected JSON"):
                load_requests(valid)
            valid.write_text("not json", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "malformed"):
                load_requests(valid)
            valid.write_bytes(b"a" * 8200)
            with self.assertRaisesRegex(ValueError, "oversized"):
                load_requests(valid)
            target = root / "safe.json"
            target.write_text('{"requests":[]}', encoding="utf-8")
            link = root / "link.json"
            link.symlink_to(target)
            with self.assertRaisesRegex(ValueError, "unsafe"):
                load_requests(link)


@unittest.skipUnless(os.name == "posix", "Gloo integration runs on Linux CI")
class MultiRequestGloo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.state, self.config = synthetic_state(
            hidden=24, heads=6, kv_heads=2, layers=4,
            inner=48, vocab=64, positions=64, seed=4180)
        self.source = self.root / "tp1"
        self.bundle = self.root / "pp"
        export_chunked(self.state, self.config, self.source, workers=1,
                       chunk_rows=11, source="random-v048")
        partition_one_rank_v2(self.source, self.bundle)

    def tearDown(self):
        self.tmp.cleanup()

    def prompt_file(self, prompts):
        path = self.root / "requests.json"
        path.write_text(json.dumps({"requests": prompts}), encoding="utf-8")
        return path

    def compare(self, actual, prompts, precision="float32"):
        rounded = ({k: x.bfloat16().float()
                    for k, x in self.state.items()}
                   if precision == "bfloat16" else self.state)
        self.assertEqual(
            [r["id"] for r in actual["results"]],
            [p["id"] for p in prompts])
        for got, req in zip(actual["results"], prompts):
            dense = generate_dense(
                rounded, self.config, req["prompt_ids"], 3)
            check = validate_generation_parity(
                dense, got, atol=1e-5, rtol=1e-5,
                candidate_name="multi-request-pp",
                reference_name="separate-dense-reference")
            self.assertTrue(check["passed"], check["step_metrics"])

    def test_one_and_four_requests_have_independent_kv_and_correct_logits(self):
        for count in (1, 4):
            with self.subTest(count=count):
                prompts = PROMPTS[:count]
                report, _ = launch_group(
                    self.bundle, self.prompt_file(prompts),
                    mode="overlap", max_new_tokens=3, timeout=90)
                self.compare(report, prompts)
                self.assertEqual(report["generated_tokens_total"], 3 * count)
                self.assertEqual(len(report["rank_profiles"]), 2)
                self.assertEqual(report["peak_inflight_rank0"], count)
                self.assertEqual(
                    report["scheduling_opportunities_rank0"],
                    (count-1)*3)
                for rank in report["rank_profiles"]:
                    self.assertEqual(rank["send_calls"], count*3)
                    self.assertEqual(rank["recv_calls"], count*3)
                    self.assertGreater(rank["kv_cache_bytes_end"], 0)
                    self.assertLessEqual(
                        rank["kv_cache_bytes_end"], report["kv_budget_bytes_per_rank"])

    def test_three_requests_sequential_vs_async_repeat_and_json_report(self):
        dest = self.root / "performance.json"
        result = run_comparison(
            request_count=3, repeats=2, dtype="float32",
            new_tokens=3, timeout=90, output=dest)
        self.assertEqual(result["schema"], "opencompute-qwen2-multi-request-v048")
        self.assertEqual(len(result["trials"]["sequential"]), 2)
        self.assertEqual(len(result["trials"]["overlap"]), 2)
        self.assertEqual(
            json.loads(dest.read_text())["summary"], result["summary"])
        self.assertGreater(
            result["summary"]["overlap"]["median_total_tokens_per_second"], 0)
        for entry in result["trials"]["sequential"]:
            self.assertEqual(entry["peak_inflight_rank0"], 0)
            self.assertEqual(entry["scheduling_opportunities_rank0"], 0)
            self.assertTrue(entry["all_requests_parity_passed"])
        for entry in result["trials"]["overlap"]:
            self.assertEqual(entry["peak_inflight_rank0"], 3)
            self.assertEqual(entry["scheduling_opportunities_rank0"], 6)
            self.assertTrue(entry["all_requests_parity_passed"])
        with self.assertRaisesRegex(ValueError, "already exists"):
            run_comparison(request_count=3, repeats=2, output=dest)

    def test_invalid_rank0_request_stops_both_peers_not_deadlock(self):
        bad = [{"id": "broken", "prompt_ids": [999]}]
        source = self.prompt_file(bad)
        args = [sys.executable, "-m", "torch.distributed.run",
                "--standalone", "--nproc-per-node=2",
                "-m", "compute.qwen2_pipeline_batch_v048",
                "--bundle", str(self.bundle),
                "--requests-json", str(source), "--max-new-tokens", "3"]
        job = subprocess.run(
            args, capture_output=True, text=True, timeout=35,
            env={**os.environ, "OPENCOMPUTE_EXTERNAL_STORE": "0",
                 "OMP_NUM_THREADS": "1"})
        self.assertNotEqual(job.returncode, 0)
        self.assertIn("multi-request pipeline preflight failed",
                      job.stdout + job.stderr)


if __name__ == "__main__":
    unittest.main()
