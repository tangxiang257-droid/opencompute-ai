"""v0.4.5 strict Qwen2 parity/profiling tests; no pretrained download."""
import json
import math
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import torch

from compute.qwen2_trace import (
    CollectiveProfiler, PROFILE_FIELDS, interpret_rank_stats,
    profile_rank_stats)
from compute.qwen2_validation import (
    summarize_profile, validate_generation_parity)
from benchmarks.qwen2_matrix_v045 import parse_choices, run_matrix


class StepwiseParityTests(unittest.TestCase):
    def setUp(self):
        self.reference = {
            "generated_ids": [1, 0],
            "step_logits": [[0., 2., -1.], [3., -1., 2.]],
        }

    def test_exact_logits_and_token_ids(self):
        report = validate_generation_parity(
            self.reference, dict(self.reference),
            reference_name="test", candidate_name="rank0")
        self.assertTrue(report["passed"])
        self.assertEqual(report["steps"], 2)
        self.assertEqual(report["vocab_size"], 3)
        self.assertEqual(len(report["step_metrics"]), 2)

    def test_fail_closed_on_different_argmax_even_if_aggregate_is_close(self):
        # Bare logit MAE can hide a tiny-margin top-1 swap.
        reference = {"generated_ids": [0],
                     "step_logits": [[1.0001, 1., -3.]]}
        candidate = {"generated_ids": [1],
                     "step_logits": [[1., 1.0001, -3.]]}
        report = validate_generation_parity(reference, candidate,
                                            atol=0.01, rtol=0.01)
        self.assertFalse(report["passed"])
        self.assertTrue(report["all_step_logits_within_tolerance"])
        self.assertFalse(report["tokens_identical"])
        self.assertTrue(report["step_metrics"][0]["near_tie_relative_to_error"])

    def test_one_wrong_step_causes_failure(self):
        changed = {"generated_ids": [1, 0],
                   "step_logits": [[0., 2., -1.], [3., -1., 2.2]]}
        result = validate_generation_parity(self.reference, changed,
                                            atol=0.01, rtol=0.01)
        self.assertFalse(result["passed"])
        self.assertFalse(result["all_step_logits_within_tolerance"])
        self.assertTrue(result["step_metrics"][0]["logits_within_tolerance"])
        self.assertFalse(result["step_metrics"][1]["logits_within_tolerance"])

    def test_reject_inconsistent_tokens_nonfinite_and_ragged_logits(self):
        bad = {"generated_ids": [0, 0],
               "step_logits": self.reference["step_logits"]}
        with self.assertRaisesRegex(ValueError, "reported tokens disagree"):
            validate_generation_parity(self.reference, bad)
        bad = {"generated_ids": [1, 0],
               "step_logits": [[0., float("inf"), -1.], [3., -1., 2.]]}
        with self.assertRaisesRegex(ValueError, "non-finite"):
            validate_generation_parity(self.reference, bad)
        with self.assertRaises(ValueError):
            validate_generation_parity(
                self.reference, {"generated_ids": [1],
                                 "step_logits": [[1.], [2., 3.]]})
        with self.assertRaises(ValueError):
            validate_generation_parity(self.reference, self.reference, atol=float("nan"))


class CommunicationProfileTests(unittest.TestCase):
    def test_profile_wrapper_counts_tensor_payload_not_network_traffic(self):
        trace = CollectiveProfiler()
        x = torch.zeros((2, 3), dtype=torch.float32)
        with patch("compute.qwen2_trace.dist.broadcast") as bcast, \
             patch("compute.qwen2_trace.dist.all_reduce") as reduce:
            trace.broadcast(x, src=0)
            trace.all_reduce(x)
        bcast.assert_called_once()
        reduce.assert_called_once()
        self.assertEqual(trace.calls_broadcast, 1)
        self.assertEqual(trace.calls_all_reduce, 1)
        self.assertEqual(trace.payload_broadcast_bytes, 24)
        self.assertEqual(trace.payload_all_reduce_bytes, 24)
        self.assertGreaterEqual(trace.collective_ns, 0)

    def test_fixed_rank_stats_and_expected_collectives(self):
        profile = CollectiveProfiler()
        profile.calls_all_reduce = 12
        profile.calls_broadcast = 3
        profile.payload_all_reduce_bytes = 2000
        profile.payload_broadcast_bytes = 800
        profile.collective_ns = 400_000
        fields = profile_rank_stats(
            preflight_us=100, decode_us=2000, prefill_us=400,
            tokens=3, profiler=profile, rss_sample_bytes=512,
            peak_rss_bytes_linux=768)
        self.assertEqual(len(fields), len(PROFILE_FIELDS))
        recorded = interpret_rank_stats(fields)
        self.assertEqual(recorded["peak_rss_bytes_linux"], 768)
        result = {
            "workers": 2, "generated_ids": [1, 2, 3],
            "profile": {"ranks": [recorded, dict(recorded)]},
        }
        summary = summarize_profile(result, layers=2, prompt_length=3)
        self.assertEqual(summary["workers"], 2)
        self.assertEqual(summary["ranks"][0]["all_reduce_calls"], 12)
        self.assertEqual(summary["ranks"][0]["broadcast_calls"], 3)
        self.assertEqual(summary["ranks"][0]["collective_fraction"], 0.2)
        self.assertIn("NOT wire", summary["scope"].replace("not", "NOT"))
        wrong = {**recorded, "broadcast_calls": 4}
        with self.assertRaisesRegex(ValueError, "collective count"):
            summarize_profile({**result,
                               "profile": {"ranks": [wrong, recorded]}},
                              layers=2, prompt_length=3)

    def test_bad_counter_rejected(self):
        profile = CollectiveProfiler()
        with self.assertRaises(ValueError):
            profile_rank_stats(preflight_us=0, decode_us=-1,
                               prefill_us=0, tokens=1, profiler=profile)
        with self.assertRaises(ValueError):
            interpret_rank_stats([0, 1])


class SyntheticGlooMatrixTests(unittest.TestCase):
    def test_invalid_cli_inputs_fail_before_running_subprocesses(self):
        for choice in ("1,1", "1,9", "2,3", ""):
            with self.assertRaises(ValueError):
                parse_choices(choice, allowed=("1", "2"))
        with patch("benchmarks.qwen2_matrix_v045.launch_rank_group") as start:
            with self.assertRaises(ValueError):
                run_matrix(worker_counts=(1, 4), precisions=("float32",))
        start.assert_not_called()

    @unittest.skipUnless(os.name == "posix", "CPU Gloo CI runs on POSIX")
    def test_real_cpu_gloo_float32_bfloat16_1_and_2_ranks(self):
        # Runs only tiny, deterministic random tensors, not pretrained weights.
        result = run_matrix(worker_counts=(1, 2),
                            precisions=("float32", "bfloat16"),
                            max_new_tokens=2, timeout=80)
        self.assertEqual(len(result["cases"]), 4)
        for case in result["cases"]:
            self.assertTrue(case["exact_rounded_dense_parity"]["passed"])
            self.assertEqual(len(case["profile"]["ranks"]), case["workers"])
            self.assertEqual(case["profile"]["steps"], 2)
            self.assertGreater(case["stored_weight_bytes"], 0)
            self.assertGreater(case["rank0_decode_seconds"], 0)
            self.assertIn("speedup_vs_same_dtype_one_rank", case)
            for rank in case["profile"]["ranks"]:
                expected_calls = 0 if case["workers"] == 1 else 8
                self.assertEqual(rank["all_reduce_calls"], expected_calls)
                self.assertEqual(rank["broadcast_calls"], 0 if case["workers"] == 1 else 2)
                if case["workers"] > 1:
                    self.assertGreater(rank["all_reduce_tensor_bytes"], 0)
                else:
                    self.assertEqual(rank["all_reduce_tensor_bytes"], 0)
        single = next(c for c in result["cases"]
                      if c["workers"] == 1 and c["stored_dtype"] == "float32")
        tiny = next(c for c in result["cases"]
                    if c["workers"] == 1 and c["stored_dtype"] == "bfloat16")
        self.assertLess(tiny["stored_weight_bytes"],
                        single["stored_weight_bytes"])
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "matrix.json"
            report = run_matrix(worker_counts=(1,), precisions=("float32",),
                                max_new_tokens=1, timeout=80, persist_to=output)
            self.assertEqual(json.loads(output.read_text())["schema"],
                             result["schema"])
            self.assertTrue(report["cases"][0]["exact_rounded_dense_parity"]["passed"])


if __name__ == "__main__":
    unittest.main()
