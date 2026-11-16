"""v0.4.6: capacity screening, repeated sample stats and actual Gloo A/B contract.

The workflow additionally runs a tiny real end-to-end torchrun benchmark and
stores the JSON artifact; most unit tests here do not spawn subprocesses.
"""
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmarks.qwen2_repeat_v046 import (
    repeated_benchmark, sample_summary, modes_for)
from compute.parallel_screen_v046 import screening
from compute.qwen2_validation import summarize_profile


class RobustStatisticsTests(unittest.TestCase):
    def test_nearest_rank_median_and_nonnegative_communication(self):
        result = sample_summary([4.0, 1.0, 3.0, 2.0, 5.0])
        self.assertEqual(result["median"], 3.)
        self.assertEqual(result["p10_nearest_rank"], 1.)
        self.assertEqual(result["p90_nearest_rank"], 5.)
        self.assertEqual(result["individual"], [4., 1., 3., 2., 5.])
        zero = sample_summary([0, 0], nonnegative=True)
        self.assertEqual(zero["median"], 0.)
        with self.assertRaises(ValueError):
            sample_summary([0, 1])
        for bad in ([1], [1, float("nan")], [1, float("inf")], [-1, 2],
                    [1] * 31):
            with self.assertRaises(ValueError):
                sample_summary(bad)

    def test_modes_distinguish_single_rank_from_genuine_tp(self):
        self.assertEqual(modes_for(1), ("local_fastpath", "legacy_collective"))
        self.assertEqual(modes_for(2), ("tensor_parallel",))
        with self.assertRaises(ValueError):
            modes_for(3)


class ParallelScreeningTests(unittest.TestCase):
    def args(self):
        return dict(hidden=1536, heads=12, kv_heads=2, layers=28,
                    workers=2, transformer_weight_bytes=2_000_000_000,
                    io_weight_bytes=930_000_000,
                    memory_per_node_bytes=2_200_000_000,
                    latency_ms=10, bandwidth_mbps=100,
                    prompt_tokens=32, new_tokens=16)

    def test_gqa_and_weight_only_feasibility_separated(self):
        output = screening(**self.args())
        self.assertEqual(output["tensor_parallel"]["estimated_collectives_per_rank"],
                         (2 * 28 + 1) * 16)
        self.assertTrue(output["tensor_parallel"]["topology_supported"])
        self.assertTrue(output["pipeline_parallel"]["topology_supported"])
        self.assertFalse(output["pipeline_parallel"]["implemented"])
        self.assertGreater(output["tensor_parallel"]["ring_proxy_communication_seconds"],
                           output["pipeline_parallel"]["unicast_proxy_communication_seconds"])
        self.assertEqual(output["conclusion"],
                         "screening_only_no_winner_without_measured_compute_time")
        self.assertIn("not network measurements", output["disclaimer"])

    def test_pipeline_is_only_a_theoretical_candidate(self):
        args = self.args()
        args.update(workers=4, memory_per_node_bytes=2_000_000_000)
        score = screening(**args)
        self.assertFalse(score["tensor_parallel"]["topology_supported"])
        self.assertTrue(score["pipeline_parallel"]["topology_supported"])
        self.assertTrue(score["pipeline_parallel"]["passes_weight_only_memory_screen"])
        self.assertFalse(score["pipeline_parallel"]["implemented"])
        self.assertIsNone(score["tensor_parallel"]["ring_proxy_bytes_per_rank"])

    def test_one_rank_and_memory_envelope(self):
        args = self.args()
        args["workers"] = 1
        one = screening(**args)
        self.assertEqual(one["tensor_parallel"]["ring_proxy_bytes_per_rank"], 0)
        self.assertEqual(one["pipeline_parallel"]["stage_boundary_transfers"], 0)
        self.assertFalse(one["tensor_parallel"]["passes_weight_only_memory_screen"])
        self.assertEqual(one["pipeline_parallel"]["weights_only_most_loaded_bytes"],
                         2_930_000_000)

    def test_reject_invalid_inputs(self):
        args = self.args()
        for key, value in (("latency_ms", -1), ("bandwidth_mbps", 0),
                           ("workers", 0), ("hidden", 1535),
                           ("memory_per_node_bytes", -2),
                           ("bandwidth_mbps", float("nan"))):
            with self.subTest(key=key):
                wrong = {**args, key: value}
                with self.assertRaises(ValueError):
                    screening(**wrong)


class RepeatedBenchmarkContracts(unittest.TestCase):
    def test_invalid_experiment_rejected_before_launch(self):
        with patch("benchmarks.qwen2_repeat_v046.launch_rank_group") as execute:
            with self.assertRaises(ValueError):
                repeated_benchmark(repeats=1)
            with self.assertRaises(ValueError):
                repeated_benchmark(model_size="unbounded")
            with self.assertRaises(ValueError):
                repeated_benchmark(storage_dtype="float16")
            with self.assertRaises(ValueError):
                repeated_benchmark(warmups=3)
        execute.assert_not_called()

    def test_no_overwrite_existing_report_before_experiment(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "existing.json"
            path.write_text("keep", encoding="utf-8")
            with patch("benchmarks.qwen2_repeat_v046.launch_rank_group") as execute:
                with self.assertRaisesRegex(ValueError, "overwrite"):
                    repeated_benchmark(output=path)
            self.assertEqual(path.read_text(), "keep")
            execute.assert_not_called()

    def test_single_rank_profile_skips_collectives_only_when_fastpath_enabled(self):
        base = {
            "workers": 1, "generated_ids": [1, 2],
            "profile": {
                "local_single_rank_collectives_skipped": True,
                "ranks": [{
                    "decode_us": 1000, "preflight_us": 100,
                    "collective_wall_us": 0, "all_reduce_calls": 0,
                    "broadcast_calls": 0, "all_reduce_tensor_bytes": 0,
                    "broadcast_tensor_bytes": 0, "rss_sample_bytes": 0,
                    "peak_rss_bytes_linux": 0,
                }],
            },
        }
        summary = summarize_profile(base, layers=2, prompt_length=3)
        self.assertTrue(summary["local_single_rank_collectives_skipped"])
        self.assertEqual(summary["ranks"][0]["collective_fraction"], 0)
        # A malformed 'fastpath' report cannot suppress two-rank validation.
        wrong = {**base, "workers": 2}
        with self.assertRaises(ValueError):
            summarize_profile(wrong, layers=2, prompt_length=3)


if __name__ == "__main__":
    unittest.main()
