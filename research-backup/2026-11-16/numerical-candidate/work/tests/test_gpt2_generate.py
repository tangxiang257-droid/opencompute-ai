"""Offline synthetic GPT-2 integration tests. No network/download required."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import torch
from compute.gpt2_reference import greedy
from compute.gpt2_shards import export_state, synthetic_state
from compute.gpt2_generate import load_rank, read_manifest


class GPT2ExportTests(unittest.TestCase):
    def test_export_partitions_and_rank_zero_io(self):
        state, cfg = synthetic_state(hidden=16, heads=4, layers=2, vocab=32)
        with tempfile.TemporaryDirectory() as tmp:
            manifest = export_state(state, cfg, tmp, 2, "synthetic")
            self.assertEqual(manifest["workers"], 2)
            self.assertEqual(manifest["layers"], 2)
            rank0, io = load_rank(Path(tmp), 0, manifest)
            rank1, missing = load_rank(Path(tmp), 1, manifest)
            self.assertIsNone(missing)
            self.assertEqual(io["wte"].shape, (32, 16))
            self.assertEqual(rank0[0]["q_w"].shape, (16, 8))
            self.assertEqual(rank1[0]["mlp_fc_w"].shape, (16, 32))
            torch.testing.assert_close(torch.cat([rank0[0]["q_w"], rank1[0]["q_w"]], 1),
                                       state["transformer.h.0.attn.c_attn.weight"][:, :16])

    def test_reject_invalid_world(self):
        state, cfg = synthetic_state(hidden=16, heads=4)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                export_state(state, cfg, tmp, 3)

    def test_distributed_decode_matches_dense_gpt2(self):
        torch.set_num_threads(1)
        state, cfg = synthetic_state(hidden=16, heads=4, layers=2, vocab=32)
        expected_ids, expected_logits = greedy([1, 2, 3, 4], state, cfg, 4)
        with tempfile.TemporaryDirectory() as tmp:
            export_state(state, cfg, tmp, 2, "synthetic")
            command = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                       "--nproc-per-node=2", "-m", "compute.gpt2_generate",
                       "--shards", tmp, "--prompt-ids", "1,2,3,4",
                       "--max-new-tokens", "4", "--report-logits"]
            completed = subprocess.run(command, capture_output=True, text=True,
                                       timeout=120, env={**os.environ, "OMP_NUM_THREADS": "1"})
            self.assertEqual(completed.returncode, 0, completed.stderr)
            reports = [json.loads(line) for line in completed.stdout.splitlines()
                       if line.startswith('{"engine":')]
            self.assertEqual(len(reports), 1, completed.stdout)
            report = reports[0]
            self.assertEqual(report["generated_ids"], expected_ids)
            actual_logits = torch.tensor(report["step_logits"])
            torch.testing.assert_close(actual_logits, expected_logits, atol=1e-5, rtol=1e-5)
            self.assertEqual(report["processed_tokens_per_layer"], 7)
            self.assertEqual(len(report["kv_cache_bytes_per_rank"]), 2)
            self.assertEqual(report["kv_cache_bytes_per_rank"][0], report["kv_cache_bytes_per_rank"][1])
            self.assertLess(report["weight_bytes_per_rank"][1], report["weight_bytes_per_rank"][0])

    def test_single_rank_decode(self):
        state, cfg = synthetic_state(hidden=8, heads=2, layers=1, vocab=16)
        with tempfile.TemporaryDirectory() as tmp:
            export_state(state, cfg, tmp, 1, "synthetic")
            command = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                       "--nproc-per-node=1", "-m", "compute.gpt2_generate",
                       "--shards", tmp, "--prompt-ids", "1,2", "--max-new-tokens", "2"]
            result = subprocess.run(command, capture_output=True, text=True,
                                    timeout=90, env={**os.environ, "OMP_NUM_THREADS": "1"})
            self.assertEqual(result.returncode, 0, result.stderr)
            report = next(json.loads(line) for line in result.stdout.splitlines()
                          if line.startswith('{"engine":'))
            self.assertEqual(len(report["generated_ids"]), 2)


if __name__ == "__main__":
    unittest.main()