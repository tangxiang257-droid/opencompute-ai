import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import torch
from compute.attention import prepare_attention, local_attention, load_shard, reference


class AttentionTests(unittest.TestCase):
    def test_invalid_head_partition(self):
        with tempfile.TemporaryDirectory() as path:
            with self.assertRaises(ValueError):
                prepare_attention(path, hidden=30, heads=4, workers=2)
            with self.assertRaises(ValueError):
                prepare_attention(path, hidden=32, heads=4, workers=3)

    def test_prefill_and_incremental_cache_equal_full_causal(self):
        with tempfile.TemporaryDirectory() as path:
            prepare_attention(path, hidden=16, heads=4, workers=2)
            torch.manual_seed(55)
            sequence = torch.randn(2, 6, 16)
            full, _ = reference(sequence, 16, 4, 2)
            cache = None
            segments = []
            for x in (sequence[:, :3], sequence[:, 3:4], sequence[:, 4:5], sequence[:, 5:6]):
                out, cache = reference(x, 16, 4, 2, cache)
                segments.append(out)
            torch.testing.assert_close(torch.cat(segments, dim=1), full, atol=1e-5, rtol=1e-5)
            self.assertEqual(cache[0][0].shape, (2, 2, 6, 4))
            shard = load_shard(path, 0, 16, 4, 2)
            self.assertEqual(shard["q"].shape, (16, 8))

    def test_distributed_two_workers(self):
        with tempfile.TemporaryDirectory() as path:
            prepare_attention(path, hidden=16, heads=4, workers=2)
            cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                   "--nproc-per-node=2", "-m", "compute.attention",
                   "--shards", path, "--batch", "2",
                   "--prompt-tokens", "3", "--decode-steps", "2"]
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                               env={**os.environ, "OMP_NUM_THREADS": "1"})
            self.assertEqual(p.returncode, 0, p.stderr)
            reports = [json.loads(line) for line in p.stdout.splitlines()
                       if line.startswith('{"model":')]
            self.assertEqual(len(reports), 1)
            self.assertTrue(reports[0]["correct"])
            self.assertEqual(reports[0]["local_weight_bytes"] * 2,
                             reports[0]["full_weight_bytes"])


if __name__ == "__main__":
    unittest.main()
