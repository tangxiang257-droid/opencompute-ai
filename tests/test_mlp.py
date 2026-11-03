import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
import torch
from compute.mlp import prepare_mlp

class MLPTests(unittest.TestCase):
    def test_shard_shapes(self):
        with tempfile.TemporaryDirectory() as root:
            prepare_mlp(root, 16, 32, 4)
            for rank in range(4):
                shard = torch.load(Path(root) / f"rank_{rank}.pt", weights_only=True)
                self.assertEqual(tuple(shard["w1"].shape), (16, 8))
                self.assertEqual(tuple(shard["w2"].shape), (8, 16))

    def test_two_rank_forward(self):
        with tempfile.TemporaryDirectory() as root:
            prepare_mlp(root, 16, 32, 2)
            cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                   "--nproc-per-node=2", "-m", "compute.mlp", "--shards", root,
                   "--batch", "2", "--iterations", "2", "--warmup", "0"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=90,
                                    env={**os.environ, "OMP_NUM_THREADS": "1"})
            self.assertEqual(result.returncode, 0, result.stderr)
            reports = [json.loads(line) for line in result.stdout.splitlines()
                       if line.startswith('{"model":')]
            self.assertEqual(len(reports), 1)
            self.assertTrue(reports[0]["correct"])
            self.assertEqual(reports[0]["local_weight_bytes"] * 2, reports[0]["full_weight_bytes"])
