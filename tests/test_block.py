import json
import os
import subprocess
import sys
import unittest

class BlockTests(unittest.TestCase):
    def test_two_worker_transformer_block(self):
        cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
               "--nproc-per-node=2", "-m", "compute.block",
               "--hidden", "16", "--heads", "4", "--expansion", "32", "--tokens", "3"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                                env={**os.environ, "OMP_NUM_THREADS": "1"})
        self.assertEqual(result.returncode, 0, result.stderr)
        reports = [json.loads(line) for line in result.stdout.splitlines()
                   if line.startswith('{"model":')]
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0]["correct"])
