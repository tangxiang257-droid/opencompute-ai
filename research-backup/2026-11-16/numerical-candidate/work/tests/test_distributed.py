import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from compute.shards import prepare


class DistributedTests(unittest.TestCase):
    def test_two_workers(self):
        with tempfile.TemporaryDirectory() as tmp:
            prepare(tmp, 32, 2)
            command = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                       "--nproc-per-node=2", "-m", "compute.sharded_demo",
                       "--shards", tmp, "--batch", "3", "--iterations", "2", "--warmup", "1"]
            process = subprocess.run(command, capture_output=True, text=True, timeout=90,
                                     env={**os.environ, "OMP_NUM_THREADS": "1"})
            self.assertEqual(process.returncode, 0, process.stderr)
            records = [json.loads(line) for line in process.stdout.splitlines()
                       if line.startswith('{"workers":')]
            self.assertEqual(len(records), 1)
            self.assertTrue(records[0]["correct"])
            self.assertFalse(records[0]["full_weight_materialized"])
            self.assertEqual(records[0]["weight_elements_per_worker"], 512)


if __name__ == "__main__":
    unittest.main()
