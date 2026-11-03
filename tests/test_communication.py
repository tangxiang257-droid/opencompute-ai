import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import torch
from compute.communication import CommunicationMeter
from compute.gpt2_shards import export_state, synthetic_state


class CommunicationTests(unittest.TestCase):
    def test_penalty_is_explicit_not_real_bandwidth(self):
        meter = CommunicationMeter(2, latency_ms=2, bandwidth_mbps=8)
        tensor = torch.zeros(256, dtype=torch.float32)
        with patch("compute.communication.dist.all_reduce"), patch("compute.communication.time.sleep") as sleeping:
            meter.all_reduce(tensor)
        self.assertEqual(meter.calls["all_reduce"], 1)
        self.assertEqual(meter.payload_bytes["all_reduce"], 1024)
        self.assertEqual(meter.wire_bytes_estimate, 1024)
        self.assertAlmostEqual(sleeping.call_args.args[0], 0.003024, places=6)
        self.assertIn("Synthetic", meter.report()["model_warning"])

    def test_single_rank_no_added_penalty(self):
        meter = CommunicationMeter(1, latency_ms=20, bandwidth_mbps=1)
        with patch("compute.communication.dist.broadcast"), patch("compute.communication.time.sleep") as sleeping:
            meter.broadcast(torch.empty(4))
        sleeping.assert_not_called()

    def test_distributed_network_stats_present(self):
        state, cfg = synthetic_state(hidden=16, heads=4, layers=1, vocab=32)
        with tempfile.TemporaryDirectory() as tmp:
            export_state(state, cfg, tmp, 2)
            cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                   "--nproc-per-node=2", "-m", "compute.gpt2_generate",
                   "--shards", tmp, "--prompt-ids", "1,2",
                   "--max-new-tokens", "2", "--net-latency-ms", "1",
                   "--net-bandwidth-mbps", "5"]
            completed = subprocess.run(cmd, capture_output=True, text=True,
                                       timeout=90, env={**os.environ, "OMP_NUM_THREADS": "1"})
            self.assertEqual(completed.returncode, 0, completed.stderr)
            report = next(json.loads(line) for line in completed.stdout.splitlines()
                          if line.startswith('{"engine":'))
            profile = report["network_profile"]
            self.assertEqual(profile["calls"], {"broadcast": 4, "all_reduce": 4})
            self.assertGreater(profile["injected_seconds"], 0)
            self.assertGreater(profile["gloo_seconds"], 0)
