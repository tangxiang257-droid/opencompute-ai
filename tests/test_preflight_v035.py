"""v0.3.5: malformed prompt/one bad rank must fail together, not hang at Gloo.

Uses the existing trusted same-host synthetic model and two torchrun ranks.
No internet, real pretrained model, or physical second device is required.
"""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from compute.gpt2_shards import export_state, synthetic_state


class GlooPreflightTests(unittest.TestCase):
    def _launch(self, folder, prompt):
        command = [
            sys.executable, "-m", "torch.distributed.run", "--standalone",
            "--nproc-per-node=2", "-m", "compute.gpt2_generate",
            "--shards", str(folder), "--prompt-ids", prompt,
            "--max-new-tokens", "2",
        ]
        started = time.monotonic()
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=50,
            env={**os.environ, "OMP_NUM_THREADS": "1"})
        elapsed = time.monotonic() - started
        self.assertNotEqual(result.returncode, 0,
                            "invalid distributed input unexpectedly succeeded")
        self.assertIn("distributed preflight failed",
                      result.stderr + result.stdout)
        # Normal torchrun startup contributes overhead. Failure must not wait
        # for the 120s process-group collective timeout.
        self.assertLess(elapsed, 45)

    def _fixture(self, folder):
        state, config = synthetic_state(
            hidden=16, heads=4, layers=2, vocab=32, positions=32)
        export_state(state, config, folder, 2, "offline-preflight-test")

    def test_rank0_invalid_vocab_id_exits_both_ranks(self):
        with tempfile.TemporaryDirectory() as root:
            self._fixture(root)
            self._launch(root, "9999")

    def test_rank0_prompt_parse_error_exits_both_ranks(self):
        with tempfile.TemporaryDirectory() as root:
            self._fixture(root)
            self._launch(root, "1,not-an-integer")

    def test_rank1_corrupt_shard_exits_both_ranks(self):
        with tempfile.TemporaryDirectory() as root:
            self._fixture(root)
            (Path(root) / "rank_1.pt").write_bytes(b"corrupt-torch-checkpoint")
            self._launch(root, "1,2,3")


if __name__ == "__main__":
    unittest.main()
