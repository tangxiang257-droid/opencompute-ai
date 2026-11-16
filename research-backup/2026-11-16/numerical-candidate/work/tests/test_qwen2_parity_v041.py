"""Offline Qwen2/GQA CPU distributed inference numerical checks.

Runs tiny synthetic Qwen2-shaped weights on 2 localhost Gloo ranks. Confirms
the gathered autoregressive logits match the original dense HF-style tensors.
This neither downloads DeepSeek weights nor proves 1.5B deployment readiness.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import torch

from compute.qwen2_shards import export_state, manifest_from_config, synthetic_state
from compute.qwen2_generate import read_manifest, load_rank
from compute.qwen2_reference import generate_dense


class Qwen2Parity(unittest.TestCase):
    def test_invalid_rope_and_incompatible_gqa_fails_closed(self):
        weights, config = synthetic_state()
        for patch in ({"model_type": "llama"},
                      {"hidden_act": "gelu"},
                      {"rope_scaling": {"type": "yarn"}},
                      {"use_sliding_window": True}):
            with self.assertRaises(ValueError):
                manifest_from_config({**config, **patch}, 2)
        # Existing exporter currently requires kv_heads divisible by world.
        with self.assertRaises(ValueError):
            manifest_from_config(config, 4)

    def _compare(self, *, tied=False, prompt=None):
        state, config = synthetic_state()
        if tied:
            config["tie_word_embeddings"] = True
            del state["lm_head.weight"]
        prompt = prompt or [1, 3, 5]
        expected = generate_dense(state, config, prompt, max_new_tokens=3)
        with tempfile.TemporaryDirectory() as folder:
            meta = export_state(state, config, folder, workers=2,
                                source="synthetic-offline-test")
            self.assertEqual(read_manifest(folder)["format"], meta["format"])
            for rank in (0, 1):
                shards, io = load_rank(folder, rank, meta)
                self.assertEqual(len(shards), meta["layers"])
                self.assertEqual(io is None, rank != 0)
            cmd = [
                sys.executable, "-m", "torch.distributed.run",
                "--standalone", "--nproc-per-node=2",
                "-m", "compute.qwen2_generate", "--shards", folder,
                "--prompt-ids", ",".join(map(str, prompt)),
                "--max-new-tokens", "3", "--report-logits",
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  timeout=60,
                                  env={**os.environ, "OMP_NUM_THREADS": "1"})
            self.assertEqual(proc.returncode, 0,
                             (proc.stderr + proc.stdout)[-3500:])
            records = [json.loads(line) for line in proc.stdout.splitlines()
                       if line.startswith('{"engine":')]
            self.assertEqual(len(records), 1, proc.stdout)
            result = records[0]
            self.assertEqual(result["generated_ids"], expected["generated_ids"])
            self.assertEqual(len(result["weight_bytes_per_rank"]), 2)
            self.assertTrue(all(x > 0 for x in result["weight_bytes_per_rank"]))
            actual = torch.tensor(result["step_logits"])
            target = expected["step_logits"]
            self.assertTrue(torch.allclose(actual, target, rtol=2e-4, atol=1e-5),
                            f"largest logit delta: {(actual-target).abs().max()}")

    def test_two_rank_untied_lm_head_matches_dense(self):
        self._compare(tied=False)

    def test_two_rank_tied_lm_head_matches_dense(self):
        self._compare(tied=True)

    def test_bad_rank_checkpoint_rejected_without_peer_hang(self):
        state, config = synthetic_state()
        with tempfile.TemporaryDirectory() as folder:
            export_state(state, config, folder, workers=2)
            (Path(folder) / "rank_1.pt").write_bytes(b"damaged-shard")
            cmd = [sys.executable, "-m", "torch.distributed.run",
                   "--standalone", "--nproc-per-node=2", "-m",
                   "compute.qwen2_generate", "--shards", folder,
                   "--prompt-ids", "1,2", "--max-new-tokens", "2"]
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=50,
                                    env={**os.environ, "OMP_NUM_THREADS": "1"})
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Qwen2 rank preflight failed",
                          result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
