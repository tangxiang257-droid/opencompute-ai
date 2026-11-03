"""Cross-implementation compatibility using offline random Hugging Face GPT-2.

No model download needed. Pretrained checkpoint integration is a separate opt-in job.
"""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest

import torch
from compute.gpt2_shards import export_state


@unittest.skipUnless(importlib.util.find_spec("transformers"), "optional transformers package not installed")
class HFCompatibilityTests(unittest.TestCase):
    def test_offline_hf_gpt2_logits_match_two_rank(self):
        from transformers import GPT2Config, GPT2LMHeadModel
        torch.set_num_threads(1)
        torch.manual_seed(2026)
        cfg = GPT2Config(n_embd=16, n_head=4, n_layer=2, n_positions=32,
                         vocab_size=32, embd_pdrop=0, attn_pdrop=0, resid_pdrop=0,
                         activation_function="gelu_new", attn_implementation="eager")
        model = GPT2LMHeadModel(cfg).eval()
        expected = []
        ids = [1, 2, 3]
        with torch.inference_mode():
            for _ in range(3):
                logits = model(input_ids=torch.tensor([ids]), use_cache=False).logits[0, -1]
                expected.append(logits)
                ids.append(int(logits.argmax().item()))
        with tempfile.TemporaryDirectory() as root:
            export_state(model.state_dict(), cfg.to_dict(), root, 2, "hf-random-offline")
            cmd = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                   "--nproc-per-node=2", "-m", "compute.gpt2_generate",
                   "--shards", root, "--prompt-ids", "1,2,3",
                   "--max-new-tokens", "3", "--report-logits"]
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=120, env={**os.environ, "OMP_NUM_THREADS": "1"})
            self.assertEqual(result.returncode, 0, result.stderr)
            reports = [json.loads(line) for line in result.stdout.splitlines()
                       if line.startswith('{"engine":')]
            self.assertEqual(len(reports), 1)
            torch.testing.assert_close(torch.tensor(reports[0]["step_logits"]),
                                       torch.stack(expected), atol=5e-4, rtol=5e-4)
            self.assertEqual(reports[0]["generated_ids"], ids[3:])
