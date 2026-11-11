import hashlib
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from compute.qwen2_shards import synthetic_state


class Qwen2WorkerTests(unittest.TestCase):
    def test_pinned_local_model_generates_text_with_the_matching_tokenizer(self):
        from safetensors.torch import save_file
        from tokenizers import Tokenizer
        from tokenizers.models import WordLevel
        from tokenizers.pre_tokenizers import Whitespace

        from cluster_v07.qwen2_job import configure, generate_text

        weights, config = synthetic_state()
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as folder:
            root = Path(folder)
            (root / "config.json").write_text(json.dumps(config), encoding="utf-8")
            save_file(weights, str(root / "model.safetensors"))
            digest = hashlib.sha256((root / "model.safetensors").read_bytes()).hexdigest()
            tokenizer = Tokenizer(WordLevel({f"t{i}": i for i in range(config["vocab_size"])}, unk_token="t0"))
            tokenizer.pre_tokenizer = Whitespace()
            tokenizer.save(str(root / "tokenizer.json"))

            model = configure(root, digest)
            payload = {"model_id": model["id"], "prompt": "t1 t7 t2", "max_new_tokens": 3}
            actual = generate_text(model, payload, device="cpu", required_vram_mb=model["vram_mb"])
            self.assertEqual(actual["generated_ids"], [52, 9, 21])
            self.assertEqual(actual["text"], "t52 t9 t21")
            self.assertEqual(actual["device"], "cpu")

            tokenizer.add_tokens(["additional-token"])
            tokenizer.save(str(root / "tokenizer.json"))
            with self.assertRaisesRegex(ValueError, "model assets changed"):
                generate_text(model, payload, device="cpu", required_vram_mb=model["vram_mb"])

    def test_subprocess_accepts_large_prompt_while_worker_polls_its_lease(self):
        from cluster_v07 import qwen2_job

        model = {"id": "a" * 64}
        job = {"payload": {"model_id": model["id"], "prompt": "🧪" * 4096, "max_new_tokens": 1},
               "lease_seconds": 7, "gpu_index": 0, "required_vram_mb": 512}
        pulses = []
        popen = subprocess.Popen
        code = ("import json,sys,time; time.sleep(0.3); request=json.load(sys.stdin); "
                "print(json.dumps({'result':{'length':len(request['payload']['prompt'])}}))")

        def fixture_child(_command, **kwargs):
            return popen([sys.executable, "-c", code], **kwargs)

        with (patch.object(qwen2_job, "PULSE_INTERVAL", 0.05),
              patch.object(qwen2_job.subprocess, "Popen", side_effect=fixture_child)):
            result = qwen2_job.execute(model, job, claim_started=time.monotonic(),
                                       pulse=lambda: pulses.append(True))
        self.assertEqual(result, {"length": 4096})
        self.assertTrue(pulses)
