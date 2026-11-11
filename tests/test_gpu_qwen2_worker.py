import hashlib
import json
import tempfile
import unittest
from pathlib import Path

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
