"""Offline independent HF Qwen2 parity for three simultaneous PP requests.

No download: randomly initialized Qwen2ForCausalLM, compared independently
per prompt to real Gloo asynchronous PP with distinct stage KV caches.
"""
import json
import os
from pathlib import Path
import tempfile
import unittest

import torch

from benchmarks.qwen2_multi_request_v048 import PROMPTS, launch_group
from compute.qwen2_chunked import export_chunked
from compute.qwen2_pipeline_v047 import partition_one_rank_v2
from compute.qwen2_validation import validate_generation_parity
def hf_models():
    try:
        from transformers import Qwen2Config, Qwen2ForCausalLM
    except Exception as exc:
        if os.environ.get("OPENCOMPUTE_REQUIRE_HF_TINY") == "1":
            raise RuntimeError("independent offline Qwen2 reference missing") from exc
        raise unittest.SkipTest("Transformers not installed in local environment")
    return Qwen2Config, Qwen2ForCausalLM


@torch.inference_mode()
def reference_greedy(model, ids, new_tokens):
    generated, logits = [], []
    current = list(ids)
    for _ in range(new_tokens):
        scores = model(input_ids=torch.tensor([current], dtype=torch.long),
                       use_cache=False, return_dict=True).logits[0, -1].float()
        token = int(scores.argmax().item())
        generated.append(token)
        logits.append(scores.tolist())
        current.append(token)
    return {"generated_ids": generated, "step_logits": logits}


@unittest.skipUnless(os.name == "posix", "Gloo CPU integration requires Linux/POSIX")
class IndependentHFMultiRequestParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.Config, cls.Model = hf_models()
        torch.set_num_threads(1)

    def test_three_independent_prompts_async_gloo_match_hf_fp32_and_bf16(self):
        torch.manual_seed(4180)
        cfg = self.Config(
            vocab_size=64, hidden_size=24,
            intermediate_size=48, num_hidden_layers=4,
            num_attention_heads=6, num_key_value_heads=2,
            max_position_embeddings=64, rms_norm_eps=1e-6,
            rope_theta=10000.0, hidden_act="silu",
            attention_bias=True, attention_dropout=0.0,
            tie_word_embeddings=False, use_sliding_window=False,
        )
        original = self.Model(cfg)
        original.eval()
        weights = {
            name: tensor.detach().cpu().clone().contiguous()
            for name, tensor in original.state_dict().items()
        }
        prompts = PROMPTS[:3]
        for precision in ("float32", "bfloat16"):
            with self.subTest(precision=precision):
                ref = self.Model(cfg)
                ref.load_state_dict({
                    name: (value.bfloat16().float()
                           if precision == "bfloat16" else value)
                    for name, value in weights.items()}, strict=True)
                ref.eval()
                references = {
                    request["id"]: reference_greedy(
                        ref, request["prompt_ids"], new_tokens=2)
                    for request in prompts
                }
                with tempfile.TemporaryDirectory(prefix="oc-v048-hf-") as temp:
                    root = Path(temp)
                    source = root / "one-rank"
                    exported = root / "pp-stages"
                    export_chunked(
                        weights, cfg.to_dict(), source,
                        workers=1, chunk_rows=11,
                        storage_dtype=precision,
                        source="local-random-hf-qwen2-no-download")
                    partition_one_rank_v2(source, exported)
                    request_file = root / "requests.json"
                    request_file.write_text(
                        json.dumps({"requests": prompts}), encoding="utf-8")
                    report, _ = launch_group(
                        exported, request_file, mode="overlap",
                        max_new_tokens=2, timeout=90)
                    self.assertEqual(
                        [item["id"] for item in report["results"]],
                        [item["id"] for item in prompts])
                    self.assertEqual(report["peak_inflight_rank0"], 3)
                    for item in report["results"]:
                        checked = validate_generation_parity(
                            references[item["id"]], item,
                            atol=3e-5, rtol=3e-5,
                            reference_name="independent-HF-Qwen2",
                            candidate_name=f"async-PP-{item['id']}")
                        self.assertTrue(
                            checked["passed"], str(checked["step_metrics"]))


if __name__ == "__main__":
    unittest.main()
