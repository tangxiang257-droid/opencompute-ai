"""v0.4.5 independent Transformers Qwen2 tiny-model parity, NO downloads.

This is a much stronger architecture check than comparing two code paths that
share compute.qwen2_core. The model is randomly instantiated locally and is
never fetched from Hugging Face or DeepSeek. Full real-model validation is
still a separate gated experiment.
"""
import os
from pathlib import Path
import tempfile
import unittest

import torch

from benchmarks.qwen2_matrix_v045 import launch_rank_group
from compute.qwen2_chunked import export_chunked
from compute.qwen2_validation import validate_generation_parity


def local_transformers_qwen2():
    try:
        from transformers import Qwen2Config, Qwen2ForCausalLM
        return Qwen2Config, Qwen2ForCausalLM
    except Exception as exc:
        if os.environ.get("OPENCOMPUTE_REQUIRE_HF_TINY") == "1":
            raise RuntimeError("required local Transformers Qwen2 class unavailable") from exc
        raise unittest.SkipTest(
            "optional transformers dependency not installed for HF architecture parity")


@torch.inference_mode()
def hf_greedy(model, prompt, new_tokens):
    generated = []
    logits = []
    tokens = list(prompt)
    for _ in range(new_tokens):
        result = model(input_ids=torch.tensor([tokens], dtype=torch.long),
                       use_cache=False, return_dict=True)
        scores = result.logits[0, -1].to(dtype=torch.float32)
        selected = int(scores.argmax().item())
        generated.append(selected)
        logits.append(scores.tolist())
        tokens.append(selected)
    return {"generated_ids": generated, "step_logits": logits}


class IndependentQwen2TransformersParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.name != "posix":
            raise unittest.SkipTest("Gloo CPU tests require POSIX CI environment")
        cls.Config, cls.Model = local_transformers_qwen2()
        torch.set_num_threads(1)

    def test_hf_qwen2_tiny_1_and_2_rank_with_fp32_and_bf16_disk(self):
        torch.manual_seed(4180)
        cfg = self.Config(
            vocab_size=64, hidden_size=24,
            intermediate_size=48, num_hidden_layers=2,
            num_attention_heads=6, num_key_value_heads=2,
            max_position_embeddings=64, rms_norm_eps=1e-6,
            rope_theta=10000.0, hidden_act="silu",
            attention_bias=True, attention_dropout=0.0,
            tie_word_embeddings=False, use_sliding_window=False,
        )
        original = self.Model(cfg)
        original.eval()
        tensors = {name: value.detach().cpu().clone().contiguous()
                   for name, value in original.state_dict().items()}
        prompt = [1, 2, 3]
        for storage_dtype in ("float32", "bfloat16"):
            # BF16 stores rounded weights; compare to an independently executed
            # HF model whose weights have received the exact same rounding.
            reference = self.Model(cfg)
            reference.load_state_dict({
                name: (tensor.to(torch.bfloat16).float()
                       if storage_dtype == "bfloat16" else tensor)
                for name, tensor in tensors.items()}, strict=True)
            reference.eval()
            expected = hf_greedy(reference, prompt, 3)
            with tempfile.TemporaryDirectory(prefix="oc-hf-tiny-") as tmp:
                for world in (1, 2):
                    with self.subTest(precision=storage_dtype, workers=world):
                        target = Path(tmp) / f"ranks-{world}"
                        export_chunked(tensors, cfg.to_dict(), target, world,
                                       chunk_rows=11, storage_dtype=storage_dtype,
                                       source="random-in-memory-hf-qwen2-tiny")
                        observed, _ = launch_rank_group(
                            target, workers=world, max_new_tokens=3, prompt=prompt)
                        compared = validate_generation_parity(
                            expected, observed,
                            atol=3e-5, rtol=3e-5,
                            reference_name="independent-local-HF-Qwen2",
                            candidate_name=f"v2-{storage_dtype}-{world}-rank")
                        self.assertTrue(compared["passed"],
                            f"independent HF mismatch: {storage_dtype}/{world}: "
                            f"{compared['step_metrics']}")


if __name__ == "__main__":
    unittest.main()
