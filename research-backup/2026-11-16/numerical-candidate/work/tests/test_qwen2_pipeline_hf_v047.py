"""v0.4.7 independent local Hugging Face Qwen2 versus real two-stage Gloo PP.

Small randomly initialized weights, entirely offline. If CI explicitly
requires HF, import errors must fail rather than silently skip validation.
"""
import os
from pathlib import Path
import tempfile
import unittest

import torch

from benchmarks.qwen2_pp_compare_v047 import launch_pp
from compute.qwen2_chunked import export_chunked
from compute.qwen2_pipeline_v047 import partition_one_rank_v2
from compute.qwen2_validation import validate_generation_parity


def hf_models():
    try:
        from transformers import Qwen2Config, Qwen2ForCausalLM
    except Exception as exc:
        if os.getenv("OPENCOMPUTE_REQUIRE_HF_TINY") == "1":
            raise RuntimeError("independent Hugging Face test dependency unavailable") from exc
        raise unittest.SkipTest("optional local Transformers dependency absent")
    return Qwen2Config, Qwen2ForCausalLM


@torch.inference_mode()
def reference_greedy(model, ids, new_tokens):
    tokens = list(ids)
    generated, logits = [], []
    for _ in range(new_tokens):
        scores = model(input_ids=torch.tensor([tokens]),
                       use_cache=False, return_dict=True).logits[0, -1].float()
        choice = int(scores.argmax().item())
        generated.append(choice)
        logits.append(scores.tolist())
        tokens.append(choice)
    return {"generated_ids": generated, "step_logits": logits}


@unittest.skipUnless(os.name == "posix", "localhost torchrun Gloo requires POSIX")
class IndependentQwen2PipelineParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.Config, cls.Model = hf_models()
        torch.set_num_threads(1)

    def test_two_stage_pp_matches_offline_hf_float32_and_bfloat16(self):
        torch.manual_seed(4180)
        cfg = self.Config(
            vocab_size=64, hidden_size=24,
            intermediate_size=48, num_hidden_layers=4,
            num_attention_heads=6, num_key_value_heads=2,
            max_position_embeddings=64, rms_norm_eps=1e-6,
            rope_theta=10000.0, hidden_act="silu",
            attention_bias=True, attention_dropout=0.0,
            tie_word_embeddings=False, use_sliding_window=False)
        model = self.Model(cfg)
        model.eval()
        state = {key: value.detach().cpu().clone().contiguous()
                 for key, value in model.state_dict().items()}
        prompt = [1, 2, 3]
        for dtype in ("float32", "bfloat16"):
            with self.subTest(dtype=dtype):
                ref = self.Model(cfg)
                ref.load_state_dict({
                    name: (value.bfloat16().float() if dtype == "bfloat16"
                           else value)
                    for name, value in state.items()}, strict=True)
                ref.eval()
                expected = reference_greedy(ref, prompt, 3)
                with tempfile.TemporaryDirectory(prefix="oc-pp-hf-tiny-") as temp:
                    folder = Path(temp)
                    source = folder / "trusted-source"
                    export_chunked(state, cfg.to_dict(), source, workers=1,
                                   storage_dtype=dtype, chunk_rows=11,
                                   source="random-hf-qwen2-tiny-no-download")
                    bundle = folder / "stages"
                    partition_one_rank_v2(source, bundle)
                    actual, _ = launch_pp(bundle, prompt, 3, timeout=90)
                    comparison = validate_generation_parity(
                        expected, actual, atol=3e-5, rtol=3e-5,
                        reference_name="independent-HF-Qwen2",
                        candidate_name=f"PP-two-stages-{dtype}")
                    self.assertTrue(comparison["passed"],
                                    str(comparison["step_metrics"]))


if __name__ == "__main__":
    unittest.main()
