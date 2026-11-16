"""No-download validation harness tests for optional real Qwen2 reference.

The tests use tiny synthetic Safetensors and mock Transformers interfaces.
No Hugging Face model is downloaded and no full pretrained model is loaded.
"""
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import Mock, patch

import torch
from safetensors.torch import save_file

from benchmarks import qwen2_offline_validation as offline
from compute.qwen2_shards import synthetic_state
from compute.qwen2_chunked import export_chunked


class OfflineValidationUnitTests(TestCase):
    def test_compare_logits_checks_values_and_token_ids(self):
        ref = {"generated_ids": [1, 2],
               "step_logits": [[0.2, 1.0, -1.0], [1.1, -1.0, 2.0]]}
        same = {"generated_ids": [1, 2],
                "step_logits": [[0.2001, 1.0, -1.0], [1.1, -1.0, 2.0]]}
        result = offline.compare_logits(ref, same, atol=0.001, rtol=0.001)
        self.assertTrue(result["passed"])
        self.assertEqual(result["steps"], 2)
        self.assertLess(result["max_abs_logit_delta"], 0.001)
        same["generated_ids"] = [2, 2]
        self.assertFalse(offline.compare_logits(ref, same)["passed"])
        same["generated_ids"] = [1, 2]
        same["step_logits"][0][0] = 5.0
        self.assertFalse(offline.compare_logits(ref, same)["logits_within_tolerance"])

    def test_compare_rejects_truncated_and_nonfinite_logits(self):
        ref = {"generated_ids": [1], "step_logits": [[0.1, 0.2]]}
        with self.assertRaisesRegex(ValueError, "vocabulary"):
            offline.compare_logits(ref, {"generated_ids": [1],
                                         "step_logits": [[0.2]]})
        with self.assertRaisesRegex(ValueError, "non-finite"):
            offline.compare_logits(ref, {"generated_ids": [1],
                                         "step_logits": [[float("nan"), 0.2]]})
        with self.assertRaises(ValueError):
            offline.compare_logits(ref, {"generated_ids": [1]})
        with self.assertRaises(ValueError):
            offline.compare_logits(ref, ref, rtol=float("inf"))

    def test_tokenizer_requires_local_only_and_rejects_custom_code(self):
        with tempfile.TemporaryDirectory() as root:
            (Path(root)/"config.json").write_text("{}", encoding="utf-8")
            tokenizer = Mock()
            tokenizer.encode.return_value = [1, 2, 3]
            factory = Mock()
            factory.from_pretrained.return_value = tokenizer
            with patch.dict(sys.modules, {"transformers":
                                         SimpleNamespace(AutoTokenizer=factory)}):
                ids = offline.local_tokenizer_ids(root, "测试")
            self.assertEqual(ids, [1, 2, 3])
            factory.from_pretrained.assert_called_once_with(
                str(Path(root).resolve()), local_files_only=True,
                trust_remote_code=False, use_fast=True)
            tokenizer.encode.assert_called_once_with(
                "测试", add_special_tokens=False)

    def test_reference_requires_local_only_safetensors_and_fp32(self):
        with tempfile.TemporaryDirectory() as root:
            (Path(root)/"config.json").write_text("{}", encoding="utf-8")
            model = Mock()
            model.config.model_type = "qwen2"
            model.side_effect = [
                SimpleNamespace(logits=torch.tensor([[[0., 0.5, 1.5]]]),
                                past_key_values="state1"),
                SimpleNamespace(logits=torch.tensor([[[2., 0.5, 1.5]]]),
                                past_key_values="state2"),
            ]
            factory = Mock()
            factory.from_pretrained.return_value = model
            with patch.dict(sys.modules, {"transformers":
                                         SimpleNamespace(AutoModelForCausalLM=factory)}):
                report = offline.hf_reference(root, [1, 2], 2)
            self.assertEqual(report["generated_ids"], [2, 0])
            factory.from_pretrained.assert_called_once_with(
                str(Path(root).resolve()), local_files_only=True,
                trust_remote_code=False, use_safetensors=True,
                torch_dtype=torch.float32)
            self.assertEqual(model.call_count, 2)
            self.assertEqual(model.call_args_list[1].kwargs["past_key_values"],
                             "state1")

    def test_local_tp_has_fixed_python_module_no_shell(self):
        with tempfile.TemporaryDirectory() as root:
            fake = SimpleNamespace(
                returncode=0, stdout='{"engine":"opencompute-qwen2-tp-v0.4.1","generated_ids":[2],"step_logits":[[0,1,2]]}\n',
                stderr="")
            with patch("benchmarks.qwen2_offline_validation.subprocess.run",
                       return_value=fake) as launched:
                result = offline.local_tp(root, [1, 2], 1, 2)
            self.assertEqual(result["generated_ids"], [2])
            argv = launched.call_args.args[0]
            self.assertEqual(argv[:3],
                             [sys.executable, "-m", "torch.distributed.run"])
            self.assertIn("compute.qwen2_generate", argv)
            self.assertEqual(launched.call_args.kwargs["env"][
                "OPENCOMPUTE_EXTERNAL_STORE"], "0")
            self.assertEqual(launched.call_args.kwargs["timeout"], 240)

    def test_preflight_with_synthetic_local_weight_files_does_not_load_hf(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            model_dir = root / "model"
            model_dir.mkdir()
            state, config = synthetic_state(
                hidden=24, heads=6, kv_heads=2, layers=2,
                inner=48, vocab=64, positions=64)
            (model_dir / "config.json").write_text(
                json.dumps(config), encoding="utf-8")
            save_file({k: v.contiguous() for k, v in state.items()},
                      str(model_dir / "model.safetensors"))
            shard_dir = root / "shards"
            export_chunked(state, config, shard_dir, 2, chunk_rows=11,
                           storage_dtype="float32")
            with patch.object(offline, "local_tokenizer_ids",
                              return_value=[1, 2, 3]), \
                 patch.object(offline, "hf_reference") as heavy, \
                 patch.object(offline, "local_tp") as network:
                result = offline.run(model_dir, shard_dir, "你好", workers=2)
            self.assertEqual(result["mode"], "offline-preflight")
            self.assertFalse(result["real_reference_executed"])
            self.assertEqual(result["prompt_ids"], [1, 2, 3])
            heavy.assert_not_called()
            network.assert_not_called()

    def test_mismatched_source_and_shards_rejected_before_hf_loading(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            model = root / "model"
            model.mkdir()
            state, config = synthetic_state(
                hidden=24, heads=6, kv_heads=2, layers=2,
                inner=48, vocab=64, positions=64)
            (model / "config.json").write_text(
                json.dumps({**config, "vocab_size": 128}), encoding="utf-8")
            save_file({k: v.contiguous() for k, v in state.items()},
                      str(model / "model.safetensors"))
            shards = root / "shards"
            export_chunked(state, config, shards, 2, chunk_rows=11)
            with patch.object(offline, "hf_reference") as heavy:
                with self.assertRaises(ValueError):
                    offline.run(model, shards, "hello", workers=2)
            heavy.assert_not_called()


if __name__ == "__main__":
    main()
