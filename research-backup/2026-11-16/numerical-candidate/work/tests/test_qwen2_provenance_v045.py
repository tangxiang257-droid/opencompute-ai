"""v0.4.5 no-download Qwen2/DeepSeek local provenance and chat-template gates."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import torch
from safetensors.torch import save_file

from benchmarks import qwen2_offline_validation as offline
from compute.qwen2_chunked import export_chunked
from compute.qwen2_shards import synthetic_state


class ProvenanceAndTokenizerGates(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "model"
        self.source.mkdir()
        self.state, self.config = synthetic_state(
            hidden=24, heads=6, kv_heads=2, layers=2,
            inner=48, vocab=64, positions=64)
        (self.source / "config.json").write_text(
            json.dumps(self.config), encoding="utf-8")
        save_file({k: v.contiguous() for k, v in self.state.items()},
                  str(self.source / "model.safetensors"))
        self.exported = self.root / "exported"
        export_chunked(self.state, self.config, self.exported, 2,
                       chunk_rows=11, storage_dtype="bfloat16")

    def tearDown(self):
        self.temp.cleanup()

    def test_reject_rope_and_norm_config_mismatch_before_expensive_reference(self):
        for name, replacement in (("rope_theta", 7777.0),
                                  ("rms_norm_eps", 1e-4),
                                  ("tie_word_embeddings", True)):
            with self.subTest(name=name):
                original = json.loads(
                    (self.source / "config.json").read_text(encoding="utf-8"))
                original[name] = replacement
                (self.source / "config.json").write_text(
                    json.dumps(original), encoding="utf-8")
                with patch.object(offline, "hf_reference") as hf:
                    # Tied-embedding mismatch is detected even earlier: the
                    # source Safetensors header now contains an unexpected
                    # separate lm_head.weight tensor for that config.
                    expected_error = ("unrecognized Qwen2 tensor"
                                      if name == "tie_word_embeddings"
                                      else "source and shards mismatch")
                    with self.assertRaisesRegex(ValueError, expected_error):
                        offline.run(self.source, self.exported, "你好",
                                    workers=2, compare_run=True)
                    hf.assert_not_called()
                (self.source / "config.json").write_text(
                    json.dumps(self.config), encoding="utf-8")

    def test_input_and_manifest_hashes_without_running_reference(self):
        with patch.object(offline, "local_tokenizer_ids",
                          return_value=[1, 2, 3]), \
             patch.object(offline, "hf_reference") as expensive, \
             patch.object(offline, "local_tp") as gloo:
            report = offline.run(
                self.source, self.exported, "你好", workers=2,
                fingerprint_weights=True, chat=True)
        weights = self.source / "model.safetensors"
        expected = hashlib.sha256(weights.read_bytes()).hexdigest()
        self.assertEqual(
            report["source_weight_fingerprints"]["model.safetensors"]["sha256"],
            expected)
        self.assertTrue(report["chat_template_applied"])
        self.assertEqual(len(report["config_fingerprint"]["sha256"]), 64)
        self.assertEqual(len(report["export_manifest_fingerprint"]["sha256"]), 64)
        self.assertFalse(report["real_reference_executed"])
        expensive.assert_not_called()
        gloo.assert_not_called()

    def test_tampered_input_hash_is_distinct_from_exported_manifest_hash(self):
        config = self.source / "config.json"
        recorded = offline.sha256_regular_file(config, max_bytes=32768)
        config.write_text(json.dumps({**self.config, "rope_theta": 1234.}),
                          encoding="utf-8")
        observed = offline.sha256_regular_file(config, max_bytes=32768)
        self.assertNotEqual(recorded["sha256"], observed["sha256"])
        with self.assertRaisesRegex(ValueError, "size exceeds"):
            offline.sha256_regular_file(config, max_bytes=1)

    def test_local_chat_prompt_uses_only_trusted_tokenizer_files(self):
        tokenizer = Mock()
        tokenizer.chat_template = "trusted-chat-template"
        tokenizer.apply_chat_template.return_value = [3, 1, 2]
        factory = Mock()
        factory.from_pretrained.return_value = tokenizer
        with patch.dict(sys.modules, {"transformers": SimpleNamespace(
                AutoTokenizer=factory)}):
            result = offline.local_tokenizer_ids(self.source, "你好", chat=True)
        self.assertEqual(result, [3, 1, 2])
        factory.from_pretrained.assert_called_once_with(
            str(self.source.resolve()), local_files_only=True,
            trust_remote_code=False, use_fast=True)
        tokenizer.apply_chat_template.assert_called_once_with(
            [{"role": "user", "content": "你好"}],
            add_generation_prompt=True, tokenize=True)
        tokenizer.encode.assert_not_called()

    def test_chat_template_absence_fails_closed(self):
        tokenizer = Mock()
        tokenizer.chat_template = None
        factory = Mock()
        factory.from_pretrained.return_value = tokenizer
        with patch.dict(sys.modules, {"transformers": SimpleNamespace(
                AutoTokenizer=factory)}):
            with self.assertRaisesRegex(ValueError, "no chat template"):
                offline.local_tokenizer_ids(self.source, "你好", chat=True)


if __name__ == "__main__":
    unittest.main()
