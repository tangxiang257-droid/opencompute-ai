"""v0.4.2 offline Qwen2 Safetensors ingress and capacity tests.

All test checkpoints are locally synthesized: no internet or download needed.
"""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import torch
from safetensors.torch import save_file

from compute.qwen2_shards import synthetic_state
from compute.qwen2_generate import load_rank, read_manifest
from compute.qwen2_safetensors import (
    export_local, inspect_directory, plan_config)
from cluster_v031.checkpoints import inspect_bundle


class TrustedSafetensorsImport(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.src = self.root / "source"
        self.src.mkdir()
        self.state, self.config = synthetic_state(
            hidden=24, heads=6, kv_heads=2, layers=2, inner=48,
            vocab=64, positions=64)
        (self.src / "config.json").write_text(
            json.dumps(self.config), encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def make_single(self, *, missing=None, extra=None):
        tensors = {name: value.clone().contiguous()
                   for name, value in self.state.items() if name != missing}
        if extra is not None:
            tensors[extra] = torch.zeros(3)
        save_file(tensors, str(self.src / "model.safetensors"))

    def make_sharded(self):
        keys = sorted(self.state)
        a = {key: self.state[key].contiguous() for key in keys[::2]}
        b = {key: self.state[key].contiguous() for key in keys[1::2]}
        filenames = ["model-00001-of-00002.safetensors",
                     "model-00002-of-00002.safetensors"]
        save_file(a, str(self.src / filenames[0]))
        save_file(b, str(self.src / filenames[1]))
        mapping = {**{key: filenames[0] for key in a},
                   **{key: filenames[1] for key in b}}
        (self.src / "model.safetensors.index.json").write_text(
            json.dumps({"metadata": {}, "weight_map": mapping}),
            encoding="utf-8")
        return mapping

    def test_single_safetensors_import_and_rank_integrity(self):
        self.make_single()
        inspected = inspect_directory(self.src, 2)
        self.assertEqual(inspected["tensor_count"], len(self.state))
        self.assertTrue(inspected["plan"]["exportable_under_current_limit"])
        self.assertEqual(inspected["input_files"], ["model.safetensors"])
        dest = self.root / "exported"
        manifest = export_local(self.src, dest, 2)
        self.assertEqual(read_manifest(dest)["format"], "opencompute-qwen2-tp-v1")
        self.assertEqual(inspect_bundle(dest, manifest)["rank_0.pt"]["name"],
                         "rank_0.pt")
        shards, io = load_rank(dest, 0, manifest)
        self.assertEqual(len(shards), 2)
        self.assertTrue(torch.equal(io["embed"], self.state["model.embed_tokens.weight"]))
        with self.assertRaisesRegex(ValueError, "already exists"):
            export_local(self.src, dest, 2)

    def test_single_file_sha256_pin_rejects_modified_weights(self):
        self.make_single()
        source = self.src / "model.safetensors"
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        accepted = inspect_directory(self.src, 2, expected_sha256=digest)
        self.assertEqual(accepted["tensor_count"], len(self.state))
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            inspect_directory(self.src, 2, expected_sha256="0" * 64)
        with self.assertRaisesRegex(ValueError, "lowercase hex"):
            inspect_directory(self.src, 2, expected_sha256="malformed")

    def test_sharded_index_import_checks_all_keys(self):
        self.make_sharded()
        entry = inspect_directory(self.src, 2)
        self.assertEqual(len(entry["input_files"]), 2)
        self.assertEqual(entry["weights"]["model.norm.weight"].shape, (24,))
        dest = self.root / "out"
        export_local(self.src, dest, 2)
        self.assertEqual(len(load_rank(dest, 1, read_manifest(dest))[0]), 2)

    def test_reject_parent_directory_traversal_in_index(self):
        mapping = self.make_sharded()
        mapping["model.norm.weight"] = "../outside.safetensors"
        (self.src / "model.safetensors.index.json").write_text(
            json.dumps({"weight_map": mapping}), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unsafe Safetensors filename"):
            inspect_directory(self.src, 2)

    def test_reject_symlinked_weight_file(self):
        self.make_single()
        original = self.src / "model.safetensors"
        other = self.root / "outside.safetensors"
        original.replace(other)
        original.symlink_to(other)
        with self.assertRaisesRegex(ValueError, "unsafe regular file"):
            inspect_directory(self.src, 2)

    def test_reject_missing_and_unrecognized_parameters(self):
        self.make_single(missing="model.norm.weight")
        with self.assertRaisesRegex(ValueError, "missing required Qwen2 tensor"):
            inspect_directory(self.src, 2)
        (self.src / "model.safetensors").unlink()
        self.make_single(extra="unsafe.extra_tensor")
        with self.assertRaisesRegex(ValueError, "unrecognized Qwen2 tensor"):
            inspect_directory(self.src, 2)

    def test_reject_shard_header_shape_mismatch(self):
        changed = dict(self.state)
        changed["model.norm.weight"] = torch.ones(3)
        save_file({k: v.contiguous() for k, v in changed.items()},
                  str(self.src / "model.safetensors"))
        with self.assertRaisesRegex(ValueError, "wrong shape"):
            inspect_directory(self.src, 2)

    def test_no_full_model_on_too_large_config(self):
        # Official DeepSeek 1.5B architecture values. This is a *plan*,
        # not a download or confirmation that full model runtime is ready.
        cfg = {
            "model_type": "qwen2", "hidden_size": 1536,
            "num_attention_heads": 12, "num_key_value_heads": 2,
            "num_hidden_layers": 28, "intermediate_size": 8960,
            "vocab_size": 151936, "max_position_embeddings": 131072,
            "rope_theta": 10000, "rms_norm_eps": 1e-6,
            "hidden_act": "silu", "tie_word_embeddings": False,
            "use_sliding_window": False,
        }
        result = plan_config(cfg, 2, source="deepseek-ai-offline-config")
        plan = result["plan"]
        self.assertFalse(plan["exportable_under_current_limit"])
        self.assertGreater(plan["rank0_io_estimated_bytes_fp32"],
                           plan["current_per_file_limit_bytes"])
        self.assertIn("chunked IO", plan["reason"])

    def test_bad_index_mapping_detected_even_when_headers_valid(self):
        mapping = self.make_sharded()
        a, b = ("model-00001-of-00002.safetensors",
                "model-00002-of-00002.safetensors")
        entry = next(k for k, v in mapping.items() if v == a)
        mapping[entry] = b
        (self.src / "model.safetensors.index.json").write_text(
            json.dumps({"weight_map": mapping}), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "index mismatch"):
            inspect_directory(self.src, 2)

    def test_export_does_not_overwrite_or_delete_existing_folder(self):
        self.make_single()
        destination = self.root / "existing"
        destination.mkdir()
        marker = destination / "keep.txt"
        marker.write_text("do not delete", encoding="utf-8")
        with self.assertRaises(ValueError):
            export_local(self.src, destination, 2)
        self.assertEqual(marker.read_text(), "do not delete")


if __name__ == "__main__":
    unittest.main()
