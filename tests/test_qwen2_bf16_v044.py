"""v0.4.4: BF16 weights, bounded caches, and offline HF comparison helpers.

All model tensors are synthetic. No large model or tokenizer is downloaded.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import torch
from safetensors import safe_open
from safetensors.torch import save_file

from compute.qwen2_shards import synthetic_state
from compute.qwen2_chunked import (
    ChunkedIO, LayerStream, TensorLRU, all_files, export_chunked)
from compute.qwen2_generate import load_rank, read_manifest
from compute.qwen2_reference import generate_dense
from compute.qwen2_safetensors import export_local, plan_config
from benchmarks.qwen2_offline_reference import compare_logits, local_token_ids
from cluster_v031.checkpoints import inspect_bundle, validate_manifest


class Bfloat16StorageAndCache(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state, self.config = synthetic_state(
            hidden=24, heads=6, kv_heads=2, layers=2,
            inner=48, vocab=64, positions=64)

    def tearDown(self):
        self.temp.cleanup()

    def run_gloo(self, folder, workers):
        argv = [sys.executable, "-m", "torch.distributed.run",
                "--standalone", f"--nproc-per-node={workers}",
                "-m", "compute.qwen2_generate", "--shards", str(folder),
                "--prompt-ids", "1,2,3", "--max-new-tokens", "3",
                "--report-logits", "--layer-cache-mib", "1",
                "--io-cache-mib", "1"]
        process = subprocess.run(
            argv, capture_output=True, text=True, timeout=70,
            env={**os.environ, "OMP_NUM_THREADS": "1",
                 "OPENCOMPUTE_EXTERNAL_STORE": "0"})
        self.assertEqual(process.returncode, 0, process.stderr[-1800:])
        reports = [json.loads(line) for line in process.stdout.splitlines()
                   if line.startswith('{"engine": "opencompute-qwen2')]
        self.assertEqual(len(reports), 1, process.stdout[-1500:])
        return reports[0]

    def test_bf16_disk_uses_less_bytes_and_fp32_math_matches_rounded_dense(self):
        cast_state = {name: tensor.to(torch.bfloat16).to(torch.float32)
                      for name, tensor in self.state.items()}
        reference = generate_dense(cast_state, self.config, [1, 2, 3], 3)
        float32 = export_chunked(self.state, self.config,
                                 self.root / "float32", 2, chunk_rows=11)
        bf16 = export_chunked(self.state, self.config,
                              self.root / "bf16", 2, chunk_rows=11,
                              storage_dtype="bfloat16")
        self.assertEqual(bf16["weight_dtype"], "bfloat16")
        sizes = {}
        for label, meta in (("f32", float32), ("bf16", bf16)):
            folder = self.root / ("float32" if label == "f32" else "bf16")
            sizes[label] = sum((folder / name).stat().st_size
                               for name in all_files(meta) if name != "manifest.json")
            self.assertEqual(set(inspect_bundle(folder, meta)), set(all_files(meta)))
        self.assertLess(sizes["bf16"], sizes["f32"] * .7)
        file = self.root / "bf16" / "rank_0_layer_000.safetensors"
        with safe_open(str(file), framework="pt", device="cpu") as data:
            self.assertEqual(data.get_slice("q_w").get_dtype(), "BF16")
        for world in (1, 2):
            path = self.root / f"ranks_{world}"
            export_chunked(self.state, self.config, path, world,
                           chunk_rows=11, storage_dtype="bfloat16")
            actual = self.run_gloo(path, world)
            self.assertEqual(actual["generated_ids"], reference["generated_ids"])
            self.assertTrue(torch.allclose(
                torch.tensor(actual["step_logits"]), reference["step_logits"],
                atol=1e-5, rtol=1e-5))

    def test_byte_bounded_cache_hits_and_zero_budget(self):
        path = self.root / "cache"
        meta = export_chunked(self.state, self.config, path, 2,
                              chunk_rows=11, storage_dtype="bfloat16")
        layers = LayerStream(path, 0, meta, cache_mib=1)
        list(layers)
        list(layers)
        self.assertEqual(layers.cache.hits, meta["layers"])
        self.assertLessEqual(layers.cache.size, layers.cache.limit)
        io = ChunkedIO(path, meta, cache_mib=1)
        one = io._chunk("head", 0, 0, 11)
        two = io._chunk("head", 0, 0, 11)
        self.assertEqual(io.cache.hits, 1)
        self.assertTrue(torch.equal(one, two))
        self.assertEqual(one.dtype, torch.float32)
        empty = TensorLRU(0)
        empty.get("value", lambda: torch.ones(1000))
        empty.get("value", lambda: torch.ones(1000))
        self.assertEqual(empty.hits, 0)
        self.assertEqual(empty.size, 0)
        with self.assertRaises(ValueError):
            TensorLRU(cache_mib=1024)

    def test_bad_storage_dtype_and_mislabelled_header_fail(self):
        path = self.root / "wrong"
        meta = export_chunked(self.state, self.config, path, 2,
                              chunk_rows=11, storage_dtype="bfloat16")
        wrong = dict(meta, weight_dtype="int8")
        with self.assertRaisesRegex(ValueError, "storage precision"):
            validate_manifest(wrong)
        altered = path / "rank0_norm.safetensors"
        save_file({"weight": torch.ones(meta["hidden"], dtype=torch.float32)},
                  str(altered))
        with self.assertRaisesRegex(ValueError, "header tensor"):
            load_rank(path, 0, meta)

    def test_trusted_local_bf16_safetensors_export(self):
        source = self.root / "trusted"
        source.mkdir()
        (source / "config.json").write_text(
            json.dumps(self.config), encoding="utf-8")
        save_file({key: val.contiguous() for key, val in self.state.items()},
                  str(source / "model.safetensors"))
        exported = export_local(source, self.root / "output", 2,
                                chunked=True, storage_dtype="bfloat16")
        self.assertEqual(exported["weight_dtype"], "bfloat16")
        self.assertEqual(read_manifest(self.root / "output"), exported)
        self.assertEqual(set(inspect_bundle(self.root / "output", exported)),
                         set(all_files(exported)))
        with self.assertRaisesRegex(ValueError, "requires chunked"):
            export_local(source, self.root / "bad", 2,
                         storage_dtype="bfloat16")

    def test_capacity_planner_exposes_bf16_layout(self):
        fp32 = plan_config(self.config, 2)["plan"]
        bf16 = plan_config(self.config, 2, storage_dtype="bfloat16")["plan"]
        self.assertEqual(bf16["v2_storage_dtype"], "bfloat16")
        self.assertLess(bf16["chunked_v2_largest_layer_estimated_bytes"],
                        fp32["chunked_v2_largest_layer_estimated_bytes"])

    def test_offline_comparison_errors_do_not_claim_success(self):
        tp = {"step_logits": [[1., 3.], [4., 2.]], "generated_ids": [1, 0]}
        reference = {"step_logits": torch.tensor([[1., 3.], [4., 2.]]),
                     "generated_ids": [1, 0]}
        self.assertTrue(compare_logits(tp, reference)["pass"])
        reference["generated_ids"] = [0, 0]
        self.assertFalse(compare_logits(tp, reference)["pass"])
        reference["step_logits"] = torch.ones((2, 3))
        with self.assertRaisesRegex(ValueError, "shape mismatch"):
            compare_logits(tp, reference)

    def test_tokenizer_is_strictly_offline_without_custom_code(self):
        token = Mock()
        token.encode.return_value = [1, 2, 3]
        token.chat_template = "mock-chat-template"
        token.apply_chat_template.return_value = [4, 5, 6]
        auto = Mock()
        auto.from_pretrained.return_value = token
        with patch.dict(sys.modules, {"transformers": SimpleNamespace(AutoTokenizer=auto)}):
            returned, ids = local_token_ids(self.root, "Hello", chat=True)
        self.assertIs(returned, token)
        self.assertEqual(ids, [4, 5, 6])
        self.assertEqual(auto.from_pretrained.call_args.kwargs["local_files_only"], True)
        self.assertEqual(auto.from_pretrained.call_args.kwargs["trust_remote_code"], False)


if __name__ == "__main__":
    unittest.main()
