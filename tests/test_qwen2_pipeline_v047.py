"""v0.4.7 genuine Qwen2 two-stage pipeline tests, offline synthetic weights."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import torch

from benchmarks.qwen2_pp_compare_v047 import benchmark, launch_pp
from compute.qwen2_chunked import export_chunked
from compute.qwen2_pipeline_v047 import (
    FORMAT, inspect_pipeline, partition_one_rank_v2, expected_stage_paths)
from compute.qwen2_reference import generate_dense
from compute.qwen2_shards import synthetic_state
from compute.qwen2_validation import validate_generation_parity


class PipelineBundleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.state, self.config = synthetic_state(
            hidden=24, heads=6, kv_heads=2, layers=4, inner=48,
            vocab=64, positions=64, seed=4180)
        self.source = self.root / "tp-one-rank"
        export_chunked(self.state, self.config, self.source, workers=1,
                       chunk_rows=11, source="synthetic-no-download")

    def tearDown(self):
        self.temp.cleanup()

    def make_bundle(self):
        out = self.root / "pp"
        meta = partition_one_rank_v2(self.source, out)
        return out, meta

    def test_disjoint_full_layer_stages_and_rank0_only_vocab(self):
        folder, manifest = self.make_bundle()
        self.assertEqual(manifest["format"], FORMAT)
        self.assertEqual(manifest["bounds"], [[0, 2], [2, 4]])
        self.assertEqual(manifest["world_size"], 2)
        allpaths = set(manifest["files"])
        self.assertIn("stage_0/rank0_embed_000.safetensors", allpaths)
        self.assertIn("stage_0/rank0_norm.safetensors", allpaths)
        self.assertIn("stage_1/rank_0_layer_001.safetensors", allpaths)
        self.assertFalse(any("rank0_head" in p and p.startswith("stage_1/")
                             for p in allpaths))
        self.assertTrue(all(p.startswith("stage_0/") or p.startswith("stage_1/")
                            for p in allpaths))
        self.assertEqual(set(expected_stage_paths(
            manifest["source_model"], manifest["bounds"])), allpaths)
        self.assertEqual(inspect_pipeline(folder, rank=0), manifest)
        self.assertEqual(inspect_pipeline(folder, rank=1), manifest)
        self.assertEqual(
            sum("_layer_" in p for p in allpaths), self.config["num_hidden_layers"])
        # Original TP1 remains intact.
        self.assertTrue((self.source / "rank_0_layer_003.safetensors").is_file())

    def test_tamper_and_missing_file_rejected_before_gloo_send(self):
        folder, manifest = self.make_bundle()
        target = folder / "stage_1" / "rank_0_layer_000.safetensors"
        with target.open("ab") as dst:
            dst.write(b"tamper")
        with self.assertRaisesRegex(ValueError, "integrity mismatch"):
            inspect_pipeline(folder, rank=1)
        self.assertEqual(inspect_pipeline(folder, rank=0), manifest)
        target.unlink()
        with self.assertRaisesRegex(ValueError, "missing/symlinked"):
            inspect_pipeline(folder, rank=1)

    def test_symlink_and_inventory_injection_fails_closed(self):
        folder, manifest = self.make_bundle()
        target = folder / "stage_1" / "rank_0_layer_000.safetensors"
        original = target.read_bytes()
        target.unlink()
        target.symlink_to(self.source / "rank_0_layer_002.safetensors")
        with self.assertRaisesRegex(ValueError, "missing/symlinked"):
            inspect_pipeline(folder, rank=1)
        target.unlink()
        target.write_bytes(original)
        manifest["files"]["../../unknown.safetensors"] = {
            "size": 1, "sha256": "0" * 64}
        (folder / "pipeline_manifest.json").write_text(json.dumps(manifest),
                                                       encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "inventory mismatch"):
            inspect_pipeline(folder, rank=1)

    def test_reject_non_single_rank_source_and_existing_output(self):
        folder, _ = self.make_bundle()
        with self.assertRaises(FileExistsError):
            partition_one_rank_v2(self.source, folder)
        with self.assertRaisesRegex(ValueError, "separate"):
            partition_one_rank_v2(self.source, self.source / "nested")
        two = self.root / "tp2"
        export_chunked(self.state, self.config, two, workers=2, chunk_rows=11)
        with self.assertRaises(ValueError):
            partition_one_rank_v2(two, self.root / "rejected")


@unittest.skipUnless(os.name == "posix", "Gloo CPU tests require POSIX")
class PipelineGlooTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def test_two_stage_send_recv_fp32_and_bf16_matches_unsliced_dense(self):
        for precision in ("float32", "bfloat16"):
            with self.subTest(storage=precision):
                report = benchmark(dtype=precision, new_tokens=3, timeout=90)
                self.assertEqual(report["schema"],
                                 "opencompute-qwen2-pp-v047-comparison-v1")
                self.assertEqual([c["mode"] for c in report["cases"]],
                                 ["tp1", "tp2", "pp2"])
                self.assertTrue(all(c["parity_passed"] for c in report["cases"]))
                pp = report["cases"][-1]
                self.assertEqual(pp["stages"], [[0, 2], [2, 4]])
                self.assertEqual(len(pp["rank_profiles"]), 2)
                for rank in pp["rank_profiles"]:
                    self.assertGreater(rank["sent_tensor_bytes"], 0)
                    self.assertGreater(rank["received_tensor_bytes"], 0)
                    self.assertGreater(rank["decode_us"], 0)
                self.assertGreater(pp["tokens_per_second_rank0"], 0)

    def test_bad_rank1_safetensors_fail_fast_without_hanging_peer(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            state, cfg = synthetic_state(
                hidden=24, heads=6, kv_heads=2, layers=2,
                inner=48, vocab=64, positions=64)
            source = root / "tp"
            export_chunked(state, cfg, source, workers=1, chunk_rows=11)
            pp = root / "pp"
            partition_one_rank_v2(source, pp)
            bad = pp / "stage_1" / "rank_0_layer_000.safetensors"
            bad.write_bytes(b"corrupt-source")
            args = [
                sys.executable, "-m", "torch.distributed.run", "--standalone",
                "--nproc-per-node=2", "-m", "compute.qwen2_pipeline_v047",
                "--bundle", str(pp), "--prompt-ids", "1,2,3",
            ]
            run = subprocess.run(
                args, capture_output=True, text=True, timeout=40,
                env={**os.environ, "OMP_NUM_THREADS": "1",
                     "OPENCOMPUTE_EXTERNAL_STORE": "0"})
            self.assertNotEqual(run.returncode, 0)
            self.assertIn("pipeline rank preflight failed",
                          run.stderr + run.stdout)


if __name__ == "__main__":
    unittest.main()
