"""v0.4.3 Qwen2 streaming TP format: parity, chunk integrity and rank boundaries.

All weights are synthetic and local. No DeepSeek download or remote experiment.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Mapping

import torch
from safetensors.torch import save_file

from compute.qwen2_shards import synthetic_state
from compute.qwen2_chunked import export_chunked, all_files, rank_files
from compute.qwen2_generate import read_manifest, load_rank
from compute.qwen2_reference import generate_dense
from compute.qwen2_safetensors import export_local, plan_config
from cluster_v031.checkpoints import inspect_bundle


class StreamingFormatTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def setUp(self):
        self.state, self.config = synthetic_state(
            hidden=24, heads=6, kv_heads=2, layers=2,
            inner=48, vocab=64, positions=64)
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def run_ranks(self, folder, world):
        args = [sys.executable, "-m", "torch.distributed.run",
                "--standalone", f"--nproc-per-node={world}",
                "-m", "compute.qwen2_generate", "--shards", str(folder),
                "--prompt-ids", "1,2,3", "--max-new-tokens", "3",
                "--report-logits"]
        result = subprocess.run(
            args, capture_output=True, text=True, timeout=60,
            env={**os.environ, "OMP_NUM_THREADS": "1",
                 "OPENCOMPUTE_EXTERNAL_STORE": "0"})
        self.assertEqual(result.returncode, 0, result.stderr[-2200:])
        records = [json.loads(line) for line in result.stdout.splitlines()
                   if line.startswith('{"engine": "opencompute-qwen2')]
        self.assertEqual(len(records), 1, result.stdout[-1200:])
        return records[0]

    def test_chunked_single_and_dual_rank_logit_parity(self):
        dense = generate_dense(self.state, self.config, [1,2,3], 3)
        for world in (1, 2):
            with self.subTest(world=world):
                output = self.root / f"world_{world}"
                meta = export_chunked(self.state, self.config, output, world,
                                      chunk_rows=13, source="synthetic")
                self.assertEqual(meta["format"], "opencompute-qwen2-tp-v2")
                self.assertEqual(meta["io_chunk_rows"], 13)
                self.assertEqual(set(all_files(meta)), set(
                    f.name for f in output.iterdir()))
                self.assertEqual(set(inspect_bundle(output, meta)),
                                 set(all_files(meta)))
                loaded, io = load_rank(output, 0, read_manifest(output))
                self.assertEqual(len(loaded), 2)
                self.assertEqual(io.embed(torch.tensor([1,14])).shape,(2,24))
                answer = self.run_ranks(output, world)
                self.assertEqual(answer["generated_ids"], dense["generated_ids"])
                logits = torch.tensor(answer["step_logits"])
                self.assertTrue(torch.allclose(logits, dense["step_logits"],
                                               rtol=1e-5, atol=1e-5),
                                f"max difference: {(logits-dense['step_logits']).abs().max()}")

    def test_tied_embedding_head_reuses_embed_chunks(self):
        model = dict(self.state)
        model.pop("lm_head.weight")
        config = {**self.config, "tie_word_embeddings": True}
        meta = export_chunked(model, config, self.root/"tied", 2, chunk_rows=11)
        self.assertFalse(any("head_" in name for name in all_files(meta)))
        expected = generate_dense(model, config, [1,2,3],3)
        answer = self.run_ranks(self.root/"tied",2)
        self.assertEqual(answer["generated_ids"], expected["generated_ids"])
        self.assertTrue(torch.allclose(torch.tensor(answer["step_logits"]),
                                       expected["step_logits"], rtol=1e-5, atol=1e-5))

    def test_files_cannot_cross_rank_and_hash_mutation_fails(self):
        meta = export_chunked(self.state,self.config,self.root/"bundle",
                              2,chunk_rows=13)
        first = set(rank_files(meta,0))
        second = set(rank_files(meta,1))
        self.assertEqual(first & second, {"manifest.json"})
        pinned = inspect_bundle(self.root/"bundle", meta)
        altered = self.root/"bundle"/"rank_1_layer_000.safetensors"
        with altered.open("ab") as output:
            output.write(b"changed")
        with self.assertRaisesRegex(ValueError, "integrity mismatch"):
            inspect_bundle(self.root/"bundle", meta, expected=pinned)

    def test_source_rows_loaded_without_whole_vocab_getitem(self):
        state=self.state
        class RowMapping(Mapping):
            def __iter__(self):
                return iter(state)
            def __len__(self):
                return len(state)
            def __getitem__(self,key):
                if key in ("model.embed_tokens.weight","lm_head.weight"):
                    raise AssertionError("exporter eagerly loaded whole vocabulary")
                return state[key]
            def get_rows(self,key,first,last):
                return state[key][first:last]
        output = self.root/"rowmap"
        meta = export_chunked(RowMapping(),self.config,output,2,chunk_rows=11)
        self.assertEqual(len(inspect_bundle(output,meta)),len(all_files(meta)))

    def test_real_safetensors_lazy_rows_and_explicit_chunked_export(self):
        source=self.root/"source"
        source.mkdir()
        (source/"config.json").write_text(json.dumps(self.config),encoding="utf-8")
        save_file({k:v.contiguous() for k,v in self.state.items()},
                  str(source/"model.safetensors"))
        output=self.root/"imported"
        exported=export_local(source,output,2,chunked=True)
        self.assertEqual(exported["format"],"opencompute-qwen2-tp-v2")
        self.assertEqual(read_manifest(output),exported)
        self.assertEqual(set(inspect_bundle(output,exported)),
                         set(all_files(exported)))

    def test_invalid_chunk_rows_rejected_and_deepseek_plan_only(self):
        with self.assertRaises(ValueError):
            export_chunked(self.state,self.config,self.root/"bad",
                           2,chunk_rows=0)
        config={
            "model_type":"qwen2", "hidden_size":1536,
            "num_attention_heads":12, "num_key_value_heads":2,
            "num_hidden_layers":28, "intermediate_size":8960,
            "vocab_size":151936, "max_position_embeddings":131072,
            "hidden_act":"silu", "rms_norm_eps":1e-6,
            "rope_theta":10000., "tie_word_embeddings":False,
            "use_sliding_window":False
        }
        plan=plan_config(config,2)["plan"]
        self.assertFalse(plan["exportable_under_current_limit"])
        self.assertTrue(plan["chunked_v2_exportable_by_size"])
        self.assertLessEqual(plan["chunked_v2_file_count"],512)


if __name__ == "__main__":
    unittest.main()
