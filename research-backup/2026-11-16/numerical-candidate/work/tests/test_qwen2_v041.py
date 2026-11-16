"""Qwen2 TP v0.4.1: GQA/RoPE/SwiGLU parity and managed-session contract.

Only synthetic Qwen2 tensors; no pretrained downloads or external machines.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import torch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from cluster_v03.store import ClusterStore
from cluster_v031.coordinator import SessionCoordinator
from cluster_v04.api import create_model_router
from compute.qwen2_shards import synthetic_state, export_state, manifest_from_config
from compute.qwen2_generate import load_rank, read_manifest
from compute.qwen2_reference import generate_dense


ADMIN = "qwen2-local-admin-key-0123456789"


class Qwen2TensorParity(unittest.TestCase):
    def setUp(self):
        torch.set_num_threads(1)
        self.state, self.config = synthetic_state(
            hidden=24, heads=6, kv_heads=2, layers=2, inner=48,
            vocab=64, positions=64)

    def run_distributed(self, directory, world, *, prompt="1,2,3", expected_ok=True):
        argv = [sys.executable, "-m", "torch.distributed.run",
                "--standalone", f"--nproc-per-node={world}",
                "-m", "compute.qwen2_generate",
                "--shards", str(directory), "--prompt-ids", prompt,
                "--max-new-tokens", "3", "--report-logits"]
        proc = subprocess.run(
            argv, capture_output=True, text=True, timeout=60,
            env={**os.environ, "OMP_NUM_THREADS": "1",
                 "OPENCOMPUTE_EXTERNAL_STORE": "0"})
        if not expected_ok:
            self.assertNotEqual(proc.returncode, 0,
                                "bad checkpoint unexpectedly executed")
            self.assertIn("Qwen2 rank preflight failed", proc.stderr + proc.stdout)
            return
        self.assertEqual(proc.returncode, 0, proc.stderr[-2400:])
        reports = [json.loads(line) for line in proc.stdout.splitlines()
                   if line.startswith('{"engine": "opencompute-qwen2')]
        self.assertEqual(len(reports), 1, proc.stdout[-800:])
        return reports[0]

    def test_exported_1_and_2_rank_decode_matches_dense_logits(self):
        prompt = [1, 2, 3]
        baseline = generate_dense(self.state, self.config, prompt, 3)
        for world in (1, 2):
            with self.subTest(world=world), tempfile.TemporaryDirectory() as folder:
                meta = export_state(self.state, self.config, folder, world,
                                    "synthetic-test")
                self.assertEqual(meta["kv_heads"], 2)
                self.assertEqual(read_manifest(folder)["workers"], world)
                shard0, io = load_rank(folder, 0, meta)
                self.assertEqual(len(shard0), 2)
                self.assertEqual(io["lm_head"].shape, (64, 24))
                distributed = self.run_distributed(folder, world)
                self.assertEqual(distributed["generated_ids"], baseline["generated_ids"])
                scores = torch.tensor(distributed["step_logits"])
                self.assertTrue(torch.allclose(
                    scores, baseline["step_logits"], rtol=1e-5, atol=1e-5),
                    f"world={world}, max logit delta="
                    f"{(scores-baseline['step_logits']).abs().max().item()}")

    def test_rank1_corrupt_shard_aborts_all_live_ranks(self):
        with tempfile.TemporaryDirectory() as folder:
            export_state(self.state, self.config, folder, 2, "synthetic-test")
            (Path(folder) / "rank_1.pt").write_bytes(b"invalid-checkpoint")
            self.run_distributed(folder, 2, expected_ok=False)

    def test_reject_wrong_kv_rank_topology_and_sliding_window(self):
        bad = {**self.config, "use_sliding_window": True}
        with self.assertRaisesRegex(ValueError, "sliding-window"):
            manifest_from_config(bad, 2)
        with self.assertRaisesRegex(ValueError, "unsupported Qwen2 dimensions"):
            manifest_from_config(self.config, 3)
        with self.assertRaisesRegex(ValueError, "only the stock Qwen2"):
            manifest_from_config({**self.config, "model_type": "llama"}, 2)

    def test_tied_embedding_without_separate_lm_head(self):
        state = dict(self.state)
        state.pop("lm_head.weight")
        cfg = {**self.config, "tie_word_embeddings": True}
        with tempfile.TemporaryDirectory() as folder:
            meta = export_state(state, cfg, folder, 2, "tied-test")
            _, io = load_rank(folder, 0, meta)
            self.assertTrue(torch.equal(io["lm_head"], io["embed"]))
            self.assertEqual(
                self.run_distributed(folder, 2)["generated_ids"],
                generate_dense(state, cfg, [1, 2, 3], 3)["generated_ids"])


class Qwen2ManagedPool(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = ClusterStore(self.root / "db.sqlite")
        self.coordinator = SessionCoordinator(self.store, self.root / "sessions")
        self.mock_server = patch.object(
            self.coordinator, "_open_rendezvous",
            return_value=SimpleNamespace(port=29556))
        self.mock_server.start()
        app = FastAPI()
        app.include_router(create_model_router(self.coordinator, admin_key=ADMIN))
        self.client = TestClient(app)
        self.auth = {"X-Admin-Key": ADMIN}

    def tearDown(self):
        self.client.close()
        self.mock_server.stop()
        self.temp.cleanup()

    def register(self, name, caps):
        node = self.store.register(
            name=name, platform="linux", cpu_cores=4, ram_mb=4096,
            compute_score=100, slots=1, capabilities=caps)
        self.store.heartbeat(node["id"], node["node_token"],
                             free_ram_mb=2048, cpu_load_pct=0)
        return node

    def test_only_qwen2_capable_nodes_get_qwen2_assignments(self):
        self.register("gpt2-only", ["tp_gpt2"])
        with self.assertRaisesRegex(Exception, "not enough"):
            self.coordinator.create_qwen2_synthetic(
                workers=2, hidden=24, heads=6, layers=1,
                prompt_ids=[1,2], new_tokens=2)
        first = self.register("qwen-a", ["tp_qwen2"])
        second = self.register("qwen-b", ["tp_qwen2"])
        response = self.client.post("/v0.4/sessions", headers=self.auth,
                                    json={"model_id": "qwen2-synthetic",
                                          "workers": 2, "hidden": 24,
                                          "heads": 6, "layers": 2,
                                          "prompt_ids": [1, 2, 3],
                                          "max_new_tokens": 3})
        self.assertEqual(response.status_code, 200, response.text)
        session = response.json()["session"]
        self.assertEqual(session["engine"], "qwen2-tp")
        sid = session["session_id"]
        self.assertEqual(self.coordinator.status(sid)["engine"], "qwen2-tp")
        rank_nodes = [first, second]
        assignments = [self.coordinator.poll(
            n["id"], n["node_token"])["assignment"] for n in rank_nodes]
        self.assertEqual({a["rank"] for a in assignments}, {0,1})
        self.assertTrue(all(a["engine"]=="qwen2-tp" for a in assignments))
        self.assertEqual({len(a["files"]) for a in assignments}, {2,3})
        for n in rank_nodes:
            self.coordinator.ready(n["id"], n["node_token"], sid)
        self.assertEqual(self.coordinator.status(sid)["status"], "running")
        self.coordinator.cancel(sid)
        for n in rank_nodes:
            self.coordinator.stopped(n["id"], n["node_token"], sid)
        self.assertEqual(self.coordinator.status(sid)["status"], "failed")

    def test_qwen2_configuration_rejects_wrong_tp_count(self):
        self.register("qwen-a", ["tp_qwen2"])
        self.register("qwen-b", ["tp_qwen2"])
        result = self.client.post("/v0.4/sessions", headers=self.auth,
                                  json={"model_id": "qwen2-synthetic",
                                        "workers": 4, "hidden": 24, "heads": 6})
        self.assertEqual(result.status_code, 422, result.text)


if __name__ == "__main__":
    unittest.main()
