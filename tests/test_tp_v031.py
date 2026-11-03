"""Offline managed tensor-parallel session tests: auth, placement, files, state."""
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from cluster_v03.api import create_router as old_router
from cluster_v03.store import ClusterStore
from cluster_v031.api import create_router
from cluster_v031.coordinator import SessionCoordinator
from compute.gpt2_shards import export_state, synthetic_state

ADMIN = "integration-admin-key-0123456789"
JOIN = "integration-join-key-0123456789"


class Sessions(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = ClusterStore(self.root / "cluster.sqlite")
        self.coordinator = SessionCoordinator(
            self.store, self.root / "sessions",
            checkpoint_root=self.root / "checkpoints")
        app = FastAPI()
        app.include_router(old_router(self.store, admin_key=ADMIN, join_key=JOIN))
        app.include_router(create_router(self.coordinator, admin_key=ADMIN))
        self.api = TestClient(app)

    def tearDown(self):
        self.api.close()
        self.tmp.cleanup()

    def node(self, name, score=100, mem=2048):
        r = self.api.post("/v0.3/nodes/register", headers={"X-Join-Key": JOIN},
                          json={"name": name, "platform": "test", "cpu_cores": 4,
                                "ram_mb": 4096, "compute_score": score, "slots": 1,
                                "capabilities": ["tp_gpt2"]})
        self.assertEqual(r.status_code, 200, r.text)
        n = r.json()
        h = self.api.post(f"/v0.3/nodes/{n['id']}/heartbeat",
                          headers={"X-Node-Token": n["node_token"]},
                          json={"free_ram_mb": mem, "cpu_load_pct": 0})
        self.assertEqual(h.status_code, 200, h.text)
        return n

    def create(self, workers=2):
        return self.api.post("/v0.3.1/sessions/synthetic",
                             headers={"X-Admin-Key": ADMIN},
                             json={"workers": workers, "hidden": 16, "heads": 4,
                                   "layers": 2, "prompt_ids": [1, 2, 3],
                                   "max_new_tokens": 3})

    def assign(self, node):
        r = self.api.get(f"/v0.3.1/nodes/{node['id']}/assignment",
                         headers={"X-Node-Token": node["node_token"]})
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["assignment"]

    def ready(self, sid, node):
        return self.api.post(f"/v0.3.1/sessions/{sid}/ready",
                             headers={"X-Node-Token": node["node_token"]},
                             json={"node_id": node["id"]})

    def status(self, sid):
        r = self.api.get(f"/v0.3.1/sessions/{sid}",
                         headers={"X-Admin-Key": ADMIN})
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    def complete(self, sid, node, *, result=None, error=None):
        return self.api.post(f"/v0.3.1/sessions/{sid}/complete",
                             headers={"X-Node-Token": node["node_token"]},
                             json={"node_id": node["id"],
                                   "result": result, "error": error})

    def test_capacity_constraint_and_missing_nodes(self):
        self.assertEqual(self.create(workers=2).status_code, 409)
        self.node("low-ram", mem=64)
        self.assertEqual(self.create(workers=1).status_code, 409)
        self.node("ready", mem=2048)
        self.assertEqual(self.create(workers=1).status_code, 200)

    def test_auth_shard_ownership_and_lifecycle(self):
        high = self.node("high", score=100)
        low = self.node("low", score=10)
        other = self.node("other", score=1)
        self.assertEqual(self.api.post("/v0.3.1/sessions/synthetic", json={}).status_code, 401)
        created = self.create()
        self.assertEqual(created.status_code, 200, created.text)
        sid = created.json()["session_id"]
        a, b = self.assign(high), self.assign(low)
        self.assertEqual((a["rank"], b["rank"]), (0, 1))
        self.assertIsNone(self.assign(other))
        self.assertEqual(self.create().status_code, 409)
        self.assertEqual(self.api.get(f"/v0.3.1/nodes/{high['id']}/assignment").status_code, 401)
        wrong = self.api.get(f"/v0.3.1/sessions/{sid}/files/rank_1.pt",
                             params={"node_id": high["id"]},
                             headers={"X-Node-Token": high["node_token"]})
        self.assertEqual(wrong.status_code, 401)
        allowed = self.api.get(f"/v0.3.1/sessions/{sid}/files/rank_0.pt",
                               params={"node_id": high["id"]},
                               headers={"X-Node-Token": high["node_token"]})
        self.assertEqual(allowed.status_code, 200)
        self.assertEqual(len(allowed.content),
                         next(file["size"] for file in a["files"] if file["name"] == "rank_0.pt"))
        self.assertFalse(self.ready(sid, high).json()["all_ready"])
        self.assertEqual(self.status(sid)["status"], "preparing")
        self.assertTrue(self.ready(sid, low).json()["all_ready"])
        self.assertEqual(self.status(sid)["status"], "running")
        self.assertTrue(self.assign(high)["run"])
        self.assertEqual(self.complete(sid, high, result={"generated_ids": [3, 3]}).status_code, 200)
        self.assertEqual(self.complete(sid, low, result={"rank": 1}).status_code, 200)
        self.assertEqual(self.status(sid)["status"], "completed")
        self.assertEqual(self.complete(sid, low, result={"rank": 1}).status_code, 409)

    def test_preexported_local_checkpoint_is_staged(self):
        source = self.root / "checkpoints" / "small"
        source.mkdir(parents=True)
        state, config = synthetic_state(hidden=16, heads=4, layers=2,
                                        vocab=64, positions=64)
        export_state(state, config, source, 2, "trusted-export")
        self.node("a", score=100)
        self.node("b", score=10)
        created = self.api.post("/v0.3.1/sessions/checkpoint",
                                headers={"X-Admin-Key": ADMIN},
                                json={"checkpoint": "small", "prompt_ids": [1, 2],
                                      "max_new_tokens": 2})
        self.assertEqual(created.status_code, 200, created.text)
        self.assertEqual(created.json()["model"]["source"], "trusted-export")
        rejected = self.api.post("/v0.3.1/sessions/checkpoint",
                                 headers={"X-Admin-Key": ADMIN},
                                 json={"checkpoint": "../private", "prompt_ids": [1]})
        self.assertEqual(rejected.status_code, 422)

    def test_failed_rank_propagates_and_duplicate_rejected(self):
        a, b = self.node("a", score=100), self.node("b", score=10)
        sid = self.create().json()["session_id"]
        self.ready(sid, a)
        self.ready(sid, b)
        self.assertEqual(self.complete(sid, a, error="simulated failure").status_code, 200)
        self.assertEqual(self.status(sid)["status"], "failed")
        self.assertEqual(self.complete(sid, a, error="repeat").status_code, 409)


if __name__ == "__main__":
    unittest.main()
