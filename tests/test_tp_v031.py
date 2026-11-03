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
        self.assertEqual(self.complete(sid, high, result={"generated_ids": [3, 3, 3]}).status_code, 200)
        self.assertEqual(self.complete(sid, low, result={"rank": 1, "completed": True}).status_code, 200)
        self.assertEqual(self.status(sid)["status"], "completed")
        self.assertEqual(self.complete(sid, low, result={"rank": 1, "completed": True}).status_code, 409)

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


    def test_expired_session_cannot_be_readied_or_downloaded(self):
        node = self.node("expiry")
        created = self.create(workers=1)
        self.assertEqual(created.status_code, 200, created.text)
        sid = created.json()["session_id"]
        start = self.coordinator.clock()
        self.coordinator.clock = lambda: start + self.coordinator.prepare_ttl + 2
        self.assertEqual(self.ready(sid, node).status_code, 409)
        denied = self.api.get(f"/v0.3.1/sessions/{sid}/files/rank_0.pt",
                              params={"node_id": node["id"]},
                              headers={"X-Node-Token": node["node_token"]})
        self.assertEqual(denied.status_code, 401)
        self.assertEqual(self.status(sid)["status"], "failed")

    def test_expired_run_cannot_complete(self):
        node = self.node("expiry-run")
        sid = self.create(workers=1).json()["session_id"]
        self.assertTrue(self.ready(sid, node).json()["all_ready"])
        start = self.coordinator.clock()
        self.coordinator.clock = lambda: start + self.coordinator.run_ttl + 1
        self.assertEqual(self.complete(
            sid, node, result={"generated_ids": [1, 2, 3]}).status_code, 409)
        self.assertEqual(self.status(sid)["status"], "failed")

    def test_cancel_and_explicit_prune_are_admin_only(self):
        node = self.node("cancel")
        sid = self.create(workers=1).json()["session_id"]
        self.assertEqual(self.api.post(
            f"/v0.3.1/sessions/{sid}/cancel").status_code, 401)
        stop = self.api.post(f"/v0.3.1/sessions/{sid}/cancel",
                             headers={"X-Admin-Key": ADMIN})
        self.assertEqual(stop.status_code, 200, stop.text)
        self.assertEqual(self.status(sid)["status"], "failed")
        self.assertIsNone(self.assign(node))
        self.assertEqual(self.ready(sid, node).status_code, 409)
        self.assertEqual(self.api.post("/v0.3.1/sessions/prune",
            json={"older_than_seconds": 60}).status_code, 401)
        self.assertEqual(self.api.post(
            f"/v0.3.1/sessions/{sid}/stopped",
            headers={"X-Node-Token": node["node_token"]},
            json={"node_id": node["id"]}).status_code, 200)
        start = self.coordinator.clock()
        self.coordinator.clock = lambda: start + 61
        cleanup = self.api.post("/v0.3.1/sessions/prune",
            headers={"X-Admin-Key": ADMIN},
            json={"older_than_seconds": 60})
        self.assertEqual(cleanup.status_code, 200, cleanup.text)
        self.assertEqual(cleanup.json()["removed"], 1)
        self.assertFalse((self.root / "sessions" / sid).exists())
        self.assertEqual(self.api.get(f"/v0.3.1/sessions/{sid}",
            headers={"X-Admin-Key": ADMIN}).status_code, 404)

    def test_prepare_error_fails_group_immediately(self):
        a, b = self.node("prepare-a", score=100), self.node("prepare-b", score=10)
        sid = self.create().json()["session_id"]
        result = self.complete(sid, a, error="corrupt rank shard")
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(self.status(sid)["status"], "failed")
        self.assertIsNone(self.assign(b))
        self.assertEqual(self.ready(sid, b).status_code, 409)

    def test_result_format_validation(self):
        fast, slow = self.node("valid-a", score=50), self.node("valid-b", score=10)
        sid = self.create().json()["session_id"]
        self.ready(sid, fast)
        self.ready(sid, slow)
        self.assertEqual(self.complete(
            sid, fast, result={"generated_ids": [1]}).status_code, 422)
        self.assertEqual(self.complete(
            sid, slow, result={"rank": 1}).status_code, 422)
        self.assertEqual(self.complete(
            sid, slow, result={"rank": True, "completed": True}).status_code, 422)
        self.assertEqual(self.complete(
            sid, fast, error="").status_code, 422)
        self.assertEqual(self.complete(
            sid, fast, result={"generated_ids": [1, 2, 3]}).status_code, 200)
        self.assertEqual(self.complete(
            sid, slow, result={"rank": 1, "completed": True}).status_code, 200)

    def test_tp_reservations_block_regular_lease_until_cancelled(self):
        node = self.store.register(
            name="shared-capacity", platform="test", cpu_cores=4,
            ram_mb=4096, compute_score=100, slots=1,
            capabilities=["tp_gpt2", "sum"])
        self.store.heartbeat(node["id"], node["node_token"],
                             free_ram_mb=2048, cpu_load_pct=0)
        sid = self.create(workers=1).json()["session_id"]
        self.store.submit("sum", {"numbers": [1, 2]}, required_memory_mb=64)
        self.assertIsNone(self.store.claim(node["id"], node["node_token"])["task"])
        self.assertEqual(self.api.post(
            f"/v0.3.1/sessions/{sid}/cancel",
            headers={"X-Admin-Key": ADMIN}).status_code, 200)
        # A failed process group keeps its reservation until its Worker
        # actually acknowledges teardown, or the conservative grace expires.
        self.assertIsNone(self.store.claim(node["id"], node["node_token"])["task"])
        self.assertEqual(self.api.post(
            f"/v0.3.1/sessions/{sid}/stopped",
            headers={"X-Node-Token": node["node_token"]},
            json={"node_id": node["id"]}).status_code, 200)
        self.assertIsNotNone(self.store.claim(node["id"], node["node_token"])["task"])


if __name__ == "__main__":
    unittest.main()
