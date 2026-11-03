"""Unit-level v0.3.3 gate: owned rendezvous and restart-from-prompt semantics.

These do not launch a real Gloo process group or contact another computer.
"""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from cluster_v03.api import create_router as v03_router
from cluster_v03.store import ClusterStore
from cluster_v031.api import create_router as tp_router
from cluster_v031.coordinator import SessionCoordinator
from compute.rendezvous import init_gloo


ADMIN = "v033-test-admin-secret-012345678901234"
JOIN = "v033-test-join-secret-012345678901234"


class RendezvousSessions(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = ClusterStore(self.root / "db.sqlite")
        self.coordinator = SessionCoordinator(self.store, self.root / "sessions")
        app = FastAPI()
        app.include_router(v03_router(self.store, admin_key=ADMIN, join_key=JOIN))
        app.include_router(tp_router(self.coordinator, admin_key=ADMIN))
        self.api = TestClient(app)
        self.servers = []
        self.counter = 24000

        def open_owned_store():
            self.counter += 1
            store = Mock()
            store.port = self.counter
            self.servers.append(store)
            return store

        # Unit tests replace the actual bound TCPStore with a fake. CI's
        # separate localhost HTTP+Gloo demo checks the real implementation.
        self.open_mock = patch.object(self.coordinator, "_open_rendezvous",
                                      side_effect=open_owned_store)
        self.open_mock.start()
        for i in range(2):
            body = {"name": f"test-{i}", "platform": "test",
                    "cpu_cores": 4, "ram_mb": 2048, "compute_score": 100-i,
                    "slots": 1, "capabilities": ["tp_gpt2"]}
            res = self.api.post("/v0.3/nodes/register",
                                headers={"X-Join-Key": JOIN}, json=body)
            self.assertEqual(res.status_code, 200, res.text)
            info = res.json()
            heartbeat = self.api.post(f"/v0.3/nodes/{info['id']}/heartbeat",
                headers={"X-Node-Token": info["node_token"]},
                json={"free_ram_mb": 1800, "cpu_load_pct": 0})
            self.assertEqual(heartbeat.status_code, 200)
            setattr(self, f"node{i}", info)

    def tearDown(self):
        self.open_mock.stop()
        self.api.close()
        self.tmp.cleanup()

    def session(self):
        return self.api.post("/v0.3.1/sessions/synthetic",
            headers={"X-Admin-Key": ADMIN},
            json={"workers": 2, "hidden": 16, "heads": 4, "layers": 1,
                  "prompt_ids": [1,2,3], "max_new_tokens": 2})

    def stop(self, sid, node):
        return self.api.post(f"/v0.3.1/sessions/{sid}/stopped",
            headers={"X-Node-Token": node["node_token"]},
            json={"node_id": node["id"]})

    def test_port_owned_and_failed_retry_makes_fresh_group(self):
        created = self.session()
        self.assertEqual(created.status_code, 200, created.text)
        sid = created.json()["session_id"]
        self.assertEqual(created.json()["max_new_tokens"], 2)
        self.assertIn(sid, self.coordinator._stores)
        self.assertIs(self.coordinator._stores[sid], self.servers[0])
        self.assertEqual(self.servers[0].port, 24001)

        # Must be explicitly failed/cancelled; not a resume during live compute.
        self.assertEqual(self.api.post(f"/v0.3.1/sessions/{sid}/retry",
            headers={"X-Admin-Key": ADMIN}).status_code, 409)
        self.assertEqual(self.api.post(f"/v0.3.1/sessions/{sid}/cancel",
            headers={"X-Admin-Key": ADMIN}).status_code, 200)
        self.assertEqual(self.api.post(f"/v0.3.1/sessions/{sid}/retry",
            headers={"X-Admin-Key": ADMIN}).status_code, 409)

        self.assertEqual(self.stop(sid, self.node0).status_code, 200)
        self.assertEqual(self.stop(sid, self.node1).status_code, 200)
        attempt = self.api.post(f"/v0.3.1/sessions/{sid}/retry",
                                headers={"X-Admin-Key": ADMIN})
        self.assertEqual(attempt.status_code, 200, attempt.text)
        retried = attempt.json()
        new_id = retried["session_id"]
        self.assertNotEqual(sid, new_id)
        self.assertEqual(retried["retry_of"], sid)
        self.assertEqual(retried["prompt_ids"], [1,2,3])
        self.assertEqual(retried["max_new_tokens"], 2)
        self.assertEqual(retried["status"], "preparing")
        self.assertIn(new_id, self.coordinator._stores)
        self.assertEqual(self.servers[-1].port, 24002)
        self.assertNotIn(sid, self.coordinator._stores)

    def test_retry_admin_only(self):
        sid = self.session().json()["session_id"]
        self.assertEqual(self.api.post(f"/v0.3.1/sessions/{sid}/retry").status_code, 401)
        self.assertEqual(self.api.post(f"/v0.3.1/sessions/../../retry",
            headers={"X-Admin-Key": ADMIN}).status_code, 404)

    def test_restart_invalidates_in_memory_rendezvous(self):
        sid = self.session().json()["session_id"]
        restarted = SessionCoordinator(self.store, self.root / "sessions")
        self.assertEqual(restarted.status(sid)["status"], "failed")
        self.assertIn("restarted", restarted.status(sid)["error"])


class ExternalGlooInitialization(unittest.TestCase):
    def test_managed_rank_joins_store_as_client(self):
        fake = Mock()
        env = {"OPENCOMPUTE_EXTERNAL_STORE": "1", "RANK": "1",
               "WORLD_SIZE": "2", "MASTER_ADDR": "127.0.0.1",
               "MASTER_PORT": "24000"}
        with patch.dict(os.environ, env), \
             patch("compute.rendezvous.dist.TCPStore", return_value=fake) as tcp, \
             patch("compute.rendezvous.dist.init_process_group") as init:
            instance = init_gloo(timeout_seconds=9)
        self.assertIs(instance, fake)
        self.assertEqual(tcp.call_args.kwargs["is_master"], False)
        self.assertEqual(init.call_args.kwargs["store"], fake)
        self.assertEqual(init.call_args.kwargs["rank"], 1)
        self.assertEqual(init.call_args.kwargs["world_size"], 2)

    def test_legacy_torchrun_unchanged(self):
        with patch.dict(os.environ, {"OPENCOMPUTE_EXTERNAL_STORE": "0"}), \
             patch("compute.rendezvous.dist.init_process_group") as init, \
             patch("compute.rendezvous.dist.TCPStore") as tcp:
            self.assertIsNone(init_gloo())
        tcp.assert_not_called()
        self.assertNotIn("store", init.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()
