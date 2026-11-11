import importlib.util
import tempfile
import unittest
from pathlib import Path

from cluster_v03.store import ClusterStore
from public_pool.worker import cpu_checksum


@unittest.skipUnless(importlib.util.find_spec("fastapi"), "FastAPI test dependencies are required")
class PublicPoolAPITests(unittest.TestCase):
    def test_public_admission_and_worker_download_need_no_administrator_key(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from public_pool.api import create_router

        with tempfile.TemporaryDirectory(dir=Path.cwd()) as folder:
            now = [1000]
            store = ClusterStore(Path(folder) / "cluster.sqlite", clock=lambda: now[0])
            app = FastAPI()
            app.include_router(create_router(store, enabled=True, signing_secret="test-admin-signing-secret"))
            with TestClient(app) as client:
                prefix = "/v0.8/public"
                self.assertEqual(client.get(prefix + "/status").json()["cpu_workers"], 0)
                source = client.get(prefix + "/worker.py")
                self.assertEqual(source.status_code, 200)
                self.assertIn("attachment", source.headers["content-disposition"])
                compile(source.text, "worker.py", "exec")
                self.assertEqual(client.post(prefix + "/challenge", json={"resource": "cpu", "padding": "x" * 4096}).status_code, 413)
                self.assertEqual(client.post(prefix + "/challenge", json={"resource": "cpu", "unknown": 1}).status_code, 422)
                challenge = client.post(prefix + "/challenge", json={"resource": "cpu"}).json()
                response = client.post(prefix + "/join", json={"challenge": challenge,
                    "resources": {"cores": 2, "ram_mb": 2048, "vram_mb": 0},
                    "checksum": cpu_checksum(challenge["seed"])})
                self.assertEqual(response.status_code, 200)
                worker = response.json()
                now[0] += 20
                request = {"node_id": worker["node_id"], "seed": worker["seed"],
                           "checksum": cpu_checksum(worker["seed"])}
                self.assertEqual(client.post(prefix + "/heartbeat", json=request).status_code, 401)
                result = client.post(prefix + "/heartbeat", json=request,
                                     headers={"Authorization": "Bearer " + worker["token"]})
                self.assertEqual(result.status_code, 200)
                self.assertEqual(client.get(prefix + "/status").json()["verified_checks"], 2)

    def test_disabled_pool_reports_state_and_refuses_join(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from public_pool.api import create_router

        with tempfile.TemporaryDirectory(dir=Path.cwd()) as folder:
            app = FastAPI()
            app.include_router(create_router(ClusterStore(Path(folder) / "cluster.sqlite"),
                                             enabled=False, signing_secret="test-admin-signing-secret"))
            with TestClient(app) as client:
                self.assertFalse(client.get("/v0.8/public/status").json()["enabled"])
                self.assertEqual(client.post("/v0.8/public/challenge", json={"resource": "cpu"}).status_code, 503)
