"""One-click public CPU browser admission revocation contracts."""
import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from cluster_v03.store import ClusterStore
from public_pool.worker import cpu_checksum


@unittest.skipUnless(importlib.util.find_spec("fastapi"), "FastAPI dependencies required")
class BrowserJoinAPITests(unittest.TestCase):
    def test_cpu_browser_can_join_and_leave_without_admin_credentials(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from public_pool.api import create_router

        with TemporaryDirectory(dir=Path.cwd()) as folder:
            clock = [10000.0]
            store = ClusterStore(Path(folder) / "db.sqlite", clock=lambda: clock[0])
            app = FastAPI()
            app.include_router(create_router(store, enabled=True))
            with TestClient(app) as client:
                base = "/v0.8/public"
                challenge = client.post(base + "/challenge", json={"resource": "cpu"}).json()
                response = client.post(base + "/join", json={
                    "challenge": challenge,
                    "resources": {"cores": 4, "ram_mb": 4096, "vram_mb": 0},
                    "checksum": cpu_checksum(challenge["seed"]),
                })
                self.assertEqual(response.status_code, 200, response.text)
                identity = response.json()
                self.assertEqual(client.get(base + "/status").json()["cpu_workers"], 1)
                endpoint = base + "/leave"
                request = {"node_id": identity["node_id"]}
                self.assertEqual(client.post(endpoint, json=request).status_code, 401)
                self.assertEqual(client.post(endpoint, json=request, headers={
                    "Authorization": "Bearer " + "f" * 43,
                }).status_code, 401)
                self.assertEqual(client.post(endpoint, json={**request, "extra": True},
                    headers={"Authorization": "Bearer " + identity["token"]}).status_code, 422)
                good = client.post(endpoint, json=request, headers={
                    "Authorization": "Bearer " + identity["token"],
                })
                self.assertEqual(good.status_code, 200, good.text)
                self.assertEqual(good.json(), {"ok": True})
                self.assertEqual(client.get(base + "/status").json()["cpu_workers"], 0)
                self.assertEqual(client.post(endpoint, json=request, headers={
                    "Authorization": "Bearer " + identity["token"],
                }).status_code, 401)
                self.assertEqual(client.post(base + "/heartbeat", json={
                    "node_id": identity["node_id"], "seed": identity["seed"],
                    "checksum": cpu_checksum(identity["seed"]),
                }, headers={"Authorization": "Bearer " + identity["token"]}).status_code, 401)

    def test_leave_does_not_disable_public_admission(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from public_pool.api import create_router
        with TemporaryDirectory(dir=Path.cwd()) as folder:
            app = FastAPI()
            app.include_router(create_router(ClusterStore(Path(folder) / "db.sqlite"),
                                             enabled=False))
            with TestClient(app) as client:
                self.assertEqual(client.post("/v0.8/public/leave",
                    json={"node_id": "a" * 32}).status_code, 503)


if __name__ == "__main__":
    unittest.main()
