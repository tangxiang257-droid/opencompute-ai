import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
from threading import Thread
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.request import urlopen

from cluster_v03.store import ClusterStore
from public_pool.worker import cpu_checksum, memory_mb


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
            app.include_router(create_router(store, enabled=True))
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
                                             enabled=False))
            with TestClient(app) as client:
                self.assertFalse(client.get("/v0.8/public/status").json()["enabled"])
                self.assertEqual(client.post("/v0.8/public/challenge", json={"resource": "cpu"}).status_code, 503)

    def test_downloaded_cpu_worker_joins_and_renews_over_http(self):
        if (os.cpu_count() or 1) < 2 or memory_mb() < 2048:
            self.skipTest("Worker test needs two CPU cores and 2 GiB of RAM")

        from fastapi import FastAPI
        import uvicorn

        from public_pool.api import create_router

        with tempfile.TemporaryDirectory(dir=Path.cwd()) as folder:
            root = Path(folder)
            app = FastAPI()
            app.include_router(create_router(ClusterStore(root / "cluster.sqlite"), enabled=True))
            with socket.socket() as listener, patch("public_pool.store.HEARTBEAT_SECONDS", 1):
                listener.bind(("127.0.0.1", 0))
                listener.listen()
                server = uvicorn.Server(uvicorn.Config(app, log_level="warning"))
                thread = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
                thread.start()
                try:
                    deadline = time.monotonic() + 5
                    while not server.started and time.monotonic() < deadline:
                        time.sleep(0.02)
                    self.assertTrue(server.started, "local HTTP server failed to start")
                    origin = f"http://127.0.0.1:{listener.getsockname()[1]}"
                    prefix = origin + "/v0.8/public"
                    worker = root / "opencompute-worker.py"
                    with urlopen(prefix + "/worker.py", timeout=2) as response:
                        worker.write_bytes(response.read())
                    with subprocess.Popen([sys.executable, str(worker), "--url", origin],
                                          stdout=subprocess.DEVNULL, stderr=subprocess.PIPE) as process:
                        try:
                            deadline = time.monotonic() + 6
                            stats = {}
                            while process.poll() is None and time.monotonic() < deadline:
                                with urlopen(prefix + "/status", timeout=2) as response:
                                    stats = json.load(response)
                                if stats["verified_checks"] >= 2:
                                    break
                                time.sleep(0.1)
                            self.assertEqual(stats.get("cpu_workers"), 1)
                            self.assertGreaterEqual(stats.get("verified_checks", 0), 2)
                        finally:
                            if process.poll() is None:
                                process.terminate()
                            process.communicate(timeout=4)
                finally:
                    server.should_exit = True
                    thread.join(timeout=4)
