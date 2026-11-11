import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from cluster_v03.store import ClusterStore
from cluster_v07.detection import detect_nvidia_gpus
from cluster_v07.api import create_router

ADMIN = "test-admin-key-for-gpu-pool-012345"


class FakeClock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class GPUInventoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.clock = FakeClock()
        self.store = ClusterStore(Path(self.temp.name) / "cluster.sqlite",
                                  clock=self.clock, heartbeat_ttl=30)
        self.node = self.store.register(
            name="gpu-host", platform="linux", cpu_cores=8, ram_mb=16384,
            compute_score=10, slots=1, capabilities=["sum"])
        app = FastAPI()
        app.include_router(create_router(self.store, admin_key=ADMIN))
        self.client = TestClient(app)

    def tearDown(self):
        self.client.close()
        self.temp.cleanup()

    def gpu(self, index=0, **overrides):
        value = {
            "index": index, "uuid": f"GPU-test-{index}", "name": "NVIDIA Test GPU",
            "vendor": "nvidia", "backend": "cuda", "total_memory_mb": 8192,
            "free_memory_mb": 4096, "utilization_pct": 12.5,
            "compute_capability": "8.6", "supported_dtypes": ["fp32", "fp16", "bf16", "tf32"],
        }
        value.update(overrides)
        return value

    def report(self, devices, token=None):
        return self.client.post(
            f"/v0.7/nodes/{self.node['id']}/gpus",
            json={"devices": devices},
            headers={"X-Node-Token": token or self.node["node_token"]},
        )

    def test_admin_authentication_is_required_for_inventory(self):
        self.assertEqual(self.client.get("/v0.7/gpus").status_code, 401)
        response = self.client.get("/v0.7/gpus", headers={"X-Admin-Key": ADMIN})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["summary"]["registered"], 0)

    def test_node_token_authentication_is_required_for_reporting(self):
        self.assertEqual(self.report([self.gpu()], token="wrong-token").status_code, 401)

    def test_reported_gpu_is_visible_and_capacity_is_aggregated(self):
        response = self.report([self.gpu()])
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["reported"], 1)
        data = self.client.get("/v0.7/gpus", headers={"X-Admin-Key": ADMIN}).json()
        self.assertEqual(data["summary"]["online"], 1)
        self.assertEqual(data["summary"]["total_vram_mb"], 8192)
        self.assertEqual(data["summary"]["free_vram_mb"], 4096)
        self.assertEqual(data["devices"][0]["verification"], "self_reported")
        self.assertNotIn("node_token", str(data))

    def test_inventory_report_replaces_removed_devices(self):
        self.assertEqual(self.report([self.gpu(0), self.gpu(1)]).status_code, 200)
        self.assertEqual(self.report([self.gpu(1)]).status_code, 200)
        data = self.client.get("/v0.7/gpus", headers={"X-Admin-Key": ADMIN}).json()
        self.assertEqual(data["summary"]["registered"], 1)
        self.assertEqual(data["devices"][0]["gpu_index"], 1)

    def test_empty_report_clears_inventory(self):
        self.assertEqual(self.report([self.gpu()]).status_code, 200)
        self.assertEqual(self.report([]).status_code, 200)
        data = self.client.get("/v0.7/gpus", headers={"X-Admin-Key": ADMIN}).json()
        self.assertEqual(data["summary"]["registered"], 0)

    def test_rejects_invalid_memory_and_duplicate_indexes(self):
        self.assertEqual(self.report([self.gpu(free_memory_mb=9000)]).status_code, 422)
        self.assertEqual(self.report([self.gpu(0), self.gpu(0, uuid="GPU-duplicate")]).status_code, 422)

    def test_stale_node_is_not_counted_as_online_gpu_capacity(self):
        self.assertEqual(self.report([self.gpu()]).status_code, 200)
        self.clock.advance(31)
        data = self.client.get("/v0.7/gpus", headers={"X-Admin-Key": ADMIN}).json()
        self.assertEqual(data["summary"]["online"], 0)
        self.assertEqual(data["summary"]["offline"], 1)
        self.assertEqual(data["summary"]["total_vram_mb"], 0)

    def test_nvidia_smi_detection_parses_inventory_without_shell(self):
        output = '0, GPU-abc, "NVIDIA GeForce RTX Test", 8192, 4096, 37, 8.6\n'
        completed = subprocess.CompletedProcess(args=["nvidia-smi"], returncode=0,
                                                 stdout=output, stderr="")
        with patch("cluster_v07.detection.shutil.which", return_value="/usr/bin/nvidia-smi"):
            with patch("cluster_v07.detection.subprocess.run", return_value=completed) as run:
                devices = detect_nvidia_gpus()
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices[0]["name"], "NVIDIA GeForce RTX Test")
        self.assertEqual(devices[0]["free_memory_mb"], 4096)
        self.assertIn("bf16", devices[0]["supported_dtypes"])
        self.assertEqual(run.call_args.kwargs["timeout"], 5)
        self.assertNotIn("shell", run.call_args.kwargs)

    def test_detection_returns_empty_when_nvidia_smi_is_unavailable(self):
        with patch("cluster_v07.detection.shutil.which", return_value=None):
            self.assertEqual(detect_nvidia_gpus(), [])


if __name__ == "__main__":
    unittest.main()
