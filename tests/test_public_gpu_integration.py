import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from cluster_v03.store import ClusterStore
from cluster_v07.api import create_router as create_gpu_router
from cluster_v07.scheduler import expected_matmul_checksum
from public_pool.api import create_router as create_public_pool_router
from public_pool.worker import cpu_checksum


ADMIN = "public-gpu-integration-admin-key-0123456789"


class FakeClock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class PublicGPUIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.clock = FakeClock()
        self.store = ClusterStore(
            Path(self.temp.name) / "cluster.sqlite",
            clock=self.clock,
            heartbeat_ttl=30,
            lease_seconds=25,
        )
        app = FastAPI()
        app.include_router(create_gpu_router(self.store, admin_key=ADMIN))
        app.include_router(create_public_pool_router(self.store, enabled=True))
        self.client = TestClient(app)

    def tearDown(self):
        self.client.close()
        self.temp.cleanup()

    def admit_gpu_worker(self):
        prefix = "/v0.8/public"
        challenge = self.client.post(
            prefix + "/challenge", json={"resource": "cuda"}
        )
        self.assertEqual(challenge.status_code, 200, challenge.text)
        value = challenge.json()
        joined = self.client.post(prefix + "/join", json={
            "challenge": value,
            "resources": {"cores": 4, "ram_mb": 8192, "vram_mb": 8192},
            "checksum": cpu_checksum(value["seed"]),
        })
        self.assertEqual(joined.status_code, 200, joined.text)
        return joined.json()

    def test_public_cuda_worker_joins_gpu_pool_but_not_trusted_cpu_pool(self):
        worker = self.admit_gpu_worker()
        node_id, token = worker["node_id"], worker["token"]

        nodes = self.store.status()["nodes"]
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0]["role"], "public_gpu")
        self.assertEqual(nodes[0]["capabilities"], [])
        with self.assertRaises(PermissionError):
            self.store.heartbeat(
                node_id, token, free_ram_mb=8192, cpu_load_pct=0.0
            )

        inventory = self.client.post(
            f"/v0.7/nodes/{node_id}/gpus",
            headers={"X-Node-Token": token},
            json={"devices": [{
                "index": 0,
                "uuid": "GPU-public-integration-test",
                "name": "NVIDIA Public Test GPU",
                "vendor": "nvidia",
                "backend": "cuda",
                "total_memory_mb": 8192,
                "free_memory_mb": 8192,
                "utilization_pct": 5.0,
                "compute_capability": "8.6",
                "supported_dtypes": ["fp32", "fp16", "bf16", "tf32"],
                "runtime": "pytorch_cuda",
                "runtime_version": "2.6.0",
            }]},
        )
        self.assertEqual(inventory.status_code, 200, inventory.text)

        submitted = self.client.post(
            "/v0.7/gpu-tasks",
            headers={"X-Admin-Key": ADMIN},
            json={
                "kind": "matmul_checksum",
                "payload": {"size": 16},
                "required_vram_mb": 256,
            },
        )
        self.assertEqual(submitted.status_code, 200, submitted.text)
        task_id = submitted.json()["id"]

        claimed = self.client.post(
            f"/v0.7/nodes/{node_id}/gpu-claim",
            headers={"X-Node-Token": token},
        )
        self.assertEqual(claimed.status_code, 200, claimed.text)
        task = claimed.json()["task"]
        self.assertEqual(task["id"], task_id)
        self.assertEqual(task["kind"], "matmul_checksum")

        rejected = self.client.post(
            f"/v0.7/gpu-tasks/{task_id}/complete",
            headers={"X-Node-Token": token},
            json={
                "node_id": node_id,
                "gpu_index": task["gpu_index"],
                "lease_token": task["lease_token"],
                "result": {"checksum": 123, "size": 16,
                           "operation": "cuda_matmul_checksum"},
                "error": None,
            },
        )
        self.assertEqual(rejected.status_code, 200, rejected.text)
        self.assertEqual(rejected.json()["status"], "queued")

        retried = self.client.post(
            f"/v0.7/nodes/{node_id}/gpu-claim",
            headers={"X-Node-Token": token},
        )
        self.assertEqual(retried.status_code, 200, retried.text)
        retry_task = retried.json()["task"]
        self.assertEqual(retry_task["id"], task_id)
        completed = self.client.post(
            f"/v0.7/gpu-tasks/{task_id}/complete",
            headers={"X-Node-Token": token},
            json={
                "node_id": node_id,
                "gpu_index": retry_task["gpu_index"],
                "lease_token": retry_task["lease_token"],
                "result": {
                    "checksum": expected_matmul_checksum(16),
                    "size": 16,
                    "operation": "cuda_matmul_checksum",
                },
                "error": None,
            },
        )
        self.assertEqual(completed.status_code, 200, completed.text)
        self.assertEqual(completed.json()["status"], "completed")

        inventory_view = self.client.get(
            "/v0.7/gpus", headers={"X-Admin-Key": ADMIN}
        )
        self.assertEqual(inventory_view.status_code, 200, inventory_view.text)
        device = inventory_view.json()["devices"][0]
        self.assertEqual(device["node_role"], "public_gpu")
        self.assertEqual(device["verification"], "self_reported")

    def test_public_gpu_cannot_claim_qwen2_or_advertise_fake_model_assets(self):
        worker = self.admit_gpu_worker()
        node_id, token = worker["node_id"], worker["token"]
        device = {
            "index": 0,
            "uuid": "GPU-public-qwen-guard-test",
            "name": "NVIDIA Public Test GPU",
            "vendor": "nvidia",
            "backend": "cuda",
            "total_memory_mb": 8192,
            "free_memory_mb": 8192,
            "utilization_pct": 5.0,
            "compute_capability": "8.6",
            "supported_dtypes": ["fp32", "fp16", "bf16", "tf32"],
            "runtime": "pytorch_cuda",
            "runtime_version": "2.6.0",
        }
        fake_model = dict(device, qwen2_model_id="a" * 64, qwen2_vram_mb=4096)
        rejected_model = self.client.post(
            f"/v0.7/nodes/{node_id}/gpus",
            headers={"X-Node-Token": token},
            json={"devices": [fake_model]},
        )
        self.assertEqual(rejected_model.status_code, 422, rejected_model.text)

        second_device = dict(device, index=1, uuid="GPU-public-qwen-guard-test-2")
        rejected_multi = self.client.post(
            f"/v0.7/nodes/{node_id}/gpus",
            headers={"X-Node-Token": token},
            json={"devices": [device, second_device]},
        )
        self.assertEqual(rejected_multi.status_code, 422, rejected_multi.text)

        accepted = self.client.post(
            f"/v0.7/nodes/{node_id}/gpus",
            headers={"X-Node-Token": token},
            json={"devices": [device]},
        )
        self.assertEqual(accepted.status_code, 200, accepted.text)

        # Simulate stale/corrupted inventory data to verify the scheduler still
        # refuses to hand Qwen2 work to a public Worker.
        with self.store._tx() as conn:
            conn.execute(
                "UPDATE gpu_devices SET qwen2_model_id=?,qwen2_vram_mb=? WHERE node_id=?",
                ("a" * 64, 4096, node_id),
            )
        qwen_task = self.client.post(
            "/v0.7/gpu-tasks",
            headers={"X-Admin-Key": ADMIN},
            json={
                "kind": "qwen2_generate",
                "payload": {"model_id": "a" * 64, "prompt": "hello", "max_new_tokens": 1},
                "required_vram_mb": 4096,
            },
        )
        self.assertEqual(qwen_task.status_code, 200, qwen_task.text)
        claimed = self.client.post(
            f"/v0.7/nodes/{node_id}/gpu-claim",
            headers={"X-Node-Token": token},
        )
        self.assertEqual(claimed.status_code, 200, claimed.text)
        self.assertIsNone(claimed.json()["task"])

    def test_public_heartbeat_refreshes_gpu_scheduler_liveness(self):
        worker = self.admit_gpu_worker()
        self.clock.advance(20)
        response = self.client.post(
            "/v0.8/public/heartbeat",
            headers={"Authorization": "Bearer " + worker["token"]},
            json={
                "node_id": worker["node_id"],
                "seed": worker["seed"],
                "checksum": cpu_checksum(worker["seed"]),
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        node = self.store.status()["nodes"][0]
        self.assertTrue(node["online"])
        self.assertEqual(node["role"], "public_gpu")


if __name__ == "__main__":
    unittest.main()
