import tempfile
import unittest
from pathlib import Path

from cluster_v03.store import ClusterStore, LeaseConflict
from cluster_v07.scheduler import GPUScheduler


MODEL = "a" * 64
PAYLOAD = {"model_id": MODEL, "prompt": "你好", "max_new_tokens": 16}


class Qwen2QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.tmp.cleanup)
        self.now = 1000
        self.store = ClusterStore(Path(self.tmp.name) / "cluster.sqlite", clock=lambda: self.now)
        self.scheduler = GPUScheduler(self.store, lease_seconds=60)

    def worker(self, name, *, model_id=None, uuid=None, index=0, free=8192, utilization=0):
        node = self.store.register(name=name, platform="linux", cpu_cores=4,
                                   ram_mb=8192, compute_score=1, slots=1,
                                   capabilities=["sum"])
        identity = node["id"], node["node_token"]
        self.store.report_gpu_inventory(*identity, [{
            "index": index, "uuid": uuid or name, "name": name,
            "vendor": "nvidia", "backend": "cuda", "runtime": "pytorch_cuda",
            "total_memory_mb": 8192, "free_memory_mb": free, "utilization_pct": utilization,
            "qwen2_model_id": model_id, "qwen2_vram_mb": 4096 if model_id else None,
        }])
        return identity

    def test_inference_uses_matching_model_and_worker_reservation(self):
        other = self.worker("other", model_id="b" * 64)
        target = self.worker("target", model_id=MODEL)
        task = self.scheduler.submit("qwen2_generate", PAYLOAD, required_vram_mb=64)
        self.assertEqual(task["required_vram_mb"], 4096)
        self.assertIsNone(self.scheduler.claim(*other)["task"])
        job = self.scheduler.claim(*target)["task"]
        self.assertEqual(job["id"], task["id"])
        self.assertEqual(job["payload"], PAYLOAD)
        result = {"text": "你好，世界", "generated_ids": [1, 2], "device": "cuda:0"}
        self.scheduler.complete(*target, job["id"], job["gpu_index"], job["lease_token"], result)
        self.assertEqual(self.scheduler.detail(task["id"])["result"], result)

    def test_inference_cannot_select_paths_or_an_unknown_model(self):
        self.worker("target", model_id=MODEL)
        for payload in (dict(PAYLOAD, model_path="/tmp/model"),
                        dict(PAYLOAD, model_id="b" * 64),
                        dict(PAYLOAD, prompt=" "), dict(PAYLOAD, max_new_tokens=65)):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.scheduler.submit("qwen2_generate", payload)

    def test_new_inference_requires_an_online_worker_for_the_model(self):
        self.worker("target", model_id=MODEL)
        self.now += 31
        with self.assertRaisesRegex(ValueError, "online"):
            self.scheduler.submit("qwen2_generate", PAYLOAD)

    def test_busy_inference_worker_does_not_block_an_idle_worker(self):
        busy = self.worker("busy", model_id=MODEL)
        self.scheduler.submit("qwen2_generate", PAYLOAD)
        self.assertIsNotNone(self.scheduler.claim(*busy)["task"])
        idle = self.worker("idle", model_id=MODEL, free=4096, utilization=25)
        queued = self.scheduler.submit("qwen2_generate", PAYLOAD)
        self.assertEqual(self.scheduler.claim(*idle)["task"]["id"], queued["id"])

    def test_cancelled_inference_holds_vram_until_worker_exits(self):
        worker = self.worker("target", model_id=MODEL, uuid="same")
        task = self.scheduler.submit("qwen2_generate", PAYLOAD, required_vram_mb=8192)
        job = self.scheduler.claim(*worker)["task"]
        self.scheduler.cancel(task["id"])
        self.assertEqual(self.scheduler.status()["reserved_vram_mb"], 8192)
        self.assertFalse(self.scheduler.lease_active(*worker, job["id"], job["gpu_index"], job["lease_token"]))
        with self.assertRaises(LeaseConflict):
            self.scheduler.lease_active(*worker, job["id"], job["gpu_index"], "wrong-token")
        with self.assertRaises(LeaseConflict):
            self.scheduler.retry(task["id"])
        other = self.worker("second registration", uuid="same", index=1)
        self.scheduler.submit("matmul_checksum", {"size": 16})
        self.assertIsNone(self.scheduler.claim(*other)["task"])
        self.assertIsNone(self.scheduler.claim(*worker)["task"])
        self.scheduler.complete(*worker, job["id"], job["gpu_index"], job["lease_token"], {})
        self.assertEqual(self.scheduler.status()["reserved_vram_mb"], 0)
        self.assertIsNone(self.scheduler.detail(task["id"])["result"])
        self.assertEqual(self.scheduler.retry(task["id"])["status"], "queued")

    def test_cancelled_lease_expiry_clears_the_assignment_for_retry(self):
        worker = self.worker("target", model_id=MODEL)
        task = self.scheduler.submit("qwen2_generate", PAYLOAD)
        self.scheduler.claim(*worker)
        self.scheduler.cancel(task["id"])
        self.now += 60
        released = self.scheduler.detail(task["id"])
        self.assertIsNone(released["lease_until"])
        self.assertIsNone(released["assigned_node"])
        self.assertEqual(self.scheduler.retry(task["id"])["status"], "queued")

    def test_offline_inference_retains_reservation_until_execution_deadline(self):
        worker = self.worker("target", model_id=MODEL, uuid="same")
        task = self.scheduler.submit("qwen2_generate", PAYLOAD, required_vram_mb=8192)
        self.scheduler.claim(*worker)
        self.now += 31
        other = self.worker("second registration", uuid="same", index=1)
        matrix = self.scheduler.submit("matmul_checksum", {"size": 16})
        self.assertIsNone(self.scheduler.claim(*other)["task"])
        self.assertEqual(self.scheduler.detail(task["id"])["status"], "leased")
        self.now += 29
        self.assertEqual(self.scheduler.claim(*other)["task"]["id"], matrix["id"])
        self.assertEqual(self.scheduler.detail(task["id"])["status"], "queued")

    def test_api_requires_admin_to_submit_and_node_credentials_to_check_lease(self):
        try:
            from fastapi import FastAPI
            from fastapi.testclient import TestClient
        except ModuleNotFoundError as exc:
            self.skipTest(f"API test dependency unavailable: {exc.name}")

        from cluster_v07.api import create_router

        admin = {"X-Admin-Key": "test-qwen2-admin-key-123"}
        node_id, node_token = self.worker("target", model_id=MODEL)
        node = {"X-Node-Token": node_token}
        app = FastAPI()
        app.include_router(create_router(self.store, admin_key=admin["X-Admin-Key"]))
        with TestClient(app) as client:
            request = {"kind": "qwen2_generate", "payload": PAYLOAD}
            queue = "/v0.7/gpu-tasks"
            self.assertEqual(client.post(queue, json=request).status_code, 401)
            submitted = client.post(queue, json=request, headers=admin)
            self.assertEqual(submitted.status_code, 200)
            task_id = submitted.json()["id"]
            job = client.post(f"/v0.7/nodes/{node_id}/gpu-claim", headers=node).json()["task"]
            identity = {"node_id": node_id, "gpu_index": job["gpu_index"],
                        "lease_token": job["lease_token"]}
            task_url = f"{queue}/{task_id}"
            self.assertEqual(client.post(task_url + "/lease", json=identity).status_code, 401)
            self.assertEqual(client.post(task_url + "/lease", json=identity, headers=node).json(),
                             {"active": True})
            self.assertEqual(client.post(task_url + "/cancel", headers=admin).json()["status"],
                             "cancelled")
            self.assertEqual(client.post(task_url + "/lease", json=identity, headers=node).json(),
                             {"active": False})
            completed = client.post(task_url + "/complete", json=identity, headers=node)
            self.assertEqual(completed.json()["status"], "cancelled")
            self.assertIsNone(client.get(task_url, headers=admin).json()["result"])
