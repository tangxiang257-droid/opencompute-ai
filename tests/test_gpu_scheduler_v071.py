import concurrent.futures
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from cluster_v03.store import ClusterStore, LeaseConflict
from cluster_v07.scheduler import GPUScheduler, validate_gpu_task
from cluster_v07.api import create_router
from cluster_v07.worker import map_gpu_inventory_to_cuda

ADMIN = "gpu-scheduler-admin-key-0123456789"


class FakeClock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class GPUTaskSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.clock = FakeClock()
        self.store = ClusterStore(
            Path(self.temp.name) / "cluster.sqlite",
            clock=self.clock, heartbeat_ttl=30, lease_seconds=25,
        )
        self.node = self.register("gpu-host")
        self.scheduler = GPUScheduler(self.store, lease_seconds=60)
        app = FastAPI()
        app.include_router(create_router(self.store, admin_key=ADMIN))
        self.client = TestClient(app)

    def tearDown(self):
        self.client.close()
        self.temp.cleanup()

    def register(self, name):
        return self.store.register(
            name=name, platform="linux", cpu_cores=8, ram_mb=16384,
            compute_score=10, slots=2, capabilities=["sum"],
        )

    def gpu(self, index=0, **overrides):
        value = {
            "index": index, "uuid": f"GPU-test-{index}",
            "name": f"NVIDIA Test GPU {index}", "vendor": "nvidia",
            "backend": "cuda", "total_memory_mb": 8192,
            "free_memory_mb": 8192, "utilization_pct": 10.0,
            "compute_capability": "8.6",
            "supported_dtypes": ["fp32", "fp16", "bf16", "tf32"],
            "runtime": "pytorch_cuda", "runtime_version": "2.6.0",
        }
        value.update(overrides)
        return value

    def report(self, node=None, devices=None, token=None):
        node = node or self.node
        return self.client.post(
            f"/v0.7/nodes/{node['id']}/gpus",
            json={"devices": devices if devices is not None else [self.gpu()]},
            headers={"X-Node-Token": token or node["node_token"]},
        )

    def submit(self, required_vram_mb=256, size=128):
        return self.client.post(
            "/v0.7/gpu-tasks",
            json={"kind": "matmul_checksum", "payload": {"size": size},
                  "required_vram_mb": required_vram_mb},
            headers={"X-Admin-Key": ADMIN},
        )

    def claim(self, node=None, token=None):
        node = node or self.node
        return self.client.post(
            f"/v0.7/nodes/{node['id']}/gpu-claim",
            headers={"X-Node-Token": token or node["node_token"]},
        )

    def test_admin_authentication_is_required_for_gpu_task_queue(self):
        self.assertEqual(self.client.get("/v0.7/gpu-tasks").status_code, 401)
        self.assertEqual(self.client.post(
            "/v0.7/gpu-tasks",
            json={"kind": "matmul_checksum", "payload": {"size": 128}},
        ).status_code, 401)
        self.assertEqual(self.client.get(
            "/v0.7/gpu-tasks", headers={"X-Admin-Key": ADMIN}
        ).status_code, 200)

    def test_gpu_task_payload_and_vram_estimate_are_bounded(self):
        self.assertEqual(self.submit(size=8).status_code, 422)
        self.assertEqual(self.submit(size=512, required_vram_mb=64).status_code, 422)
        invalid = self.client.post(
            "/v0.7/gpu-tasks",
            json={"kind": "matmul_checksum",
                  "payload": {"size": 128, "source": "print(1)"}},
            headers={"X-Admin-Key": ADMIN},
        )
        self.assertEqual(invalid.status_code, 422)
        with self.assertRaises(ValueError):
            validate_gpu_task("arbitrary_python", {"source": "print(1)"}, 256)

    def test_worker_runtime_is_required_before_gpu_can_be_scheduled(self):
        self.assertEqual(self.report(devices=[self.gpu(runtime="unknown")]).status_code, 200)
        self.assertEqual(self.submit().status_code, 200)
        self.assertIsNone(self.claim().json()["task"])
        self.assertEqual(self.report().status_code, 200)
        self.assertIsNotNone(self.claim().json()["task"])

    def test_claim_reserves_vram_and_prevents_overcommit(self):
        self.assertEqual(self.report().status_code, 200)
        first = self.submit(required_vram_mb=6000).json()
        self.clock.advance(1)
        self.submit(required_vram_mb=3000)
        claimed = self.claim().json()["task"]
        self.assertEqual(claimed["id"], first["id"])
        self.assertEqual(claimed["gpu_index"], 0)
        self.assertIsNone(self.claim().json()["task"])
        status = self.client.get("/v0.7/gpu-tasks",
                                 headers={"X-Admin-Key": ADMIN}).json()
        self.assertEqual(status["reserved_vram_mb"], 6000)
        self.assertEqual(status["tasks"][0]["status"], "queued")
        self.assertEqual(status["tasks"][1]["status"], "leased")

    def test_fresh_telemetry_does_not_release_an_active_reservation(self):
        self.report()
        first = self.submit(required_vram_mb=6000).json()
        self.assertEqual(self.claim().json()["task"]["id"], first["id"])
        self.clock.advance(1)
        self.report()
        second = self.submit(required_vram_mb=3000).json()
        self.assertIsNone(self.claim().json()["task"])
        self.scheduler.cancel(first["id"])
        self.assertEqual(self.claim().json()["task"]["id"], second["id"])

    def test_duplicate_device_uuid_shares_reservations_between_workers(self):
        self.report()
        first = self.submit(required_vram_mb=6000).json()
        self.assertEqual(self.claim().json()["task"]["id"], first["id"])
        self.report(devices=[self.gpu(free_memory_mb=0)])

        other = self.register("same-physical-gpu")
        self.report(node=other, devices=[self.gpu(index=1, uuid="GPU-test-0")])
        second = self.submit(required_vram_mb=3000).json()
        self.assertIsNone(self.claim(node=other).json()["task"])
        self.scheduler.cancel(first["id"])
        self.assertEqual(self.claim(node=other).json()["task"]["id"], second["id"])

    def test_oversized_jobs_do_not_block_a_runnable_task(self):
        self.report()
        for _ in range(100):
            self.submit(required_vram_mb=65536)
        self.clock.advance(1)
        task = self.submit().json()
        self.assertEqual(self.claim().json()["task"]["id"], task["id"])

    def test_rejects_non_finite_results_without_losing_the_lease(self):
        self.report()
        task = self.submit().json()
        lease = self.claim().json()["task"]
        identity = (self.node["id"], self.node["node_token"], task["id"],
                    lease["gpu_index"], lease["lease_token"])
        with self.assertRaisesRegex(ValueError, "valid JSON"):
            self.scheduler.complete(*identity, {"checksum": float("nan")})
        self.assertEqual(self.scheduler.complete(*identity, {"checksum": 7})["status"],
                         "completed")

    def test_scheduler_prefers_less_busy_gpu(self):
        self.report(devices=[
            self.gpu(0, utilization_pct=90.0),
            self.gpu(1, utilization_pct=5.0),
        ])
        self.submit()
        job = self.claim().json()["task"]
        self.assertEqual(job["gpu_index"], 1)

    def test_lease_expiry_requeues_and_releases_reservation(self):
        self.report()
        task = self.submit(required_vram_mb=6000).json()
        first = self.claim().json()["task"]
        self.clock.advance(121)
        self.store.heartbeat(self.node["id"], self.node["node_token"],
                             free_ram_mb=8192, cpu_load_pct=5)
        self.report()
        second = self.claim().json()["task"]
        self.assertEqual(second["id"], task["id"])
        self.assertNotEqual(first["lease_token"], second["lease_token"])
        detail = self.client.get(
            f"/v0.7/gpu-tasks/{task['id']}", headers={"X-Admin-Key": ADMIN}
        ).json()
        self.assertEqual(detail["attempts"], 2)
        self.assertEqual(detail["status"], "leased")

    def test_completion_requires_node_token_gpu_index_and_lease_token(self):
        self.report()
        task = self.submit().json()
        job = self.claim().json()["task"]
        url = f"/v0.7/gpu-tasks/{task['id']}/complete"
        body = {
            "node_id": self.node["id"], "gpu_index": 0,
            "lease_token": job["lease_token"], "result": {"checksum": 123},
        }
        self.assertEqual(self.client.post(url, json=body).status_code, 401)
        headers = {"X-Node-Token": self.node["node_token"]}
        self.assertEqual(self.client.post(
            url, json=dict(body, gpu_index=1), headers=headers
        ).status_code, 409)
        self.assertEqual(self.client.post(
            url, json=dict(body, lease_token="not-the-lease"), headers=headers
        ).status_code, 409)
        response = self.client.post(url, json=body, headers=headers)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["status"], "completed")
        detail = self.client.get(
            f"/v0.7/gpu-tasks/{task['id']}", headers={"X-Admin-Key": ADMIN}
        ).json()
        self.assertEqual(detail["result"]["checksum"], 123)
        self.assertNotIn("lease_token", detail)

    def test_cancel_retry_and_stale_lease_rejection(self):
        self.report()
        task = self.submit().json()
        old = self.claim().json()["task"]
        url = f"/v0.7/gpu-tasks/{task['id']}"
        cancelled = self.client.post(url + "/cancel", headers={"X-Admin-Key": ADMIN})
        self.assertEqual(cancelled.status_code, 200)
        self.assertEqual(cancelled.json()["status"], "cancelled")
        self.assertEqual(self.client.post(
            url + "/retry", headers={"X-Admin-Key": ADMIN}
        ).status_code, 200)
        new = self.claim().json()["task"]
        with self.assertRaises(LeaseConflict):
            self.scheduler.complete(
                self.node["id"], self.node["node_token"], task["id"], 0,
                old["lease_token"], {"checksum": 1},
            )
        self.assertNotEqual(old["lease_token"], new["lease_token"])

    def test_concurrent_workers_cannot_claim_the_same_gpu_task(self):
        self.report()
        other = self.register("gpu-host-2")
        self.report(node=other)
        self.submit()

        def claim(node):
            return self.scheduler.claim(node["id"], node["node_token"])["task"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, (self.node, other)))
        self.assertEqual(len([item for item in results if item is not None]), 1)

    def test_worker_offline_releases_gpu_lease(self):
        self.report()
        other = self.register("gpu-host-2")
        self.report(node=other, devices=[self.gpu(utilization_pct=95.0)])
        task = self.submit().json()
        first = self.claim().json()["task"]
        self.clock.advance(31)
        self.store.heartbeat(other["id"], other["node_token"],
                             free_ram_mb=8192, cpu_load_pct=5)
        second = self.scheduler.claim(other["id"], other["node_token"])["task"]
        self.assertEqual(second["id"], task["id"])
        self.assertNotEqual(first["lease_token"], second["lease_token"])


    def test_cuda_logical_indices_are_mapped_by_gpu_uuid(self):
        a = "a1712003-9000-4779-9944-dbe8d14d299d"
        b = "b1712003-9000-4779-9944-dbe8d14d299d"
        name = "NVIDIA Same Model"
        inventory = [self.gpu(0, uuid=f"GPU-{a}", name=name),
                     self.gpu(1, uuid=f"GPU-{b}", name=name)]
        runtime = {
            "runtime": "pytorch_cuda",
            "runtime_version": "2.7.0",
            "devices": [
                {"index": 0, "uuid": b, "name": name},
                {"index": 1, "uuid": a, "name": name},
            ],
        }
        mapped = map_gpu_inventory_to_cuda(inventory, runtime)
        indices = {item["uuid"]: item["index"] for item in mapped}
        self.assertEqual(indices, {f"GPU-{a}": 1, f"GPU-{b}": 0})
        self.assertTrue(all(item["runtime"] == "pytorch_cuda" for item in mapped))

    def test_ambiguous_cuda_device_names_are_not_marked_schedulable(self):
        inventory = [
            self.gpu(0, uuid="GPU-physical-a", name="NVIDIA Same Model"),
            self.gpu(1, uuid="GPU-physical-b", name="NVIDIA Same Model"),
        ]
        runtime = {
            "runtime": "pytorch_cuda",
            "runtime_version": "2.6.0",
            "devices": [
                {"index": 0, "uuid": None, "name": "NVIDIA Same Model"},
                {"index": 1, "uuid": None, "name": "NVIDIA Same Model"},
            ],
        }
        mapped = map_gpu_inventory_to_cuda(inventory, runtime)
        self.assertTrue(all(item["runtime"] == "unknown" for item in mapped))

    def test_unmapped_physical_index_cannot_collide_with_mapped_cuda_index(self):
        inventory = [
            self.gpu(0, uuid="GPU-physical-a", name="NVIDIA A"),
            self.gpu(1, uuid="GPU-physical-b", name="NVIDIA B"),
        ]
        runtime = {
            "runtime": "pytorch_cuda",
            "runtime_version": "2.6.0",
            "devices": [
                {"index": 0, "uuid": "GPU-physical-b", "name": "NVIDIA B"},
            ],
        }
        mapped = map_gpu_inventory_to_cuda(inventory, runtime)
        self.assertEqual(len(mapped), 1)
        self.assertEqual(mapped[0]["uuid"], "GPU-physical-b")
        self.assertEqual(mapped[0]["index"], 0)
        self.assertEqual(mapped[0]["runtime"], "pytorch_cuda")

    def test_single_cuda_device_can_use_unambiguous_name_fallback(self):
        inventory = [self.gpu(3, uuid="GPU-physical-a", name="NVIDIA Single")]
        runtime = {
            "runtime": "pytorch_cuda",
            "runtime_version": "2.6.0",
            "devices": [{"index": 0, "uuid": None, "name": "NVIDIA Single"}],
        }
        mapped = map_gpu_inventory_to_cuda(inventory, runtime)
        self.assertEqual(mapped[0]["index"], 0)
        self.assertEqual(mapped[0]["runtime"], "pytorch_cuda")


if __name__ == "__main__":
    unittest.main()
