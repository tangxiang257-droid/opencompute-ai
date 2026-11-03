"""v0.3 offline API, scheduling, security and lease-state tests."""
import concurrent.futures
import hashlib
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from cluster_v03.api import create_router
from cluster_v03.store import ClusterStore, LeaseConflict
from cluster_v03.worker import execute_task

ADMIN = "unit-test-admin-key-0123456789"
JOIN = "unit-test-join-key-0123456789"


class FakeClock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class ClusterTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.clock = FakeClock()
        self.store = ClusterStore(Path(self.dir.name) / "cluster.sqlite",
                                  clock=self.clock, heartbeat_ttl=10, lease_seconds=6)
        app = FastAPI()
        app.include_router(create_router(self.store, admin_key=ADMIN, join_key=JOIN))
        self.api = TestClient(app)

    def tearDown(self):
        self.api.close()
        self.dir.cleanup()

    def node(self, name, score=100, cores=4, memory=4096, caps=None, slots=1):
        body = {"name": name, "platform": "simulated", "cpu_cores": cores,
                "ram_mb": memory, "compute_score": score, "slots": slots,
                "capabilities": caps or ["sum", "sha256", "matmul_checksum"]}
        result = self.api.post("/v0.3/nodes/register", json=body,
                               headers={"X-Join-Key": JOIN})
        self.assertEqual(result.status_code, 200, result.text)
        return result.json()

    def job(self, kind="sum", payload=None, cpu=1, mem=64, max_attempts=3):
        return self.api.post("/v0.3/tasks", json={"kind": kind,
            "payload": payload or {"numbers": [1, 2, 3]},
            "required_cpu": cpu, "required_memory_mb": mem,
            "max_attempts": max_attempts}, headers={"X-Admin-Key": ADMIN})

    def claim(self, node):
        return self.api.post(f"/v0.3/nodes/{node['id']}/claim",
                             headers={"X-Node-Token": node["node_token"]})

    def heartbeat(self, node, free_mem=2048, load=0):
        return self.api.post(f"/v0.3/nodes/{node['id']}/heartbeat",
                             json={"free_ram_mb": free_mem, "cpu_load_pct": load},
                             headers={"X-Node-Token": node["node_token"]})

    def test_auth_separation_and_status_redaction(self):
        self.assertEqual(self.api.get("/v0.3/status").status_code, 401)
        self.assertEqual(self.api.post("/v0.3/nodes/register", json={
            "name": "bad", "cpu_cores": 1, "ram_mb": 128,
            "compute_score": 1, "capabilities": ["sum"]}).status_code, 401)
        node = self.node("test")
        self.assertEqual(self.heartbeat(node).status_code, 200)
        self.assertEqual(self.api.post(f"/v0.3/nodes/{node['id']}/heartbeat",
            json={"free_ram_mb": 100, "cpu_load_pct": 0},
            headers={"X-Node-Token": "bad"}).status_code, 401)
        status = self.api.get("/v0.3/status", headers={"X-Admin-Key": ADMIN}).json()
        self.assertEqual(len(status["nodes"]), 1)
        self.assertNotIn("node_token", str(status))
        self.assertNotIn("token_hash", str(status))

    def test_fastest_wins_until_saturated_then_uses_slower(self):
        slow = self.node("laptop", score=20, cores=2, memory=1024)
        fast = self.node("desktop", score=200, cores=8, memory=8192)
        self.heartbeat(slow, free_mem=900)
        self.heartbeat(fast, free_mem=7000)
        self.assertEqual(self.job().status_code, 200)
        self.assertIsNone(self.claim(slow).json()["task"])
        first = self.claim(fast).json()["task"]
        self.assertIsNotNone(first)
        self.assertIsNone(self.claim(fast).json()["task"])
        second_id = self.job().json()["id"]
        second = self.claim(slow).json()["task"]
        self.assertEqual(second["id"], second_id)
        done = self.api.post(f"/v0.3/tasks/{first['id']}/complete", json={
            "node_id": fast["id"], "lease_token": first["lease_token"],
            "result": {"value": 6}}, headers={"X-Node-Token": fast["node_token"]})
        self.assertEqual(done.status_code, 200)
        status = self.api.get("/v0.3/status", headers={"X-Admin-Key": ADMIN}).json()
        self.assertEqual(status["counts"], {"queued": 0, "leased": 1,
                                             "completed": 1, "failed": 0})
        self.assertEqual(next(x for x in status["tasks"] if x["id"] == first["id"])["assigned_node"], fast["id"])
        self.assertEqual(self.api.post(f"/v0.3/tasks/{first['id']}/complete", json={
            "node_id": fast["id"], "lease_token": first["lease_token"],
            "result": {"value": 6}}, headers={"X-Node-Token": fast["node_token"]}).status_code, 409)

    def test_resource_and_capability_matching(self):
        small = self.node("small", score=500, cores=2, memory=256, caps=["sum"])
        big = self.node("big", score=10, cores=8, memory=8192, caps=["matmul_checksum"])
        self.heartbeat(small, free_mem=128)
        self.heartbeat(big, free_mem=6000)
        job = self.job(kind="matmul_checksum", payload={"size": 4}, cpu=4, mem=1000)
        self.assertEqual(job.status_code, 200)
        self.assertIsNone(self.claim(small).json()["task"])
        self.assertEqual(self.claim(big).json()["task"]["id"], job.json()["id"])

    def test_heartbeat_ttl_and_reclaim(self):
        a = self.node("a")
        b = self.node("b", score=50)
        self.heartbeat(a)
        self.heartbeat(b)
        task_id = self.job().json()["id"]
        first = self.claim(a).json()["task"]
        self.clock.advance(11)
        self.heartbeat(b)
        reclaimed = self.claim(b).json()["task"]
        self.assertEqual(reclaimed["id"], task_id)
        self.assertNotEqual(reclaimed["lease_token"], first["lease_token"])
        stale = self.api.post(f"/v0.3/tasks/{task_id}/complete", json={
            "node_id": a["id"], "lease_token": first["lease_token"],
            "result": {}}, headers={"X-Node-Token": a["node_token"]})
        self.assertEqual(stale.status_code, 409)
        self.clock.advance(5)
        offline = self.claim(a)
        self.assertEqual(offline.status_code, 409)

    def test_failed_worker_result_retries_then_terminal(self):
        node = self.node("x")
        self.heartbeat(node)
        id = self.job(max_attempts=2).json()["id"]
        for index in range(2):
            lease = self.claim(node).json()["task"]
            self.assertEqual(lease["id"], id)
            r = self.api.post(f"/v0.3/tasks/{id}/complete", json={
                "node_id": node["id"], "lease_token": lease["lease_token"],
                "result": {}, "error": "worker error"},
                headers={"X-Node-Token": node["node_token"]})
            self.assertEqual(r.json()["status"], "queued" if index == 0 else "failed")
        self.assertIsNone(self.claim(node).json()["task"])

    def test_reject_invalid_and_nonfinite_payload(self):
        for payload in ({"numbers": [True]}, {"numbers": "1,2"}):
            self.assertEqual(self.job(payload=payload).status_code, 422)
        for payload in ({"numbers": [float("nan")]}, {"numbers": [float("inf")]}):
            with self.assertRaises(ValueError):
                self.store.submit("sum", payload)
        self.assertEqual(self.job(kind="matmul_checksum", payload={"size": 1000}).status_code, 422)

    def test_atomic_claim_only_once(self):
        node = self.node("one", slots=1)
        self.heartbeat(node)
        self.job()
        def run(_):
            return self.store.claim(node["id"], node["node_token"])["task"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            jobs = list(pool.map(run, range(4)))
        self.assertEqual(sum(v is not None for v in jobs), 1)

    def test_unconfigured_cluster_does_not_accept_credentials(self):
        app = FastAPI()
        app.include_router(create_router(self.store, admin_key="", join_key=""))
        with TestClient(app) as api:
            self.assertEqual(api.get("/v0.3/status").status_code, 503)
            self.assertEqual(api.post("/v0.3/nodes/register", json={
                "name": "a", "cpu_cores": 1, "ram_mb": 128,
                "compute_score": 1, "capabilities": ["sum"]}).status_code, 503)


class WorkerTaskTests(unittest.TestCase):
    def test_allowlisted_jobs_execute(self):
        self.assertEqual(execute_task("sum", {"numbers": [1, 2, 3]}), {"value": 6})
        self.assertEqual(execute_task("sha256", {"text": "abc"})["digest"], hashlib.sha256(b"abc").hexdigest())
        self.assertEqual(execute_task("matmul_checksum", {"size": 1}), {"checksum": 0, "size": 1})
        with self.assertRaises(ValueError):
            execute_task("shell", {"code": "rm -rf /"})


if __name__ == "__main__":
    unittest.main()
