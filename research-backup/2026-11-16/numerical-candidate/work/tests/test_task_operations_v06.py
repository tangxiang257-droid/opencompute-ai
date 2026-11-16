"""Task recovery through real SQLite transactions and authenticated HTTP."""
import concurrent.futures
import tempfile
import unittest
import contextlib
import io
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from cluster_v03.api import create_router
from cluster_v03.store import ClusterStore, LeaseConflict

ADMIN = "operations-admin-0123456789"


class TaskOperationsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = ClusterStore(Path(self.temp.name) / "db.sqlite")
        app = FastAPI()
        app.include_router(create_router(self.store, admin_key=ADMIN,
                                        join_key="operations-join-0123456789"))
        self.api = TestClient(app)
        self.headers = {"X-Admin-Key": ADMIN}
        self.task = self.store.submit("sum", {"numbers": [2, 3]})["id"]

    def tearDown(self):
        self.api.close()
        self.temp.cleanup()

    def url(self, suffix=""):
        return f"/v0.3/tasks/{self.task}{suffix}"

    def register(self):
        return self.store.register(name="worker", platform="test", cpu_cores=2,
                                   ram_mb=512, compute_score=1, slots=1,
                                   capabilities=["sum"])

    def test_task_details_require_admin_and_redact_leases(self):
        self.assertEqual(self.api.get(self.url()).status_code, 401)
        self.assertEqual(self.api.get(self.url(), headers={"X-Admin-Key":"wrong"}).status_code, 401)
        node = self.register()
        self.store.claim(node["id"], node["node_token"])
        response = self.api.get(self.url(), headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["payload"], {"numbers": [2, 3]})
        self.assertNotIn("lease_token", response.json())
        self.assertNotIn("node_token", response.text)

    def test_cancel_removes_task_from_claims_and_counts_it(self):
        response = self.api.post(self.url("/cancel"), headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "cancelled")
        node = self.register()
        self.assertIsNone(self.store.claim(node["id"], node["node_token"])["task"])
        self.assertEqual(self.store.status()["counts"]["cancelled"], 1)

    def test_cancelled_lease_cannot_complete_even_after_retry(self):
        node = self.register()
        old = self.store.claim(node["id"], node["node_token"])["task"]
        self.assertEqual(self.api.post(self.url("/cancel"), headers=self.headers).status_code, 200)
        self.assertEqual(self.api.post(self.url("/retry"), headers=self.headers).status_code, 200)
        new = self.store.claim(node["id"], node["node_token"])["task"]
        with self.assertRaises(LeaseConflict):
            self.store.complete(node["id"], node["node_token"], self.task,
                                old["lease_token"], {"sum": 500})
        self.store.complete(node["id"], node["node_token"], self.task,
                            new["lease_token"], {"sum": 5})
        detail = self.api.get(self.url(), headers=self.headers).json()
        self.assertEqual(detail["result"], {"sum": 5})
        self.assertEqual(detail["attempts"], 1)

    def test_terminal_success_cannot_cancel_or_retry(self):
        node = self.register()
        job = self.store.claim(node["id"], node["node_token"])["task"]
        self.store.complete(node["id"], node["node_token"], self.task,
                            job["lease_token"], {"sum": 5})
        for action in ("cancel", "retry"):
            self.assertEqual(self.api.post(self.url("/"+action), headers=self.headers).status_code, 409)

    def test_failed_task_can_retry_but_queued_task_cannot(self):
        self.assertEqual(self.api.post(self.url("/retry"), headers=self.headers).status_code, 409)
        self.task = self.store.submit("sum", {"numbers": [2]}, max_attempts=1)["id"]
        node = self.register()
        # Older queued work is completed first.
        first = self.store.claim(node["id"], node["node_token"])["task"]
        self.store.complete(node["id"], node["node_token"], first["id"], first["lease_token"], {})
        job = self.store.claim(node["id"], node["node_token"])["task"]
        self.store.complete(node["id"], node["node_token"], self.task, job["lease_token"], {}, error="test failure")
        response = self.api.post(self.url("/retry"), headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["attempts"], 0)
        self.assertIsNone(response.json()["error"])

    def test_unknown_ids_and_authentication_on_mutations(self):
        for action in ("cancel", "retry"):
            self.assertEqual(self.api.post(self.url("/"+action)).status_code, 401)
            self.assertEqual(self.api.post("/v0.3/tasks/absent/"+action, headers=self.headers).status_code, 404)
        self.assertEqual(self.api.get("/v0.3/tasks/absent", headers=self.headers).status_code, 404)

    def test_concurrent_retry_has_one_winner(self):
        self.store.cancel(self.task)
        def retry(_):
            try:
                self.store.retry(self.task)
                return "queued"
            except LeaseConflict:
                return "conflict"
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertCountEqual(list(pool.map(retry, range(2))), ["queued", "conflict"])

    def test_worker_discards_completion_cancelled_during_execution(self):
        from cluster_v03.worker import run, execute_task
        def execute(kind, payload):
            self.store.cancel(self.task)
            return execute_task(kind, payload)
        output = io.StringIO()
        with patch("cluster_v03.worker.requests.Session", return_value=self.api), \
                patch("cluster_v03.worker.os.cpu_count", return_value=2), \
                patch("cluster_v03.worker.execute_task", side_effect=execute), \
                contextlib.redirect_stdout(output):
            run(base_url="http://testserver", join_key="operations-join-0123456789",
                name="worker", score=1, once=True)
        self.assertIn("discarded", output.getvalue())
        self.assertEqual(self.store.task_detail(self.task)["status"], "cancelled")


if __name__ == "__main__":
    unittest.main()
