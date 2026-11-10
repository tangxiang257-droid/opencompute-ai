"""Diagnostic responses describe actual state without leaking configuration."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.operations import create_router
from cluster_v03.store import ClusterStore

ADMIN = "diagnostic-admin-0123456789"


class SystemTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = ClusterStore(Path(self.temp.name) / "private.sqlite")
        app = FastAPI()
        app.include_router(create_router(self.store, None, admin_key=ADMIN))
        self.api = TestClient(app)

    def tearDown(self):
        self.api.close()
        self.temp.cleanup()

    def get(self):
        return self.api.get("/v0.6/system", headers={"X-Admin-Key": ADMIN})

    def test_requires_admin(self):
        self.assertEqual(self.api.get("/v0.6/system").status_code, 401)

    def test_reports_api_database_and_absent_workers_model_separately(self):
        response = self.get()
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["version"], "0.6.0")
        self.assertEqual(data["database"], "ok")
        self.assertEqual(data["workers"]["online"], 0)
        self.assertEqual(data["qwen2"]["reason"], "bundle_not_configured")
        self.assertEqual(data["state"], "waiting_for_workers")
        self.assertNotIn(self.temp.name, response.text)
        self.assertNotIn(ADMIN, response.text)

    def test_counts_real_nodes_and_task_states(self):
        self.store.register(name="cpu", platform="test", cpu_cores=2,
                            ram_mb=512, compute_score=1, slots=1, capabilities=["sum"])
        task = self.store.submit("sum", {"numbers": [1]})
        self.store.cancel(task["id"])
        data = self.get().json()
        self.assertEqual(data["state"], "ready")
        self.assertEqual(data["workers"]["online"], 1)
        self.assertEqual(data["workers"]["cpu_cores"], 2)
        self.assertEqual(data["tasks"]["cancelled"], 1)

    def test_database_failure_is_reported_without_exception_or_path(self):
        import sqlite3
        with patch.object(self.store, "status", side_effect=sqlite3.OperationalError("secret /private/db")):
            response = self.get()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["state"], "degraded")
        self.assertEqual(response.json()["database"], "unavailable")
        self.assertNotIn("/private/db", response.text)


if __name__ == "__main__":
    unittest.main()
