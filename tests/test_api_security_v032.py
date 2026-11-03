"""Security regression tests for formerly public CPU benchmark endpoints."""
import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from backend.app import app


ADMIN = "a-different-long-local-admin-key-01234"


class BenchmarkAuthTests(unittest.TestCase):
    def setUp(self):
        self.api = TestClient(app)

    def tearDown(self):
        self.api.close()

    def test_unconfigured_admin_blocks_legacy_expensive_endpoints(self):
        with patch.dict(os.environ, {"OPENCOMPUTE_ADMIN_KEY": ""}):
            response = self.api.post("/benchmarks", json={
                "workers": 1, "hidden": 64, "batch": 1})
            self.assertEqual(response.status_code, 503)
            self.assertEqual(self.api.post("/v0.2/plan", json={}).status_code, 503)

    def test_missing_and_wrong_admin_rejected_before_compute(self):
        with patch.dict(os.environ, {"OPENCOMPUTE_ADMIN_KEY": ADMIN}):
            for endpoint in ("/benchmarks", "/v0.2/plan"):
                self.assertEqual(self.api.post(endpoint, json={}).status_code, 401)
                self.assertEqual(self.api.post(endpoint, json={},
                    headers={"X-Admin-Key": "wrong"}).status_code, 401)

    def test_correct_admin_can_use_cpu_free_plan(self):
        with patch.dict(os.environ, {"OPENCOMPUTE_ADMIN_KEY": ADMIN}):
            response = self.api.post("/v0.2/plan",
                                     headers={"X-Admin-Key": ADMIN},
                                     json={"hidden": 16, "heads": 4, "layers": 1,
                                           "positions": 64, "inner": 64, "vocab": 32,
                                           "memory_mib": 512})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertIn("selected_workers", response.json())


if __name__ == "__main__":
    unittest.main()
