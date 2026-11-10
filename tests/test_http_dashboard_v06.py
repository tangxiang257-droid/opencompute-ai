import unittest
from fastapi.testclient import TestClient
from backend.app import app


class DashboardHttpTests(unittest.TestCase):
    def test_private_responses_are_not_cacheable(self):
        with TestClient(app) as api:
            for path in ("/", "/health", "/assets/dashboard.js", "/v0.3/status"):
                response = api.get(path)
                self.assertEqual(response.headers.get("cache-control"), "no-store", path)
                self.assertEqual(response.headers.get("x-content-type-options"), "nosniff", path)

    def test_serves_script_with_correct_type_and_current_health(self):
        with TestClient(app) as api:
            self.assertEqual(api.get("/health").json()["version"], "0.6.0")
            page = api.get("/")
            self.assertIn('/assets/dashboard.js', page.text)
            script = api.get("/assets/dashboard.js")
            self.assertEqual(script.status_code, 200)
            self.assertTrue(script.headers["content-type"].startswith("text/javascript"))
