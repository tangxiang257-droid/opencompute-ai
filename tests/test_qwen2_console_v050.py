"""Packaging and browser-console contract for the v0.5.0 live Qwen2 API."""
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Qwen2ConsolePackagingTests(unittest.TestCase):
    def test_default_backend_mounts_optional_queue_and_lifecycle(self):
        source = (ROOT / "backend" / "app.py").read_text(encoding="utf-8")
        self.assertIn("create_qwen2_queue_router", source)
        self.assertIn("Qwen2PipelineService.from_env()", source)
        self.assertIn("app.router.on_startup.append(qwen2_queue.start)", source)
        self.assertIn("app.router.on_shutdown.append(qwen2_queue.close)", source)

    def test_docker_image_contains_the_v050_package(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("COPY cluster_v05/ cluster_v05/", dockerfile)

    def test_model_console_exposes_authenticated_queue_actions(self):
        html = (ROOT / "cluster_v04" / "console.html").read_text(encoding="utf-8")
        self.assertIn("'/v0.5/status'", html)
        self.assertIn("'/v0.5/requests'", html)
        self.assertIn("'X-Admin-Key'", html)
        self.assertIn("'X-Client-ID'", html)
        self.assertIn('id="queueResult"', html)


if __name__ == "__main__":
    unittest.main()
