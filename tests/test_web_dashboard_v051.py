"""Regression checks for the v0.5.1 browser dashboard packaging."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class WebDashboardTests(unittest.TestCase):
    def test_backend_serves_dashboard_at_root(self):
        source = (ROOT / "backend" / "app.py").read_text(encoding="utf-8")
        self.assertIn('WEB_INDEX = Path(__file__).resolve().parent.parent / "web" / "index.html"', source)
        self.assertIn('@app.get("/", include_in_schema=False)', source)
        self.assertIn('return FileResponse(WEB_INDEX, media_type="text/html")', source)

    def test_docker_image_includes_web_assets(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("COPY web/ web/", dockerfile)

    def test_default_compose_file_uses_the_current_dashboard_backend(self):
        for name in ("compose.yaml", "docker-compose.yml"):
            content = (ROOT / name).read_text(encoding="utf-8")
            self.assertIn('OPENCOMPUTE_ADMIN_KEY: "${OPENCOMPUTE_ADMIN_KEY:-}"', content)
            self.assertIn('OPENCOMPUTE_JOIN_KEY: "${OPENCOMPUTE_JOIN_KEY:-}"', content)
            self.assertIn("dockerfile: Dockerfile.worker", content)
            self.assertNotIn("python -m app.worker", content)

    def test_dashboard_exposes_real_cluster_workflows(self):
        html = (ROOT / "web" / "index.html").read_text(encoding="utf-8") + (ROOT / "web" / "dashboard.js").read_text(encoding="utf-8")
        for expected in (
            "算力总览", "设备节点", "任务队列", "模型中心", "推理实验",
            "设备接入与 API", "/v0.3/status", "/v0.3/tasks",
            "/v0.4/models", "/v0.5/status", "/v0.5/requests",
            "X-Client-ID", "sessionStorage", "python -m cluster_v03.worker",
        ):
            self.assertIn(expected, html)

    def test_dashboard_does_not_overstate_current_capabilities(self):
        html = (ROOT / "web" / "index.html").read_text(encoding="utf-8")
        self.assertIn("目录收录不代表当前都可执行", html)
        self.assertIn("不等于使用本平台设备算力", html)
        self.assertIn("GPU 模式当前执行可验证矩阵任务", html)
        self.assertIn("尚不承载 Qwen2 / DeepSeek 推理", html)
        self.assertIn("暂非硬件远程证明", html)


if __name__ == "__main__":
    unittest.main()
