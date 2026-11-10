"""Real-browser integration smoke. Requires playwright and its Chromium runtime.

Runs against a disposable database and local test credentials only.
python web/tests/browser-smoke.py
"""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[2]
ADMIN = "disposable-browser-admin-0123456789"
JOIN = "disposable-browser-join-0123456789"


def main():
    with tempfile.TemporaryDirectory() as temp, socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        origin = f"http://127.0.0.1:{port}"
        env = {**os.environ, "OPENCOMPUTE_ADMIN_KEY": ADMIN, "OPENCOMPUTE_JOIN_KEY": JOIN,
               "OPENCOMPUTE_DB_PATH": str(Path(temp) / "db.sqlite"),
               "OPENCOMPUTE_TP_ROOT": str(Path(temp) / "tp"),
               "OPENCOMPUTE_TP_CHECKPOINTS": str(Path(temp) / "checkpoints"),
               "OPENCOMPUTE_QWEN2_PP_BUNDLE": "", "OMP_NUM_THREADS": "1"}
        with open(Path(temp) / "server.log", "w+") as log:
            process = subprocess.Popen([sys.executable, "-m", "uvicorn", "backend.app:app",
                                        "--host", "127.0.0.1", "--port", str(port)],
                                       cwd=ROOT, env=env, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 30
                while True:
                    try:
                        urllib.request.urlopen(origin + "/health", timeout=1).close()
                        break
                    except OSError:
                        if time.monotonic() >= deadline or process.poll() is not None:
                            log.seek(0)
                            raise RuntimeError(log.read())
                        time.sleep(0.1)
                with sync_playwright() as playwright:
                    browser = playwright.chromium.launch()
                    screenshots = Path(os.getenv("OPENCOMPUTE_SCREENSHOT_DIR", ROOT / ".opencompute" / "screenshots"))
                    screenshots.mkdir(parents=True, exist_ok=True)
                    page = browser.new_page(viewport={"width": 1440, "height": 1000})
                    errors = []
                    page.on("pageerror", lambda error: errors.append(str(error)))
                    page.goto(origin)
                    page.locator("#adminKey").fill(ADMIN)
                    page.locator("#connectBtn").click()
                    expect(page.locator("#connectionText")).to_have_text("集群在线")
                    page.locator('[data-page="tasks"]').click()
                    page.locator("#submitTask").click()
                    row = page.locator("#taskRows tr").first
                    expect(row).to_contain_text("排队中")
                    row.get_by_role("button", name="详情", exact=True).click()
                    expect(page.locator("#taskDetail")).to_contain_text('"numbers"')
                    row.get_by_role("button", name="取消", exact=True).click()
                    expect(row).to_contain_text("已取消")
                    row.get_by_role("button", name="重试", exact=True).click()
                    expect(row).to_contain_text("排队中")
                    worker = subprocess.run([sys.executable, "-m", "cluster_v03.worker", "--url", origin,
                                             "--name", "browser-smoke-worker", "--score", "1", "--once"],
                                            cwd=ROOT, env=env, capture_output=True, text=True, timeout=20)
                    assert worker.returncode == 0, worker.stderr
                    page.locator("#refreshTasks").click()
                    expect(row).to_contain_text("已完成")
                    row.get_by_role("button", name="详情", exact=True).click()
                    expect(page.locator("#taskDetail")).to_contain_text('"value": 15')
                    page.screenshot(path=str(screenshots / "desktop-tasks.png"), full_page=True)
                    page.locator('[data-page="system"]').click()
                    expect(page.locator("#systemState")).to_have_text("CPU 节点就绪")
                    expect(page.locator("#systemChecks")).to_contain_text("读写正常")
                    page.reload()
                    expect(page.locator("#connectionText")).to_have_text("集群在线")
                    page.locator("#disconnectBtn").click()
                    expect(page.locator("#kpiNodes")).to_have_text("—")
                    page.reload()
                    expect(page.locator("#connectionText")).to_have_text("等待连接")
                    mobile = browser.new_page(viewport={"width": 390, "height": 844}, is_mobile=True)
                    mobile.add_init_script("Object.defineProperty(window, 'sessionStorage', {get() {throw new Error('blocked')}})")
                    mobile.goto(origin)
                    mobile.locator("#adminKey").fill(ADMIN)
                    mobile.locator("#connectBtn").click()
                    expect(mobile.locator("#connectionText")).to_have_text("集群在线")
                    assert mobile.evaluate("document.documentElement.scrollWidth <= innerWidth"), "mobile page overflows horizontally"
                    mobile.screenshot(path=str(screenshots / "mobile-overview.png"), full_page=True)
                    assert not errors, errors
                    browser.close()
                print(json.dumps({"passed": True, "screenshots": str(screenshots)}))
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
