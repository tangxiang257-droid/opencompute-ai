"""Static safety and packaging checks for the Windows local launcher."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


class WindowsLauncherTests(unittest.TestCase):
    def test_startup_script_generates_independent_secrets_without_overwriting_env(self):
        source = (SCRIPTS / "start-local.ps1").read_text(encoding="utf-8")
        self.assertIn("RandomNumberGenerator", source)
        self.assertIn("if (-not (Test-Path $envPath))", source)
        self.assertIn("WriteAllLines($envPath", source)
        self.assertIn("if ($admin.Length -lt 16 -or $join.Length -lt 16", source)
        self.assertIn("$admin -eq $join", source)

    def test_startup_supports_three_simulated_workers_and_health_check(self):
        source = (SCRIPTS / "start-local.ps1").read_text(encoding="utf-8")
        self.assertIn("--profile sim up --build -d --scale worker=3", source)
        self.assertIn('"$apiUrl/health"', source)
        self.assertIn('"$apiUrl/v0.3/status"', source)
        self.assertIn('X-Admin-Key', source)

    def test_launcher_never_deletes_persistent_data(self):
        stop = (SCRIPTS / "stop-local.ps1").read_text(encoding="utf-8")
        start = (SCRIPTS / "start-local.ps1").read_text(encoding="utf-8")
        self.assertIn("docker compose --profile sim down", stop)
        self.assertNotIn("down -v", stop)
        self.assertNotIn("volume rm", stop)
        self.assertNotIn("Remove-Item", start)

    def test_launcher_and_documentation_are_in_repository(self):
        self.assertTrue((SCRIPTS / "start-local.ps1").is_file())
        self.assertTrue((SCRIPTS / "status-local.ps1").is_file())
        self.assertTrue((SCRIPTS / "stop-local.ps1").is_file())
        self.assertTrue((ROOT / "docs" / "v0.6-windows-local-launcher.md").is_file())
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("Windows 本地一键启动", readme)
        self.assertIn("scripts/start-local.ps1", readme)


if __name__ == "__main__":
    unittest.main()
