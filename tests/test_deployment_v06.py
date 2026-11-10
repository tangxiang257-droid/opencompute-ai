import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("deployment_check", Path(__file__).resolve().parents[1] / "scripts" / "check-deployment.py")
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class DeploymentChecks(unittest.TestCase):
    def test_dns_unknown_is_not_reported_missing(self):
        with patch.object(checker, "command", return_value=None):
            self.assertEqual(checker.public_dns("node.example"), "unknown")

    def test_two_nxdomain_responses_establish_missing_public_record(self):
        with patch.object(checker, "command", return_value=";; ->>HEADER<<- status: NXDOMAIN, id: 123"):
            self.assertEqual(checker.public_dns("node.example"), "missing")

    def test_one_address_family_is_enough(self):
        with patch.object(checker, "command", side_effect=["status: NOERROR ANSWER: 1", None]):
            self.assertEqual(checker.public_dns("node.example"), "present")

    def test_diagnosis_preserves_unverified_public_access(self):
        self.assertEqual(checker.conclusion({"ok": True}, None, None)[0], "local_ok_public_not_checked")
        self.assertEqual(checker.conclusion({"ok": True}, {"ok": False}, "missing"), ("local_ok_public_dns_missing", 3))
        self.assertEqual(checker.conclusion({"ok": False}, None, None)[1], 2)

    def test_rejects_insecure_urls_and_embedded_secrets(self):
        for value in ("http://example.com", "https://key@example.com", "https://example.com/?key=secret"):
            with self.assertRaises(ValueError):
                checker.validate_public_url(value)
        self.assertEqual(checker.validate_public_url("https://example.com/"), "https://example.com")
