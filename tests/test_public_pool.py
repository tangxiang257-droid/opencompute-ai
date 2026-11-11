import tempfile
import unittest
from pathlib import Path

from cluster_v03.store import ClusterStore
from public_pool.store import AdmissionError, PublicPool
from public_pool.worker import cpu_checksum


class PublicPoolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path.cwd())
        self.addCleanup(self.temp.cleanup)
        self.now = 1000
        self.store = ClusterStore(Path(self.temp.name) / "cluster.sqlite", clock=lambda: self.now)
        self.pool = PublicPool(self.store, signing_secret="public-pool-tests-only")

    def join(self, kind="cpu", **changes):
        challenge = self.pool.challenge(kind)
        resources = {"cores": 4, "ram_mb": 8192, "vram_mb": 4096 if kind == "cuda" else 0}
        resources.update(changes)
        admission = self.pool.join(challenge, resources, cpu_checksum(challenge["seed"]))
        return challenge, admission

    def test_automatically_admits_qualified_worker_without_trusted_identity(self):
        challenge, admission = self.join()
        self.assertEqual(admission["node_id"], challenge["seed"])
        self.assertEqual(self.pool.status()["cpu_workers"], 1)
        self.assertEqual(self.store.status()["nodes"], [])
        with self.assertRaises(PermissionError):
            self.store.heartbeat(admission["node_id"], admission["token"],
                                 free_ram_mb=2048, cpu_load_pct=0)
        self.assertNotIn("token", self.pool.status())
        self.assertNotIn(admission["node_id"], str(self.pool.status()))

    def test_resource_and_challenge_checks_reject_invalid_admissions(self):
        challenge = self.pool.challenge("cuda")
        resources = {"cores": 4, "ram_mb": 8192, "vram_mb": 4096}
        proof = cpu_checksum(challenge["seed"])
        for claim, supplied, checksum in (
            (dict(challenge, signature="0" * 64), resources, proof),
            (challenge, dict(resources, vram_mb=1024), proof),
            (challenge, dict(resources, cores=1), proof),
            (challenge, resources, proof + 1),
        ):
            with self.assertRaises(AdmissionError):
                self.pool.join(claim, supplied, checksum)
        self.now += 61
        with self.assertRaises(AdmissionError):
            self.pool.join(challenge, resources, proof)
        self.assertEqual(self.pool.status()["gpu_workers"], 0)

    def test_replay_cannot_create_second_worker(self):
        challenge, _ = self.join("cuda")
        with self.assertRaises(AdmissionError) as refused:
            self.pool.join(challenge, {"cores": 4, "ram_mb": 8192, "vram_mb": 4096},
                           cpu_checksum(challenge["seed"]))
        self.assertEqual(refused.exception.status, 409)
        self.assertEqual(self.pool.status()["gpu_workers"], 1)

    def test_heartbeat_requires_token_and_rotates_the_work_challenge(self):
        _, worker = self.join()
        self.now += 20
        seed = worker["seed"]
        checksum = cpu_checksum(seed)
        with self.assertRaises(AdmissionError) as refused:
            self.pool.heartbeat(worker["node_id"], "x" * 43, seed, checksum)
        self.assertEqual(refused.exception.status, 401)
        with self.assertRaises(AdmissionError):
            self.pool.heartbeat(worker["node_id"], worker["token"], seed, checksum + 1)
        reply = self.pool.heartbeat(worker["node_id"], worker["token"], seed, checksum)
        self.assertNotEqual(reply["seed"], seed)
        retry = self.pool.heartbeat(worker["node_id"], worker["token"], seed, checksum)
        self.assertEqual(retry["seed"], reply["seed"])
        self.assertEqual(self.pool.status()["verified_checks"], 2)
        self.now += 91
        self.assertEqual(self.pool.status()["cpu_workers"], 0)
        with self.assertRaises(AdmissionError) as expired:
            self.pool.heartbeat(worker["node_id"], worker["token"], reply["seed"],
                                cpu_checksum(reply["seed"]))
        self.assertEqual(expired.exception.status, 401)

    def test_global_challenge_budget_recovers_after_window(self):
        for _ in range(60):
            self.pool.challenge("cpu")
        with self.assertRaises(AdmissionError) as refused:
            self.pool.challenge("cpu")
        self.assertEqual(refused.exception.status, 429)
        self.now += 60
        self.assertIn("signature", self.pool.challenge("cpu"))

    def test_worker_accepts_https_and_only_loopback_http(self):
        from public_pool.worker import validate_url

        self.assertEqual(validate_url("https://example.com/"), "https://example.com")
        self.assertEqual(validate_url("http://127.0.0.1:8000"), "http://127.0.0.1:8000")
        for url in ("http://example.com", "https://user:pass@example.com", "https://example.com/a"):
            with self.assertRaises(ValueError):
                validate_url(url)
