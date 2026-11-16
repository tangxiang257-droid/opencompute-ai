"""v0.3.5: immutable checkpoint pins, recovery lineage and child permissions.

These tests use synthetic GPT-2 files, mocked TCPStore and no network peers.
"""
from pathlib import Path
import os
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from cluster_v03.store import ClusterStore
from cluster_v031.checkpoints import (
    expected_files, inspect_bundle, inspect_file, validate_manifest,
)
from cluster_v031.coordinator import SessionCoordinator
from cluster_v031.worker import restricted_child_env, download_and_verify
from compute.gpt2_shards import export_state, synthetic_state


class PinnedSessions(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.store = ClusterStore(self.base / "cluster.sqlite")
        self.coordinator = SessionCoordinator(
            self.store, self.base / "sessions",
            checkpoint_root=self.base / "checkpoints")
        self.bound = patch.object(self.coordinator, "_open_rendezvous",
                                  return_value=SimpleNamespace(port=27501))
        self.bound.start()
        self.node = self.store.register(
            name="trusted-local", platform="linux", cpu_cores=4,
            ram_mb=4096, compute_score=100, slots=1,
            capabilities=["tp_gpt2"])
        self.store.heartbeat(self.node["id"], self.node["node_token"],
                             free_ram_mb=2048, cpu_load_pct=0)

    def tearDown(self):
        self.bound.stop()
        self.temp.cleanup()

    def create(self):
        return self.coordinator.create_synthetic(
            workers=1, hidden=16, heads=4, layers=2,
            prompt_ids=[1, 2], new_tokens=2)

    def fail_and_stop(self, sid):
        self.coordinator.cancel(sid)
        self.coordinator.stopped(self.node["id"], self.node["node_token"], sid)

    def test_persisted_pins_and_changed_shard_download_rejected(self):
        sid = self.create()["session_id"]
        original = self.coordinator._indexed_files(sid)
        self.assertEqual(set(original), set(expected_files(1)))
        with sqlite3.connect(self.store.path) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM tp_files WHERE session_id=?", (sid,)).fetchone()[0], 3)
        assignment = self.coordinator.poll(
            self.node["id"], self.node["node_token"])["assignment"]
        self.assertEqual({v["sha256"] for v in assignment["files"]},
                         {original[n]["sha256"] for n in ("manifest.json", "rank_0.pt", "rank0_io.pt")})
        shard = self.base / "sessions" / sid / "rank_0.pt"
        with shard.open("r+b") as fp:
            first = fp.read(1)
            fp.seek(0)
            fp.write(bytes([first[0] ^ 1]))
        self.assertEqual(self.coordinator._indexed_files(sid), original)
        with self.assertRaisesRegex(ValueError, "changed after session creation"):
            self.coordinator.authorize_file(
                self.node["id"], self.node["node_token"], sid, "rank_0.pt")

    def test_authenticated_download_holds_verified_inode_after_path_swap(self):
        sid = self.create()["session_id"]
        file = self.base / "sessions" / sid / "rank_0.pt"
        original = file.read_bytes()
        with self.coordinator.open_authorized_file(
                self.node["id"], self.node["node_token"], sid, "rank_0.pt") as handle:
            # This reproduces a path swap between authorization and transfer.
            # The already-open descriptor continues reading the original bytes.
            replacement = file.with_suffix(".replacement")
            replacement.write_bytes(b"X" * len(original))
            replacement.replace(file)
            self.assertEqual(handle.read(), original)
        with self.assertRaisesRegex(ValueError, "changed after session creation"):
            self.coordinator.open_authorized_file(
                self.node["id"], self.node["node_token"], sid, "rank_0.pt")

    def test_retry_checks_prior_shard_hash_and_keeps_lineage(self):
        sid = self.create()["session_id"]
        self.fail_and_stop(sid)
        shard = self.base / "sessions" / sid / "rank_0.pt"
        original = shard.read_bytes()
        shard.write_bytes(original[:-1] + bytes([original[-1] ^ 0x20]))
        with self.assertRaisesRegex(ValueError, "integrity mismatch"):
            self.coordinator.retry(sid)
        shard.write_bytes(original)
        retried = self.coordinator.retry(sid)
        new_id = retried["session_id"]
        self.assertNotEqual(new_id, sid)
        self.assertEqual(retried["retry_of"], sid)
        self.assertEqual(self.coordinator._indexed_files(new_id),
                         self.coordinator._indexed_files(sid))
        # A fresh application process loses in-memory Store and fails the live
        # session; the persistent lineage and file pins survive.
        restarted = SessionCoordinator(self.store, self.base / "sessions")
        self.assertEqual(restarted.status(new_id)["status"], "failed")
        self.assertEqual(restarted.status(new_id)["retry_of"], sid)
        self.assertEqual(restarted._indexed_files(new_id),
                         self.coordinator._indexed_files(sid))

    def test_unpinned_historical_session_is_not_retried(self):
        sid = self.create()["session_id"]
        self.fail_and_stop(sid)
        with self.store._tx() as db:
            db.execute("DELETE FROM tp_files WHERE session_id=?", (sid,))
        with self.assertRaisesRegex(ValueError, "lacks complete persisted file digests"):
            self.coordinator.retry(sid)

    def test_prune_deletes_pins_and_model_files(self):
        sid = self.create()["session_id"]
        self.fail_and_stop(sid)
        now = self.coordinator.clock()
        self.coordinator.clock = lambda: now + 62
        self.assertEqual(self.coordinator.prune(older_than_seconds=60)["removed"], 1)
        self.assertFalse((self.base / "sessions" / sid).exists())
        with sqlite3.connect(self.store.path) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM tp_files WHERE session_id=?", (sid,)).fetchone()[0], 0)

    def test_local_checkpoint_symlink_rejected(self):
        source = self.base / "checkpoints" / "bad-link"
        source.mkdir(parents=True)
        state, config = synthetic_state(hidden=16, heads=4, layers=1, vocab=64, positions=64)
        export_state(state, config, source, 1, "local-trusted")
        shard = source / "rank_0.pt"
        replacement = source / "rank_0_original.pt"
        shard.rename(replacement)
        try:
            shard.symlink_to(replacement.name)
        except OSError as exc:
            self.skipTest(f"symlink unsupported: {exc}")
        with self.assertRaisesRegex(ValueError, "unsafe"):
            self.coordinator.create_checkpoint(
                checkpoint="bad-link", prompt_ids=[1, 2], new_tokens=2)


class CheckpointValidation(unittest.TestCase):
    def test_rejects_invalid_sizes_types_and_manifest(self):
        state, config = synthetic_state(hidden=16, heads=4, layers=1)
        with tempfile.TemporaryDirectory() as root:
            folder = Path(root)
            manifest = export_state(state, config, folder, 1, "trusted")
            pinned = inspect_bundle(folder, manifest)
            self.assertEqual(set(pinned), set(expected_files(1)))
            with self.assertRaisesRegex(ValueError, "dimension: workers"):
                validate_manifest({**manifest, "workers": True})
            with self.assertRaisesRegex(ValueError, "divisible"):
                validate_manifest({**manifest, "heads": 3})
            with self.assertRaisesRegex(ValueError, "normalization epsilon"):
                validate_manifest({**manifest, "layer_norm_epsilon": float("nan")})
            with self.assertRaisesRegex(ValueError, "integrity mismatch"):
                inspect_bundle(folder, manifest, expected={
                    **pinned, "rank_0.pt": {**pinned["rank_0.pt"], "sha256": "0" * 64}
                })

    def test_old_session_schema_adds_retry_link_and_pins_table(self):
        with tempfile.TemporaryDirectory() as root:
            store = ClusterStore(Path(root) / "cluster.sqlite")
            with sqlite3.connect(store.path) as db:
                db.execute("""CREATE TABLE tp_sessions (
                    id TEXT PRIMARY KEY,state TEXT,created REAL,updated REAL,
                    deadline REAL,workers INTEGER,prompt_json TEXT,
                    new_tokens INTEGER,master_host TEXT,master_port INTEGER,
                    config_json TEXT,error TEXT)""")
            SessionCoordinator(store, Path(root) / "sessions")
            with sqlite3.connect(store.path) as db:
                columns = [row[1] for row in db.execute("PRAGMA table_info(tp_sessions)")]
                self.assertIn("retry_of", columns)
                tables = [row[0] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")]
                self.assertIn("tp_files", tables)


class WorkerCheckpointGuard(unittest.TestCase):
    def test_rejects_oversized_or_fake_digest_before_download(self):
        session = "e" * 32
        entries = [
            {"name": "manifest.json", "size": 30000, "sha256": "a" * 64},
            {"name": "rank_0.pt", "size": 16, "sha256": "b" * 64},
            {"name": "rank0_io.pt", "size": 16, "sha256": "c" * 64},
        ]
        assignment = {"session_id": session, "rank": 0,
                      "workers": 1, "files": entries}
        with tempfile.TemporaryDirectory() as root, \
             patch("cluster_v031.worker.requests.get") as download:
            with self.assertRaisesRegex(ValueError, "unsafe model shard"):
                download_and_verify("http://127.0.0.1:8000",
                                    assignment, "worker", "secret", root)
            download.assert_not_called()
        entries[0] = {"name": "manifest.json", "size": 123, "sha256": "bad"}
        with tempfile.TemporaryDirectory() as root, \
             patch("cluster_v031.worker.requests.get") as download:
            with self.assertRaisesRegex(ValueError, "unsafe model shard"):
                download_and_verify("http://127.0.0.1:8000",
                                    assignment, "worker", "secret", root)
            download.assert_not_called()


    def test_duplicate_descriptors_and_cache_directory_symlink(self):
        session = "a" * 32
        entries = [
            {"name": "manifest.json", "size": 100, "sha256": "a" * 64},
            {"name": "rank_0.pt", "size": 100, "sha256": "b" * 64},
            {"name": "rank0_io.pt", "size": 100, "sha256": "c" * 64},
        ]
        assignment = {"session_id": session, "rank": 0,
                      "workers": 1, "files": [entries[0], entries[1], entries[1]]}
        with tempfile.TemporaryDirectory() as root, \
             patch("cluster_v031.worker.requests.get") as download:
            with self.assertRaisesRegex(ValueError, "duplicate"):
                download_and_verify("http://127.0.0.1:8000",
                                    assignment, "node", "token", root)
            download.assert_not_called()
        assignment["files"] = entries
        with tempfile.TemporaryDirectory() as root, \
             patch("cluster_v031.worker.requests.get") as download:
            cache = Path(root) / "cache"
            cache.mkdir()
            outside = Path(root) / "outside"
            outside.mkdir()
            try:
                (cache / session).symlink_to(outside, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"directory symlink unavailable: {exc}")
            with self.assertRaisesRegex(ValueError, "cached model directory symlink"):
                download_and_verify("http://127.0.0.1:8000",
                                    assignment, "node", "token", cache)
            download.assert_not_called()


class ChildEnvironment(unittest.TestCase):
    def test_secrets_never_reach_inference_child(self):
        assignment = {"rank": 0, "workers": 2, "master_host": "127.0.0.1",
                      "master_port": 29500}
        with patch.dict(os.environ, {
            "OPENCOMPUTE_ADMIN_KEY": "secret-admin",
            "OPENCOMPUTE_JOIN_KEY": "secret-join",
            "AWS_SECRET_ACCESS_KEY": "secret-cloud",
            "LD_PRELOAD": "/tmp/untrusted.so",
            "PYTHONPATH": "/tmp/untrusted",
            "PATH": "/usr/local/bin:/usr/bin",
        }):
            env = restricted_child_env(assignment)
        for key in ("OPENCOMPUTE_ADMIN_KEY", "OPENCOMPUTE_JOIN_KEY",
                    "AWS_SECRET_ACCESS_KEY", "LD_PRELOAD", "PYTHONPATH"):
            self.assertNotIn(key, env)
        self.assertEqual(env["RANK"], "0")
        self.assertEqual(env["WORLD_SIZE"], "2")
        self.assertEqual(env["OPENCOMPUTE_EXTERNAL_STORE"], "1")


if __name__ == "__main__":
    unittest.main()
