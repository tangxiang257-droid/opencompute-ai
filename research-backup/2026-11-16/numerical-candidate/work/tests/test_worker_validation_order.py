"""Untrusted download assignments must be rejected without model dependencies."""
import builtins
import hashlib
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

from cluster_v031.worker import download_and_verify


class DownloadValidationTests(unittest.TestCase):
    def assignment(self, engine="gpt2-tp"):
        return {"session_id": "a" * 32, "rank": 0, "workers": 2,
                "engine": engine, "files": [
                    {"name": name, "size": 1,
                     "sha256": hashlib.sha256(b"x").hexdigest()}
                    for name in ("manifest.json", "rank_0.pt", "rank0_io.pt")]}

    def test_invalid_assignments_rejected_before_import_network_or_files(self):
        real_import = builtins.__import__
        cases = [({"session_id": value}, "session identifier")
                 for value in ("../../outside", "", None, 123, "A" * 32)]
        cases += [({"rank": value}, "rank assignment")
                  for value in (-1, 2, True, "0")]
        cases += [({"workers": value}, "rank assignment")
                  for value in (0, 5, True, "2")]
        cases += [({"engine": "unknown"}, "engine"),
                  ({"files": []}, "manifest files"),
                  ({"files": [{"name": "../outside"}, {"name": "manifest.json"}]},
                   "manifest files")]
        bad_digest = self.assignment()["files"]
        bad_digest[0]["sha256"] = "invalid"
        cases.append(({"files": bad_digest}, "digest"))
        for engine in ("gpt2-tp", "qwen2-tp"):
            for changes, message in cases:
                with self.subTest(engine=engine, changes=changes):
                    assignment = {**self.assignment(engine), **changes}
                    attempted_imports = []
                    def guarded_import(name, *args, **kwargs):
                        if name.startswith(("torch", "compute.gpt2_generate",
                                            "compute.qwen2_generate",
                                            "compute.qwen2_chunked")):
                            attempted_imports.append(name)
                            raise AssertionError("model import before validation: " + name)
                        return real_import(name, *args, **kwargs)
                    with tempfile.TemporaryDirectory() as root:
                        with patch("builtins.__import__", side_effect=guarded_import), \
                             patch("requests.sessions.Session.request",
                                   side_effect=AssertionError("unexpected network")) as network:
                            with self.assertRaisesRegex(ValueError, message):
                                download_and_verify("http://127.0.0.1:8000", assignment,
                                                    "node", "token", root)
                        self.assertEqual(attempted_imports, [])
                        network.assert_not_called()
                        self.assertEqual(list(Path(root).iterdir()), [])

    def test_valid_legacy_assignments_keep_download_and_validation_flow(self):
        for engine, module in (("gpt2-tp", "compute.gpt2_generate"),
                               ("qwen2-tp", "compute.qwen2_generate")):
            with self.subTest(engine=engine), tempfile.TemporaryDirectory() as root:
                assignment = self.assignment(engine)
                config = {"format": "legacy-test-format"}
                model = types.ModuleType(module)
                model.read_manifest = Mock(return_value=config)
                model.load_rank = Mock(return_value=([], None))
                responses = []
                def response(*args, **kwargs):
                    result = Mock()
                    result.iter_content.return_value = [b"x"]
                    responses.append(result)
                    return result
                with patch.dict("sys.modules", {module: model}), \
                     patch("cluster_v031.worker.requests.get", side_effect=response) as get, \
                     patch("cluster_v031.worker.validate_manifest") as validate:
                    folder = download_and_verify("http://127.0.0.1:8000", assignment,
                                                 "node", "token", root)
                self.assertEqual(folder, Path(root) / assignment["session_id"])
                self.assertEqual({p.name for p in folder.iterdir()},
                                 {f["name"] for f in assignment["files"]})
                for file in folder.iterdir():
                    self.assertEqual(file.read_bytes(), b"x")
                self.assertEqual(get.call_count, 3)
                for result in responses:
                    result.raise_for_status.assert_called_once_with()
                    result.close.assert_called_once_with()
                model.read_manifest.assert_called_once_with(folder)
                validate.assert_called_once_with(config, expected_workers=2)
                model.load_rank.assert_called_once_with(folder, 0, config)


if __name__ == "__main__":
    unittest.main()
