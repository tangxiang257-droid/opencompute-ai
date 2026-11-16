"""v0.3.4 local resource guard unit tests (no Gloo and no real limits changed)."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from cluster_v031 import limits
from cluster_v031.worker import execute_rank


class LimitsUnitTests(unittest.TestCase):
    def test_limit_settings_are_kernel_enforced_before_import(self):
        if os.name != "posix":
            self.skipTest("POSIX-only kernel resource limits")
        import resource
        with patch.object(resource, "getrlimit", return_value=(
                resource.RLIM_INFINITY, resource.RLIM_INFINITY)), \
             patch.object(resource, "setrlimit") as configured:
            limits.apply_kernel_limits(cpu_seconds=90, max_vmem_mib=4096)
        configured.assert_any_call(resource.RLIMIT_CPU, (90, 92))
        configured.assert_any_call(resource.RLIMIT_AS,
                                   (4096 * 1048576, 4096 * 1048576))

    def test_no_default_virtual_memory_cap_and_bad_limits_rejected(self):
        if os.name != "posix":
            self.skipTest("POSIX-only kernel resource limits")
        import resource
        with patch.object(resource, "getrlimit", return_value=(
                resource.RLIM_INFINITY, resource.RLIM_INFINITY)), \
             patch.object(resource, "setrlimit") as configured:
            limits.apply_kernel_limits(cpu_seconds=180)
        self.assertEqual(configured.call_count, 1)
        self.assertEqual(configured.call_args.args[0], resource.RLIMIT_CPU)
        with self.assertRaises(ValueError):
            limits.apply_kernel_limits(cpu_seconds=0)
        with self.assertRaises(ValueError):
            limits.apply_kernel_limits(cpu_seconds=30, max_vmem_mib=100)

    def test_fixed_module_entry_without_arbitrary_shell_or_module(self):
        argv = ["--cpu-seconds", "50", "--", "--shards", "/tmp/trusted",
                "--prompt-ids", "1,2", "--max-new-tokens", "2"]
        with patch("cluster_v031.limits.apply_kernel_limits") as setter, \
             patch("cluster_v031.limits.runpy.run_module") as called, \
             patch.object(sys, "argv", ["test-runner"]):
            limits.main(argv)
            self.assertEqual(sys.argv[0], "compute.gpt2_generate")
            self.assertEqual(sys.argv[1:], argv[3:])
        setter.assert_called_once_with(50, None)
        called.assert_called_once_with(
            "compute.gpt2_generate", run_name="__main__", alter_sys=False)

    def test_rss_measures_all_visible_children(self):
        parent = Mock()
        child = Mock()
        parent.memory_info.return_value.rss = 200 * 1048576
        child.memory_info.return_value.rss = 125 * 1048576
        parent.children.return_value = [child]
        with patch("cluster_v031.limits.psutil.Process", return_value=parent):
            self.assertEqual(limits.process_tree_rss_bytes(4321),
                             325 * 1048576)

    def test_missing_process_does_not_raise(self):
        import psutil
        with patch("cluster_v031.limits.psutil.Process",
                   side_effect=psutil.NoSuchProcess(4321)):
            self.assertIsNone(limits.process_tree_rss_bytes(4321))


class HungProcess:
    pid = 424242

    def __init__(self):
        self.stopped = False
        self.calls = 0

    def poll(self):
        return 1 if self.stopped else None

    def communicate(self, timeout=None):
        self.calls += 1
        if not self.stopped:
            raise subprocess.TimeoutExpired(cmd="synthetic-rank", timeout=timeout)
        return "", ""

    def terminate(self):
        self.stopped = True

    def kill(self):
        self.stopped = True


class WorkerResourceIntegrationUnitTests(unittest.TestCase):
    def test_rss_over_budget_kills_group_and_reports_error(self):
        if os.name != "posix":
            self.skipTest("POSIX group signal")
        child = HungProcess()

        def kill_group(pid, sig):
            self.assertEqual(pid, child.pid)
            child.stopped = True

        assignment = {
            "rank": 1, "workers": 2, "master_host": "127.0.0.1",
            "master_port": 29500, "prompt_ids": [1, 2], "max_new_tokens": 2,
        }
        with tempfile.TemporaryDirectory() as folder, \
             patch("cluster_v031.worker.subprocess.Popen", return_value=child) as spawned, \
             patch("cluster_v031.worker.process_tree_rss_bytes",
                   return_value=700 * 1048576), \
             patch("cluster_v031.worker.os.killpg", side_effect=kill_group):
            with self.assertRaisesRegex(MemoryError, "exceeded 512 MiB"):
                execute_rank(Path(folder), assignment, timeout=3,
                             rss_limit_mib=512)
            command = spawned.call_args.args[0]
        self.assertEqual(command[:3],
                         [sys.executable, "-m", "cluster_v031.limits"])
        self.assertIn("--cpu-seconds", command)
        self.assertIn("--", command)
        self.assertTrue(child.stopped)
        self.assertGreaterEqual(child.calls, 2)

    def test_bad_rss_budget_rejected_before_process_creation(self):
        assignment = {
            "rank": 0, "workers": 1, "master_host": "127.0.0.1",
            "master_port": 29500, "prompt_ids": [1], "max_new_tokens": 2,
        }
        with patch("cluster_v031.worker.subprocess.Popen") as spawned:
            with self.assertRaisesRegex(ValueError, "RSS reservation"):
                execute_rank(Path("/tmp"), assignment, rss_limit_mib=128)
        spawned.assert_not_called()


if __name__ == "__main__":
    unittest.main()
