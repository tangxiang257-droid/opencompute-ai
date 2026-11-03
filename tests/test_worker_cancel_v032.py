"""The TP Worker must terminate its child process when the session is revoked."""
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cluster_v031.worker import execute_rank


class FakeHungChild:
    pid = 424242

    def __init__(self):
        self.stopped = False
        self.calls = 0

    def poll(self):
        return 1 if self.stopped else None

    def communicate(self, timeout=None):
        self.calls += 1
        if not self.stopped:
            raise subprocess.TimeoutExpired(cmd="fake-gloo", timeout=timeout)
        return "", ""

    def terminate(self):
        self.stopped = True

    def kill(self):
        self.stopped = True


class WorkerCancellationTests(unittest.TestCase):
    def test_cancelled_session_terminates_hung_gloo_child(self):
        child = FakeHungChild()
        def stop_process_group(_pid, _signal):
            child.stopped = True
        with tempfile.TemporaryDirectory() as tmp, \
             patch("cluster_v031.worker.subprocess.Popen", return_value=child), \
             patch("cluster_v031.worker.os.killpg", side_effect=stop_process_group):
            with self.assertRaisesRegex(RuntimeError, "cancelled"):
                execute_rank(
                    Path(tmp),
                    {"rank": 1, "workers": 2, "master_host": "127.0.0.1",
                     "master_port": 29500, "prompt_ids": [1, 2],
                     "max_new_tokens": 2},
                    cancellation_check=lambda: True,
                    timeout=5)
        self.assertTrue(child.stopped)
        self.assertGreaterEqual(child.calls, 2)


if __name__ == "__main__":
    unittest.main()
