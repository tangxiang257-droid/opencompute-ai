import json
import tempfile
import unittest
from pathlib import Path

import torch

from compute.qwen2_reference import generate_dense
from compute.qwen2_shards import synthetic_state


class Qwen2SingleDeviceTests(unittest.TestCase):
    def setUp(self):
        self.state, self.config = synthetic_state()
        self.prompt = [1, 7, 2]

    def test_explicit_cpu_device_generates_expected_tokens(self):
        actual = generate_dense(self.state, self.config, self.prompt, device="cpu")
        self.assertEqual(actual["generated_ids"], [52, 9, 21])
        self.assertEqual(actual["step_logits"].device.type, "cpu")

    @unittest.skipUnless(torch.cuda.is_available(), "requires CUDA-enabled PyTorch and a GPU")
    def test_prefill_and_cached_decode_run_on_cuda(self):
        reference = generate_dense(self.state, self.config, self.prompt)
        actual = generate_dense(self.state, self.config, self.prompt, device="cuda:0")
        self.assertEqual(actual["step_logits"].device, torch.device("cuda:0"))
        self.assertEqual(actual["generated_ids"], reference["generated_ids"])
        torch.testing.assert_close(actual["step_logits"].cpu(), reference["step_logits"],
                                   rtol=1e-4, atol=1e-5)

    def test_loads_local_safetensors_and_generates_tokens(self):
        from safetensors.torch import save_file
        from compute.qwen2_single_device import run

        with tempfile.TemporaryDirectory(dir=Path.cwd()) as folder:
            root = Path(folder)
            (root / "config.json").write_text(json.dumps(self.config), encoding="utf-8")
            save_file(self.state, str(root / "model.safetensors"))
            actual = run(root, self.prompt, 3, device="cpu")
        expected = generate_dense(self.state, self.config, self.prompt)
        self.assertEqual(actual["generated_ids"], expected["generated_ids"])
        self.assertEqual(actual["device"], "cpu")

    def test_cuda_request_fails_without_a_visible_cuda_device(self):
        if torch.cuda.is_available():
            self.skipTest("requires an environment without a CUDA device")
        from compute.qwen2_single_device import run

        with self.assertRaisesRegex(ValueError, "CUDA device"):
            run("unused", self.prompt, 3, device="cuda:0")
