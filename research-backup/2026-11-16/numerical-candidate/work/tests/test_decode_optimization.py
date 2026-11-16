"""Regression checks for communication-elision and the capacity planner."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import torch
from compute.gpt2_shards import export_state, synthetic_state
from compute.topology_planner import plan


class TopologyTests(unittest.TestCase):
    def setUp(self):
        self.config = {"hidden": 256, "heads": 4, "layers": 4,
                       "positions": 128, "inner": 1024, "vocab": 128}

    def test_use_one_rank_if_capacity_permits(self):
        result = plan(self.config, devices=4, memory_mib=64,
                      runtime_reserve_mib=1, prompt_tokens=4, new_tokens=4)
        self.assertEqual(result["selected_workers"], 1)
        self.assertEqual(result["candidates"][0]["network_collectives_per_generation"], 0)

    def test_minimum_memory_feasible_group(self):
        result = plan(self.config, devices=4, memory_mib=9,
                      runtime_reserve_mib=1, prompt_tokens=4, new_tokens=4)
        self.assertEqual(result["selected_workers"], 2)
        two = result["candidates"][1]
        self.assertEqual(two["network_collectives_per_generation"], 4 * (1 + 2 * 4))
        self.assertEqual(result["candidates"][2]["feasible"], False)
        self.assertGreater(two["modeled_added_network_wait_ms"], 0)

    def test_insufficient_io_memory_and_invalid_input(self):
        config = {**self.config, "vocab": 500000}
        result = plan(config, devices=4, memory_mib=10,
                      runtime_reserve_mib=0, prompt_tokens=4, new_tokens=4)
        self.assertIsNone(result["selected_workers"])
        with self.assertRaises(ValueError):
            plan(self.config, bandwidth_mbps=0)
        with self.assertRaises(ValueError):
            plan(self.config, prompt_tokens=128, new_tokens=2)

    def test_formula_matches_exported_rank_parameters(self):
        state, config = synthetic_state(hidden=16, heads=4, layers=2,
                                        vocab=32, positions=32)
        config.update({"hidden": 16, "heads": 4, "layers": 2,
                       "positions": 32, "inner": 64, "vocab": 32})
        with tempfile.TemporaryDirectory() as folder:
            export_state(state, config=state_config(config), output=folder, workers=2)
            result = plan(config, devices=2, memory_mib=32,
                          runtime_reserve_mib=0, prompt_tokens=3, new_tokens=2)
            two = result["candidates"][1]
            for rank in (0, 1):
                layers = torch.load(Path(folder) / f"rank_{rank}.pt", weights_only=True)
                weight_bytes = sum(t.numel() * t.element_size()
                                   for layer in layers for t in layer.values())
                if rank == 0:
                    io = torch.load(Path(folder) / "rank0_io.pt", weights_only=True)
                    weight_bytes += sum(t.numel() * t.element_size() for t in io.values())
                self.assertEqual(weight_bytes, two["rank0_weights_bytes"] if rank == 0
                                 else two["peer_weights_bytes"])


def state_config(config):
    return {"n_embd": config["hidden"], "n_head": config["heads"],
            "n_layer": config["layers"], "n_positions": config["positions"],
            "n_inner": config["inner"], "vocab_size": config["vocab"],
            "activation_function": "gelu_new"}


class OptimizedDecodeTests(unittest.TestCase):
    def test_same_logits_with_one_fewer_broadcast_per_token(self):
        torch.set_num_threads(1)
        state, config = synthetic_state(hidden=16, heads=4, layers=1, vocab=32)
        with tempfile.TemporaryDirectory() as folder:
            export_state(state, config, folder, 2)
            reports = []
            for legacy in (False, True):
                command = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                           "--nproc-per-node=2", "-m", "compute.gpt2_generate",
                           "--shards", folder, "--prompt-ids", "1,2,3",
                           "--max-new-tokens", "3", "--report-logits"]
                if legacy:
                    command.append("--legacy-token-broadcast")
                completed = subprocess.run(command, capture_output=True, text=True,
                                           timeout=120, env={**os.environ, "OMP_NUM_THREADS": "1"})
                self.assertEqual(completed.returncode, 0, completed.stderr)
                records = [json.loads(line) for line in completed.stdout.splitlines()
                           if line.startswith('{"engine":')]
                self.assertEqual(len(records), 1)
                reports.append(records[0])
            current, old = reports
            self.assertEqual(current["generated_ids"], old["generated_ids"])
            torch.testing.assert_close(torch.tensor(current["step_logits"]),
                                       torch.tensor(old["step_logits"]), atol=0, rtol=0)
            self.assertEqual(current["network_profile"]["calls"]["broadcast"], 3)
            self.assertEqual(old["network_profile"]["calls"]["broadcast"], 6)
            self.assertEqual(current["network_profile"]["calls"]["all_reduce"], 6)
            self.assertEqual(current["generation_sync"], "hidden_only")


if __name__ == "__main__":
    unittest.main()
