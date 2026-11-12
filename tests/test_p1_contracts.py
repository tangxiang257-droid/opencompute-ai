import unittest
from pydantic import ValidationError
from cluster_p1.contracts import Task, Epoch, ResourceLimits, Stage, Tile

H = "a" * 64


class ContractsTests(unittest.TestCase):
    def test_strict_bounded_models(self):
        for kw in [
            {"cost_cap": float("nan")},
            {"deadline": float("inf")},
            {"total_layers": True},
            {"unknown": 1},
            {"privacy_level": "L0"},
        ]:
            args = dict(
                task_id="t", owner_id="u", model_hash=H, total_layers=4, payload_ref="x"
            )
            args.update(kw)
            with self.subTest(kw=kw), self.assertRaises(ValidationError):
                Task(**args)
        for kw in [
            {"memory_bytes": 0},
            {"max_inflight_tiles": True},
            {"cpu_fraction": float("nan")},
            {"cpu_fraction": 1.1},
        ]:
            with self.subTest(kw=kw), self.assertRaises(ValidationError):
                ResourceLimits(**kw)
        with self.assertRaises(ValidationError):
            Epoch(value=0)
        with self.assertRaises(ValidationError):
            Stage(
                session_id="s",
                stage_id=0,
                layer_start=3,
                layer_end=2,
                model_hash=H,
                layout_hash=H,
                assigned_node="n",
            )
        with self.assertRaises(ValidationError):
            Tile(
                model_hash=H,
                layout_hash=H,
                session_id="s",
                epoch=1,
                stage_id=0,
                tile_id=0,
                token_start=0,
                token_count=1,
                shape=(0, 1),
                dtype="float16",
            )
