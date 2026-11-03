import json
import tempfile
import unittest
from pathlib import Path

import torch

from compute.shards import prepare, weight_rows


class ShardTests(unittest.TestCase):
    def test_shards_match_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = prepare(tmp, 16, 4)
            self.assertEqual(manifest["shard_shape"], [4, 16])
            shards = [torch.load(Path(tmp) / f"rank_{i}.pt", weights_only=True)
                      for i in range(4)]
            for i, shard in enumerate(shards):
                self.assertTrue(torch.equal(shard, weight_rows(i * 4, (i + 1) * 4, 16)))
            self.assertTrue(torch.equal(torch.cat(shards), weight_rows(0, 16, 16)))
            self.assertEqual(json.loads((Path(tmp) / "manifest.json").read_text())["workers"], 4)

    def test_invalid_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                prepare(tmp, 15, 4)


if __name__ == "__main__":
    unittest.main()
