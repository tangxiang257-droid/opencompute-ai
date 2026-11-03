import unittest

from benchmarks.autotune import best_observed


class AutotuneTests(unittest.TestCase):
    def test_selects_lowest_measured_latency_not_smallest_group(self):
        rows = [
            {"workers": 1, "measured_median_seconds": 0.1},
            {"workers": 2, "measured_median_seconds": 0.08},
            {"workers": 4, "measured_median_seconds": 0.12},
        ]
        self.assertEqual(best_observed(rows)["workers"], 2)

    def test_ignores_unmeasured(self):
        rows = [
            {"workers": 1, "measured_median_seconds": None},
            {"workers": 2, "measured_median_seconds": 0.2},
        ]
        self.assertEqual(best_observed(rows)["workers"], 2)
        self.assertIsNone(best_observed([]))


if __name__ == "__main__":
    unittest.main()
