import datetime as dt
import unittest

import settlement_seed as seed


class ItemCountsTest(unittest.TestCase):
    def test_uniform_split_keeps_total(self):
        counts = seed.item_counts(1_000, 10_000, skew=False)

        self.assertEqual(sum(counts), 10_000)
        self.assertEqual(set(counts), {10})

    def test_top_one_percent_holds_seventy_percent(self):
        counts = seed.item_counts(5_000, 100_000, skew=True)

        self.assertEqual(sum(counts), 100_000)
        self.assertEqual(sum(counts[:50]), 70_000)
        self.assertGreaterEqual(min(counts), 1)

    def test_largest_seller_gets_pinned_share(self):
        counts = seed.item_counts(20_000, 270_000, skew=True, max_share=0.10)

        self.assertEqual(sum(counts), 270_000)
        self.assertEqual(max(counts), 27_000)
        self.assertEqual(sum(counts[:200]), 189_000)
        self.assertGreaterEqual(min(counts), 1)

    def test_rejects_sellers_without_items(self):
        with self.assertRaises(ValueError):
            seed.item_counts(10, 5, skew=False)


class ProductCountsTest(unittest.TestCase):
    def test_largest_seller_has_fifty_products_and_others_five(self):
        products = seed.product_counts([3, 27_000, 10])

        self.assertEqual(products, [3, 50, 5])

    def test_fixed_override_is_capped_by_items(self):
        self.assertEqual(seed.product_counts([3, 100], fixed=10), [3, 10])


class CutoffTest(unittest.TestCase):
    def test_cutoff_is_ten_kst(self):
        cutoff = seed.cutoff_of(dt.date(2026, 11, 1))

        self.assertEqual(cutoff.astimezone(dt.timezone.utc).isoformat(), "2026-11-01T01:00:00+00:00")


SCRAPE_BEFORE = """\
# HELP settlement_batch_seller_phase_seconds x
settlement_batch_seller_phase_seconds_bucket{application="settlement",phase="context",le="0.001"} 0.0
settlement_batch_seller_phase_seconds_bucket{application="settlement",phase="context",le="0.01"} 1.0
settlement_batch_seller_phase_seconds_bucket{application="settlement",phase="context",le="+Inf"} 1.0
settlement_batch_seller_phase_seconds_count{application="settlement",phase="context"} 1.0
settlement_batch_seller_phase_seconds_sum{application="settlement",phase="context"} 0.005
settlement_batch_seller_phase_seconds_count{application="settlement",phase="reserve"} 7.0
"""
SCRAPE_AFTER = """\
settlement_batch_seller_phase_seconds_bucket{application="settlement",phase="context",le="0.001"} 0.0
settlement_batch_seller_phase_seconds_bucket{application="settlement",phase="context",le="0.01"} 91.0
settlement_batch_seller_phase_seconds_bucket{application="settlement",phase="context",le="+Inf"} 101.0
settlement_batch_seller_phase_seconds_count{application="settlement",phase="context"} 101.0
settlement_batch_seller_phase_seconds_sum{application="settlement",phase="context"} 1.005
settlement_batch_seller_phase_seconds_count{application="settlement",phase="reserve"} 9.0
"""


class HistogramTest(unittest.TestCase):
    def test_delta_excludes_samples_before_the_run(self):
        before = seed.parse_prometheus(SCRAPE_BEFORE, "settlement_batch_seller")
        after = seed.parse_prometheus(SCRAPE_AFTER, "settlement_batch_seller")

        delta = seed.histogram_delta(before, after, "settlement_batch_seller_phase_seconds",
                                     lambda labels: labels.get("phase") == "context")

        self.assertEqual(delta["count"], 100)
        self.assertAlmostEqual(delta["sum"], 1.0)
        self.assertEqual(delta["buckets"], [(0.001, 0.0), (0.01, 90.0), (float("inf"), 100.0)])

    def test_quantile_returns_bucket_upper_bound(self):
        buckets = [(0.001, 0.0), (0.01, 90.0), (float("inf"), 100.0)]

        self.assertEqual(seed.quantile(buckets, 0.5), 0.01)
        self.assertEqual(seed.quantile(buckets, 0.95), float("inf"))

    def test_quantile_without_samples_is_unknown(self):
        self.assertIsNone(seed.quantile([(0.01, 0.0), (float("inf"), 0.0)], 0.5))


if __name__ == "__main__":
    unittest.main()
