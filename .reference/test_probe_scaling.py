"""Test that the pre-decision source probe rejects source drift and estimates conservatively."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probe_scaling as probe


class ResearchProbeTests(unittest.TestCase):
    ROOT = Path(__file__).resolve().parents[1]

    def test_source_method_shape_and_real_comparison_count(self):
        func = probe.checked_lookup(self.ROOT / probe.PUBLICATION)
        for n in (1, 8, 128):
            record = probe.probe(n, func)
            m = 2 * n + 1
            self.assertEqual(record["executed_lookup_comparisons"], m * (m + 1) // 2)

    def test_source_safeguard_fails_on_changed_lookup(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "module.py"
            path.write_text("class MediaPublicationPlan:\n def decision_for(self, output_id):\n  return self.decisions[output_id]\n")
            with self.assertRaises(AssertionError):
                probe.checked_lookup(path)

    def test_recorded_history_attribution_is_source_anchored(self):
        probe.check_recorded_history_expansion(self.ROOT / probe.RELEVANCE)

    def test_output_growth_models_are_separate_from_measurements(self):
        for n in (128, 1000, 10000, 100000):
            d = probe.model(n)
            self.assertEqual(d["output_decisions"], 2 * n + 1)
            self.assertEqual(d["incidence_links_if_32_selected_shared_assertions_per_member"], n * 32)
            self.assertEqual(d["incidence_links_if_n_selected_shared_assertions_per_member"], n * n)


if __name__ == "__main__":
    unittest.main()
