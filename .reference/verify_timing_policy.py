"""Checks exact guide deltas and actual prospective timing-policy functions."""
import ast
import dataclasses
import math
from pathlib import Path
import re
import subprocess
import tempfile
import types
import typing
import unittest
import os
ROOT = Path(os.environ['TIMING_REFERENCE_WORKSPACE']).resolve()
ns = {'__name__': 'prospective_policy', 'dataclass': dataclasses.dataclass, 'math': math, 'Literal': typing.Literal, 'Sequence': typing.Sequence, 'cast': typing.cast}
import sys
m = types.ModuleType('prospective_policy')
m.__dict__.update(ns)
sys.modules[m.__name__] = m
qtree = ast.parse((ROOT / 'scripts/ci_qualification.py').read_text())
for node in qtree.body:
    if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in {'ValidationWork', 'require_validation_policy'}:
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'prospective-ci_qualification.py', 'exec'), m.__dict__)
ttree = ast.parse((ROOT / 'scripts/ci_timing.py').read_text())
for node in ttree.body:
    if isinstance(node, ast.FunctionDef) and node.name == 'validation_comparison':
        exec(compile(ast.Module(body=[node], type_ignores=[]), 'prospective-ci_timing.py', 'exec'), m.__dict__)
W = m.ValidationWork

class RevisionTests(unittest.TestCase):

    def compare(self, work, elapsed=601, code=0, clean=True):
        return m.validation_comparison(work.definition(), elapsed=elapsed, exit_status=code, source_clean=clean)

    def target(self):
        return W('test', ('true',), 600, timing_policy='approved-target')

    def test_met(self):
        self.assertEqual(self.compare(self.target(), 599)['status'], 'met')

    def test_missed_is_report_only(self):
        r = self.compare(self.target())
        self.assertEqual(r['status'], 'missed')
        self.assertTrue(r['report_only'])

    def test_exact_boundary_is_miss(self):
        self.assertEqual(self.compare(self.target(), 600)['status'], 'missed')

    def test_no_target_retains_observation_without_pass(self):
        r = self.compare(W('test', ('true',), timing_policy='approved-no-target'))
        self.assertEqual((r['status'], r['reason']), ('not-compared', 'no-numerical-target-approved'))

    def test_no_target_still_rejects_invalid_measurement(self):
        with self.assertRaises(ValueError):
            self.compare(W('test', ('true',), timing_policy='approved-no-target'), -1)

    def test_unresolved_never_passes(self):
        self.assertEqual(self.compare(W('test', ('true',)))['reason'], 'timing-applicability-unresolved')

    def test_unresolved_policy_rejected_for_required_evidence(self):
        m.validation_work = lambda n: W(n, ('true',))
        with self.assertRaisesRegex(ValueError, 'approval required'):
            m.require_validation_policy(['test'])

    def test_explicit_no_target_policy_accepted_for_required_measurement(self):
        m.validation_work = lambda n: W(n, ('true',), timing_policy='approved-no-target')
        self.assertEqual(len(m.require_validation_policy(['test'])), 1)

    def test_invalid_policy_pairings_rejected(self):
        for policy, target in [('approved-target', None), ('approved-no-target', 600), ('unresolved', 600), ('unknown', None)]:
            with self.subTest(policy=policy), self.assertRaises(ValueError):
                W('test', ('true',), target, timing_policy=policy)

    def test_functional_failure_is_never_timing_success(self):
        self.assertEqual(self.compare(self.target(), 10, 7)['reason'], 'command-failed')

    def test_dirty_source_never_timing_success(self):
        self.assertEqual(self.compare(self.target(), 10, 0, False)['reason'], 'source-unverified')

    def test_silent_target_removal_fails(self):
        d = self.target().definition()
        d['target_seconds'] = None
        with self.assertRaises(ValueError):
            m.validation_comparison(d, elapsed=1, exit_status=0, source_clean=True)

    def test_scale_target_and_call_sites_exact(self):
        s = (ROOT / 'scripts/ci_qualification.py').read_text()
        self.assertEqual(s.count('target_seconds=600,'), 1)
        self.assertIn('STOVE0_SCALE_FILES=128', s)
        self.assertIn('STOVE0_SCALE_AUDIO_FRAMES=2000', s)
        for p in (ROOT / 'scripts').glob('*.py'):
            self.assertNotIn('require_runtime_targets', p.read_text(), str(p))
if __name__ == '__main__':
    unittest.main()
