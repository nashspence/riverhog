"""Checks for evidence harness correctness; these are not repository gate tests."""
import json
from pathlib import Path
import unittest
import probe_quadratic as p

class EvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r=json.loads(Path(__file__).with_name('results.json').read_text())['cases']

    def test_pending_work_does_not_omit_rows(self):
        for name in ('history_pending_queue','snapshot_pending_queue','history_pending_queue_with_partial_index'):
            for row in self.r[name]: self.assertEqual(row['visited'],row['n'])

    def test_full_touch_index_preserves_flags(self):
        for original, fixed in zip(self.r['journal_touch_reset'], self.r['journal_touch_reset_with_full_index']):
            self.assertEqual((original['row_count'],original['touched']),(fixed['row_count'],fixed['touched']))
            self.assertLess(fixed['sqlite_vm_steps_rounded_down_100'],original['sqlite_vm_steps_rounded_down_100'])

    def test_pending_index_reduces_work(self):
        for original, fixed in zip(self.r['history_pending_queue'],self.r['history_pending_queue_with_partial_index']):
            self.assertLess(fixed['sqlite_vm_steps_rounded_down_100'],original['sqlite_vm_steps_rounded_down_100'])

    def test_sparse_edges_remain_exact(self):
        for row in self.r['incremental_reverse_chain']:
            self.assertEqual(row['edges'],row['n']);self.assertTrue(row['rejects_cycle'])

    def test_filename_pairs_are_sparse(self):
        for row in self.r['filename_pairing']:
            self.assertEqual(row['comparisons'],row['n']**2)
            self.assertEqual(row['candidate_rows'],row['n'])

    def test_request_lookup_visits_triangular_prefix(self):
        for row in self.r['observer_subject_lookup']:
            self.assertEqual(row['subject_visits'],row['n']*(row['n']+1)//2)

    def test_recovery_outputs_remain_linear(self):
        for row in self.r['recovery_journal_ranges']:
            self.assertEqual(row['descriptors'],2*row['n']*(row['n']+1))
            self.assertEqual(row['payload_reads'],row['n'])

    def test_corpus_cycle_is_rejected(self):
        self.assertTrue(p.negatives()['corpus_cycle_rejected'])

    def test_source_bytes_equal_commit(self):
        data=json.loads(Path(__file__).with_name('results.json').read_text())
        for path,identity in data['source_hashes'].items():
            current=p.subprocess.check_output(['git','hash-object',path],cwd=p.ROOT,text=True).strip()
            recorded=p.subprocess.check_output(['git','rev-parse',f'{p.SHA}:{path}'],cwd=p.ROOT,text=True).strip()
            self.assertEqual(current,identity['git_blob']);self.assertEqual(current,recorded)

if __name__=='__main__':unittest.main()
