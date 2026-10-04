"""Documentation impact witnesses. Meaning scopes and artifacts are synthetic source inputs."""
from __future__ import annotations

import copy
import unittest

from compiler import Invalid, compile_corpus, digest, report_bytes, target
from documentation_audit import (audited_candidate_evidence, build_audit, check_record,
    render_panel, snapshot, terminal_summary, verify_audited_candidate)
from test_candidate import prepared
from test_compiler import corpus, edited, ledger, parameter, root, body_edit


def captured(*, files=None, requirements=None):
    original, identity, artifacts, observed = prepared()
    req = ledger() if requirements is None else requirements
    compiled = original if files is None and requirements is None else compile_corpus(
        corpus() if files is None else files, req)
    semantics = {target(row['target']): {'owned': {'type': 'integer', 'default': 3},
        'dependencies': {'recovery-policy': {'reuse_existing': False}},
        'scope': 'owned member plus applicable shared recovery policy'} for row in req['subjects']}
    value = snapshot(compiled, req, identity=identity, semantics=semantics,
        global_semantics={'ordering': 'unspecified'}, profile='synthetic-owned-scope/v1',
        artifacts=artifacts, expected=observed, observed=observed,
        destinations={name: [root(), parameter()] for name in observed})
    return compiled, identity, artifacts, observed, value


def audit(current, previous=None, stage='prepared'):
    previous = copy.deepcopy(current) if previous is None else previous
    return build_audit(current, stage=stage, baseline=previous,
                       baseline_sha256=digest(report_bytes(previous)))


def codes(record):
    return {row['code'] for row in record['findings']}


class DocumentationAuditTests(unittest.TestCase):
    def test_unchanged_pass_does_not_approve(self):
        *_, current = captured(); record = audit(current)
        self.assertEqual(record['state'], 'PASS'); self.assertEqual(check_record(record, prepared=True), 0)
        self.assertNotIn('approved', record)
        self.assertTrue(all(d['change'] == 'unchanged' for d in record['deltas']))

    def test_meaning_changed_under_same_id_with_unchanged_prose(self):
        *_, previous = captured(); current = copy.deepcopy(previous)
        current['subjects'][target(parameter())]['meaning']['owned']['default'] = 7
        record = audit(current, previous)
        self.assertIn('meaning-only', codes(record)); self.assertEqual(record['state'], 'REVIEW')
        self.assertEqual(check_record(record, prepared=True), 0)

    def test_shared_dependency_change_is_not_hidden_by_unchanged_leaf(self):
        *_, previous = captured(); current = copy.deepcopy(previous)
        for row in current['subjects'].values():
            row['meaning']['dependencies']['recovery-policy']['reuse_existing'] = True
        record = audit(current, previous)
        self.assertEqual(sum(d['change'] == 'meaning-only' for d in record['deltas']), 3)

    def test_global_change_remains_visible_when_all_local_subjects_unchanged(self):
        *_, previous = captured(); current = copy.deepcopy(previous)
        current['global_semantics']['ordering'] = 'snapshot-consistent'
        record = audit(current, previous)
        self.assertIn('global-meaning-changed', codes(record)); self.assertEqual(record['state'], 'REVIEW')

    def test_prose_only_and_coordinated_update_have_distinct_labels(self):
        *_, previous = captured()
        files = edited(corpus(), lambda meta: meta['subjects'][0].update(summary='Updated recovery explanation.'))
        *_, current = captured(files=files)
        self.assertIn('prose-only', codes(audit(current, previous)))
        current['subjects'][target(root())]['meaning']['owned']['default'] = 4
        self.assertIn('meaning-and-prose', codes(audit(current, previous)))

    def test_changed_guide_marks_its_explicit_subjects(self):
        *_, previous = captured()
        files = body_edit(corpus(), '# Steps\n\nUse the new recovery procedure.\n')
        *_, current = captured(files=files)
        changed = [d['subject'] for d in audit(current, previous)['deltas'] if d['change'] == 'prose-only']
        self.assertEqual(changed, [target(root())])

    def test_relaxed_requirement_and_policy_changes_are_visible(self):
        *_, previous = captured(); current = copy.deepcopy(previous)
        current['subjects'][target(root())]['requirement'].update(rule='structural', detail=False)
        current['policy_sha256'] = '9'*64
        record = audit(current, previous)
        self.assertTrue({'requirement-policy-changed', 'requirement-changed'} <= codes(record))
        self.assertEqual(record['state'], 'REVIEW')

    def test_canonical_retargeting_is_requirement_drift(self):
        *_, previous = captured(); current = copy.deepcopy(previous)
        current['subjects'][target(parameter())]['requirement'].update(rule='reference', canonical=root())
        self.assertIn('requirement-changed', codes(audit(current, previous)))

    def test_new_missing_member_cannot_be_hidden_by_parent_coverage(self):
        *_, previous = captured(); current = copy.deepcopy(previous)
        row = copy.deepcopy(current['subjects'][target(parameter())]); row.update(target=parameter('limit'), covered=False)
        row['requirement']['target'] = row['target']; current['subjects'][target(row['target'])] = row
        record = audit(current, previous)
        self.assertTrue({'subject-added', 'missing-documentation'} <= codes(record))
        self.assertEqual(check_record(record, prepared=True), 2)

    def test_removed_requirement_stays_in_full_record(self):
        *_, previous = captured(); current = copy.deepcopy(previous)
        del current['subjects'][target(root('count-type'))]
        record = audit(current, previous)
        self.assertIn('subject-removed', codes(record))
        self.assertIsNotNone(next(d for d in record['deltas'] if d['change'] == 'removed')['before'])

    def test_initial_candidate_is_full_review_not_no_changes(self):
        *_, current = captured()
        record = build_audit(current, stage='prepared', initial=True)
        self.assertEqual(record['comparison'], 'initial'); self.assertEqual(record['state'], 'REVIEW')
        self.assertTrue(all(d['change'] == 'initial' for d in record['deltas']))

    def test_missing_or_wrong_requested_baseline_refuses(self):
        *_, current = captured()
        with self.assertRaisesRegex(Invalid, 'baseline unavailable'): build_audit(current, stage='prepared')
        with self.assertRaisesRegex(Invalid, 'identity differs'):
            build_audit(current, stage='prepared', baseline=current, baseline_sha256='0'*64)

    def test_unknown_snapshot_format_is_not_empty_diff(self):
        *_, current = captured(); current['format'] = 'future/unknown'
        with self.assertRaisesRegex(Invalid, 'unsupported'): audit(current)

    def test_fingerprint_profile_change_means_unknown_not_unchanged(self):
        *_, previous = captured(); current = copy.deepcopy(previous); current['profile'] = 'different/v2'
        record = audit(current, previous)
        self.assertIn('incomparable-meaning-profile', codes(record))
        self.assertEqual({d['change'] for d in record['deltas']}, {'unknown-meaning'})

    def test_pending_native_evidence_preview_vs_prepared(self):
        *_, current = captured(); current['observed'] = {}
        preview, prepared_record = audit(current, stage='preview'), audit(current)
        self.assertEqual(preview['state'], 'REVIEW'); self.assertEqual(check_record(preview, prepared=True), 2)
        self.assertEqual(prepared_record['state'], 'FAIL'); self.assertEqual(check_record(prepared_record, prepared=True), 2)
        self.assertEqual({o['status'] for o in preview['outputs']}, {'unverified'})

    def test_native_mismatch_blocks_even_preview(self):
        *_, current = captured(); current['observed']['cli/recover.txt'] = '0'*64
        record = audit(current, stage='preview')
        self.assertEqual(record['state'], 'FAIL'); self.assertEqual(check_record(record), 2)

    def test_missing_inventory_does_not_mean_verified(self):
        *_, current = captured(); current.update(expected={}, observed={}, destinations={})
        self.assertIn('native-evidence-unavailable', codes(audit(current)))
        self.assertEqual(audit(current)['state'], 'FAIL')

    def test_expected_destinations_change_is_review_attention(self):
        *_, previous = captured(); current = copy.deepcopy(previous)
        current['expected']['cli/recover.txt'] = current['observed']['cli/recover.txt'] = '9'*64
        self.assertIn('destination-projection-changed:cli/recover.txt', codes(audit(current, previous)))

    def test_removed_destination_does_not_silently_reduce_coverage(self):
        *_, previous = captured(); current = copy.deepcopy(previous)
        for name in ("expected", "observed", "destinations"):
            del current[name]["cli/recover.txt"]
        self.assertIn("destination-removed:cli/recover.txt", codes(audit(current, previous)))

    def test_semantic_scope_must_cover_every_source_requirement(self):
        compiled, identity, artifacts, observed, _ = captured()
        with self.assertRaisesRegex(Invalid, 'complete semantic scopes'):
            snapshot(compiled, ledger(), identity=identity, semantics={}, global_semantics={}, profile='v1',
                     artifacts=artifacts, expected=observed, observed=observed, destinations={})

    def test_corpus_formatting_changes_identity_not_effective_prose(self):
        *_, previous = captured(); files = corpus()
        name = 'reference/recover.md'; files[name] = files[name].replace(b'kind: reference', b'kind: "reference"')
        *_, current = captured(files=files)
        record = audit(current, previous)
        self.assertNotEqual(current['inputs']['corpus_sha256'], previous['inputs']['corpus_sha256'])
        self.assertTrue(all(d['change'] == 'unchanged' for d in record['deltas']))

    def test_terminal_and_html_share_complete_finding_ids(self):
        *_, current = captured(); current['observed'] = {}; record = audit(current)
        summary = terminal_summary(record, limit=1); panel = render_panel(record)
        self.assertIn('more findings', summary)
        for f in record['findings']:
            self.assertIn('doc-audit-'+f['id'], panel)
        self.assertEqual(check_record(record), 2)

    def test_context_panel_preserves_global_attention_and_destination_state(self):
        *_, previous = captured(); current = copy.deepcopy(previous)
        current['global_semantics']['ordering'] = 'new'; record = audit(current, previous)
        panel = render_panel(record, subject=target(parameter()))
        self.assertIn('global-meaning-changed', panel); self.assertIn('cli/recover.txt', panel)
        self.assertNotIn('count-type', panel)

    def test_panel_escapes_source_values(self):
        *_, current = captured()
        current['subjects'][target(root())]['meaning']['owned']['payload'] = '<script>alert(1)</script>'
        panel = render_panel(audit(current))
        self.assertNotIn('<script>', panel); self.assertIn('&lt;script&gt;', panel)

    def test_forged_pass_summary_cannot_hide_failures(self):
        *_, current = captured(); current['observed'] = {}; record = audit(current); record['state'] = 'PASS'
        with self.assertRaisesRegex(Invalid, 'suppresses'): check_record(record)

    def test_deleting_failure_rows_cannot_forge_mechanical_success(self):
        *_, current = captured(); current['observed'] = {}; record = audit(current)
        record['findings'] = []; record['state'] = 'PASS'
        with self.assertRaisesRegex(Invalid, 'mechanical blockers'): check_record(record)

    def test_candidate_binds_audit_and_panel_without_second_approval(self):
        compiled, identity, artifacts, observed, current = captured(); record = audit(current)
        result = audited_candidate_evidence(compiled, identity=identity, artifacts=artifacts, observed=observed,
            required_outputs=set(observed), audit=record)
        verify_audited_candidate(result, result, artifacts=artifacts, observed=observed,
                                audit=report_bytes(record), panel=render_panel(record).encode())
        with self.assertRaisesRegex(Invalid, 'panel changed'):
            verify_audited_candidate(result, result, artifacts=artifacts, observed=observed,
                                    audit=report_bytes(record), panel=b'different review panel')

    def test_prior_audit_cannot_be_attached_to_another_candidate(self):
        compiled, identity, artifacts, observed, current = captured(); record = audit(current)
        identity['code_sha'] = '9'*40
        with self.assertRaisesRegex(Invalid, 'another prepared candidate'):
            audited_candidate_evidence(compiled, identity=identity, artifacts=artifacts, observed=observed,
                                       required_outputs=set(observed), audit=record)

    def test_record_is_repeatable_and_does_not_modify_inputs(self):
        *_, current = captured(); original = copy.deepcopy(current)
        self.assertEqual(report_bytes(audit(current)), report_bytes(audit(current)))
        self.assertEqual(current, original)


if __name__ == '__main__': unittest.main()
