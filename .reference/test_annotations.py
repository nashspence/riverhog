# SPDX-License-Identifier: Apache-2.0
"""Executable challenges for the small, non-production annotation model."""
from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from annotation_reference import (
    ACTOR, RECORDER, WHEN, ArchiveFixture, Conflict, Copy, Invalid,
    compose, digest, encoded, newest_compatible, parse_frame, verify_portable,
)

REPO = Path(__file__).resolve().parents[1]


class AnnotationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = ArchiveFixture.from_repository(REPO)
        self.source = Copy(self.fixture.root)
        self.source.mutate("init", "initial")
        self.initial = self.source.head

    def mutate(self, name: str, op: str, value: str | None = None, **kw):
        return self.source.mutate(name, op, value, **kw)

    def test_initial_empty_is_explicit_not_missing(self):
        self.assertEqual(self.source.recover().state,
                         {"tags": [], "description": {"status": "unset"}})
        with self.assertRaises(Invalid):
            Copy(self.fixture.root).recover()

    def test_add_remove_readd_preserves_three_distinct_acts(self):
        first = self.mutate("add", "tag_add", "reviewed")
        self.mutate("remove", "tag_remove", "reviewed")
        last = self.mutate("again", "tag_add", "reviewed")
        self.assertNotEqual(first, last)
        self.assertEqual(self.source.recover(first).state, self.source.recover(last).state)
        self.assertEqual(len(self.source.recover().frames), 4)
        self.assertTrue(all(parse_frame(f)["entry_kind"] != "correction"
                            for f in self.source.recover().frames))

    def test_description_replace_clear_retains_values(self):
        one = self.mutate("d1", "description_set", "Family — été\nFirst account")
        two = self.mutate("d2", "description_set", "Corrected account")
        self.mutate("d3", "description_clear")
        self.source.collect()
        self.assertIn("été", self.source.recover(one).state["description"]["value"])
        self.assertEqual(self.source.recover(two).state["description"]["value"], "Corrected account")
        self.assertEqual(self.source.recover().state["description"], {"status": "unset"})

    def test_noop_does_not_create_an_event(self):
        self.mutate("add", "tag_add", "x")
        old = self.source.recover()
        self.mutate("same", "tag_add", "x")
        self.assertEqual(self.source.head, old.checkpoint)
        self.assertEqual(self.source.recover().journal, old.journal)

    def test_stale_noop_does_not_bypass_precondition(self):
        self.mutate("add", "tag_add", "x")
        with self.assertRaises(Conflict):
            self.source.prepare("late", "tag_add", "x", expected=self.initial)

    def test_staging_without_selector_does_not_publish(self):
        pending = self.source.prepare("add", "tag_add", "x", expected=self.source.head)
        self.source.stage(pending)
        self.assertEqual(self.source.recover().state["tags"], [])
        self.assertNotIn(pending.checkpoint, self.source.recover().checkpoints)

    def test_partial_staging_cannot_publish(self):
        pending = self.source.prepare("add", "tag_add", "x", expected=self.source.head)
        self.source.stage(pending)
        checkpoint = json.loads(self.source.objects[pending.checkpoint])
        del self.source.objects[checkpoint["snapshot"]]
        with self.assertRaises(Invalid):
            self.source.publish(pending)
        self.assertEqual(self.source.head, self.initial)

    def test_selector_success_before_db_commit_reconciles(self):
        self.source.reconcile_catalog()
        pending = self.source.prepare("add", "tag_add", "x", expected=self.source.head)
        self.source.stage(pending)
        self.source.publish(pending)
        self.assertEqual(self.source.catalog_head, self.initial)
        restarted = copy.deepcopy(self.source)
        restarted.reconcile_catalog()
        self.assertEqual(restarted.catalog_head, pending.checkpoint)

    def test_lost_response_retry_preserves_entry_and_recording_time(self):
        pending = self.source.prepare("add", "tag_add", "x", expected=self.initial, when=WHEN)
        self.source.stage(pending)
        self.source.publish(pending)
        journal = self.source.recover().journal
        retry = self.source.prepare("add", "tag_add", "x", expected=self.initial,
                                    when="2026-10-14T14:00:00Z")
        self.source.publish(retry)
        self.assertEqual(self.source.recover().journal, journal)

    def test_retry_after_later_edit_returns_original_not_republished_head(self):
        first = self.mutate("add", "tag_add", "x")
        last = self.mutate("remove", "tag_remove", "x")
        retry = self.source.prepare("add", "tag_add", "x", expected=self.initial)
        self.assertEqual(self.source.publish(retry), first)
        self.assertEqual(self.source.head, last)

    def test_changed_input_with_same_operation_identity_conflicts(self):
        self.mutate("add", "tag_add", "x")
        with self.assertRaises(Conflict):
            self.source.prepare("add", "tag_add", "y", expected=self.initial)

    def test_concurrent_tag_and_description_cannot_split_authorities(self):
        tags = self.source.prepare("tag", "tag_add", "x", expected=self.initial)
        description = self.source.prepare("desc", "description_set", "d", expected=self.initial)
        self.source.stage(tags)
        self.source.stage(description)
        self.source.publish(tags)
        with self.assertRaises(Conflict):
            self.source.publish(description)
        self.assertEqual(self.source.recover().state["description"], {"status": "unset"})
        self.mutate("desc-rebased", "description_set", "d")
        self.assertEqual(self.source.recover().state,
                         {"tags": ["x"], "description": {"status": "present", "value": "d"}})

    def test_old_copy_is_complete_not_globally_latest(self):
        old = copy.deepcopy(self.source)
        self.mutate("new", "tag_add", "x")
        self.assertEqual(old.recover().checkpoint, self.initial)
        self.assertEqual(newest_compatible(old, self.source), self.source.head)

    def test_divergent_copies_are_not_merged_by_revision_or_clock(self):
        other = copy.deepcopy(self.source)
        self.mutate("one", "tag_add", "x", when="2026-10-15T00:00:00Z")
        other.mutate("two", "tag_add", "y", when="2026-10-13T00:00:00Z")
        other.mutate("three", "tag_add", "z")
        with self.assertRaises(Conflict):
            newest_compatible(self.source, other)

    def test_recordkeeping_time_does_not_sort_changes(self):
        self.mutate("one", "description_set", "first", when="2026-10-15T00:00:00Z")
        self.mutate("two", "description_set", "second", when="2026-10-13T00:00:00Z")
        self.assertEqual(self.source.recover().state["description"]["value"], "second")

    def test_missing_middle_of_selected_history_fails(self):
        middle = self.mutate("one", "tag_add", "x")
        self.mutate("two", "tag_remove", "x")
        del self.source.objects[middle]
        with self.assertRaises(Invalid):
            self.source.recover()

    def test_tampered_journal_object_fails(self):
        self.mutate("one", "tag_add", "x")
        cp = json.loads(self.source.read(self.source.head))
        self.source.objects[cp["entry"]] += b" "
        with self.assertRaises(Invalid):
            self.source.recover()

    def test_self_consistent_changed_snapshot_still_fails_replay(self):
        self.mutate("one", "tag_add", "x")
        cp = json.loads(self.source.read(self.source.head))
        raw = encoded({"tags": ["forged"], "description": {"status": "unset"}})
        self.source.objects[digest(raw)] = raw
        cp["snapshot"] = digest(raw)
        raw = encoded(cp)
        self.source.objects[digest(raw)] = raw
        self.source.head = digest(raw)
        with self.assertRaises(Invalid):
            self.source.recover()

    def test_gc_removes_orphans_but_not_committed_history(self):
        self.mutate("one", "tag_add", "x")
        self.mutate("two", "tag_remove", "x")
        before = self.source.recover().required.copy()
        orphan = self.source.prepare("orphan", "tag_add", "z", expected=self.source.head)
        self.source.stage(orphan)
        self.source.collect()
        self.assertEqual(set(self.source.objects), before)
        self.source.recover(self.initial)

    def test_gc_respects_explicit_inflight_owner(self):
        pending = self.source.prepare("one", "tag_add", "x", expected=self.source.head)
        self.source.stage(pending)
        self.source.collect((pending,))
        self.source.publish(pending)
        self.assertEqual(self.source.recover().state["tags"], ["x"])

    def test_collection_context_never_synthesizes_artifact_state(self):
        self.mutate("one", "tag_add", "reviewed")
        self.mutate("two", "description_set", "Collection prose")
        for raw in self.source.recover().frames:
            groups = parse_frame(raw)["body"]["assertions"]
            self.assertFalse(set(groups) & {"artifacts", "occurrences", "states", "descriptions", "journal_subjects"})
            activity = groups["activities"][0]
            self.assertEqual(activity["kind"], "metadata_update")
            self.assertNotIn("started_at", activity)
            self.assertEqual(activity["associations"][0]["agent_id"], ACTOR)
            self.assertEqual(activity["evidence"][0]["asserted_by_agent_id"], RECORDER)

    def test_portable_composition_survives_loss_of_all_source_state(self):
        self.mutate("one", "tag_add", "reviewed")
        original = self.fixture.original_journal
        bundle = compose(self.source, self.fixture, self.fixture.member_id,
                         checkpoint=self.source.head)
        self.source.objects.clear()
        self.source.head = None
        self.fixture.proof = b""
        self.fixture.original_journal = b""
        recovered = verify_portable(bundle)
        self.assertEqual(recovered.state["tags"], ["reviewed"])
        self.assertEqual(bundle["objects"][bundle["original_journal"]], original)
        bridge = parse_frame(bundle["objects"][bundle["bridge"]])["body"]["assertions"]["extensions"][0]
        self.assertEqual(bridge["subject"]["object_type"], "state")
        self.assertEqual(bridge["value"]["value"]["object_type"], "context")
        self.assertNotIn("tag", bridge)

    def test_portable_composition_is_pinned_not_following_future_source_edits(self):
        chosen = self.mutate("one", "tag_add", "x")
        bundle = compose(self.source, self.fixture, self.fixture.member_id, checkpoint=chosen)
        self.mutate("two", "tag_remove", "x")
        self.assertEqual(verify_portable(bundle).state["tags"], ["x"])

    def test_composition_idempotency_and_distinct_duplicate_byte_members(self):
        a = compose(self.source, self.fixture, "a" * 64, checkpoint=self.initial)
        self.assertEqual(a, compose(self.source, self.fixture, "a" * 64, checkpoint=self.initial))
        b = compose(self.source, self.fixture, "b" * 64, checkpoint=self.initial)
        self.assertNotEqual(a["bridge"], b["bridge"])
        with self.assertRaises(Invalid):
            compose(self.source, self.fixture, "c" * 64, checkpoint=self.initial)

    def test_wrong_collection_and_missing_portable_context_fail(self):
        with self.assertRaises(Invalid):
            compose(Copy("0" * 64), self.fixture, self.fixture.member_id, checkpoint=self.initial)
        bundle = compose(self.source, self.fixture, self.fixture.member_id, checkpoint=self.initial)
        del bundle["objects"][self.initial]
        with self.assertRaises(Invalid):
            verify_portable(bundle)

    def test_individual_value_bounds_and_exact_unicode(self):
        self.mutate("max-tag", "tag_add", "x" * 65536)
        self.mutate("max-desc", "description_set", "y" * 32768)
        for action, value in [("tag_add", "x" * 65537), ("description_set", "y" * 32769),
                              ("tag_add", "e\u0301"), ("tag_add", " x"), ("description_set", " ")]:
            with self.subTest(action=action, size=len(value)):
                with self.assertRaises(Invalid):
                    self.source.prepare("bad", action, value, expected=self.source.head)


if __name__ == "__main__":
    unittest.main(verbosity=2)
