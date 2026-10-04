"""Reference-only executable acceptance examples; no production integration claim."""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from model import (
    AuthorityError, ModelStore, Observation, StaleProposal, catalog_run_boundary, digest, membership,
    placement, positive_decimal, schedule_health,
)

SOURCE, VIEW, OTHER_VIEW = "a" * 64, "b" * 64, "c" * 64
WHEN = "2025-12-24T08:00:12.123456789Z"


def upsert(cid: str = "7", revision: int = 1, matched: bool = True, *,
           when: str = WHEN, root: str | None = None, description: str = "") -> Observation:
    return Observation({
        "operation": "upsert", "collection_id": cid, "revision": str(revision),
        "archive_root_sha256": root or digest({"collection": cid}),
        "finalized_at": when, "description": description,
        "tag_revision": revision, "tag_set_identity": digest({"revision": revision}),
    }, matched)


def departure(cid: str, revision: int, cause: str = "visibility_lost") -> Observation:
    return Observation({"operation": "departure", "collection_id": cid,
                        "revision": str(revision), "cause": cause}, False)


class PlacementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.vectors = json.loads(Path(__file__).with_name("placement-vectors.json").read_text())

    def test_golden_paths(self) -> None:
        for row in self.vectors["valid"]:
            with self.subTest(row=row):
                self.assertEqual(placement(row["timestamp"], row["partition"]), row["path"])

    def test_invalid_timestamps(self) -> None:
        for value in self.vectors["invalid_timestamps"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                placement(value)

    def test_invalid_patterns(self) -> None:
        for value in self.vectors["invalid_partitions"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                placement(WHEN, value)

    def test_pattern_extent_is_not_collection_extent(self) -> None:
        for pattern in ("%Y/" * 8 + "%Y", "0" * 257):
            with self.assertRaises(ValueError):
                placement(WHEN, pattern)

    def test_nanosecond_order_is_preserved(self) -> None:
        stamps = ["2025-12-24T08:00:12.000000001Z", "2025-12-24T08:00:12.000000002Z",
                  "2025-12-24T08:00:12.999999999Z", "2025-12-24T08:00:13.000000000Z"]
        self.assertEqual(sorted(map(placement, stamps)), list(map(placement, stamps)))
        self.assertEqual(len(set(map(placement, stamps))), 4)

    def test_no_locale_or_timezone_lookup(self) -> None:
        with patch.dict("os.environ", {"TZ": "Pacific/Honolulu", "LANG": "tr_TR.UTF-8"}):
            self.assertEqual(placement(WHEN), "2025/12/20251224T080012.123456789Z")

    def test_large_exact_ids_and_bad_scalar_forms(self) -> None:
        self.assertEqual(positive_decimal("9007199254740993", (1 << 63) - 1), 9007199254740993)
        for value in ("01", "-1", "0", "9223372036854775808", True, 7):
            with self.subTest(value=value), self.assertRaises(ValueError):
                positive_decimal(value, (1 << 63) - 1)


class MembershipTests(unittest.TestCase):
    def setUp(self) -> None:
        self.descriptor = upsert().document
        self.receipt = {
            "collection_id": "7", "revision": 1,
            "tag_set_identity": self.descriptor["tag_set_identity"],
            "tag": "camera", "present": True,
        }

    def test_exact_positive_and_negative(self) -> None:
        self.assertTrue(membership(self.receipt, self.descriptor, "camera"))
        self.assertFalse(membership({**self.receipt, "present": False}, self.descriptor, "camera"))

    def test_mismatched_authorities(self) -> None:
        for key, value in (("collection_id", "8"), ("revision", 2),
                           ("tag_set_identity", "f" * 64), ("tag", "Camera")):
            with self.subTest(key=key), self.assertRaises(AuthorityError):
                membership({**self.receipt, key: value}, self.descriptor, "camera")

    def test_missing_or_untyped_truth_is_not_false(self) -> None:
        for value in (None, 0, 1, "false", [], {}):
            with self.subTest(value=value), self.assertRaises(AuthorityError):
                membership({**self.receipt, "present": value}, self.descriptor, "camera")
        self.receipt.pop("present")
        with self.assertRaises(AuthorityError):
            membership(self.receipt, self.descriptor, "camera")

    def test_bool_is_not_a_revision(self) -> None:
        with self.assertRaises(AuthorityError):
            membership({**self.receipt, "revision": True}, self.descriptor, "camera")


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "reference.sqlite3"
        self.store = ModelStore.initialize(self.path, source=SOURCE, view=VIEW, tag="camera")

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def restart(self) -> None:
        self.store.close()
        self.store = ModelStore(self.path)

    def baseline(self, items: tuple[Observation, ...] = (), *, backfill: bool = False) -> None:
        if backfill:
            self.store.rebaseline(self.store.position(), source=SOURCE, view=VIEW,
                                  cursor="explicit:backfill", backfill=True)
        self.store.accept(self.store.position(), items, source=SOURCE, view=VIEW,
                          next_cursor="catchup:0", next_phase="catchup")

    def changes(self, *items: Observation, through: int | None = None) -> None:
        before = self.store.position()
        high = through if through is not None else (items[-1].revision if items else before.through)
        self.store.accept(before, items, source=before.source, view=before.view,
                          next_cursor=f"changes:{high}", next_phase="following", through=high)

    def test_observe_baseline_does_not_admit(self) -> None:
        self.baseline((upsert(),))
        self.changes(upsert())  # Equal per-collection revision replay during catchup.
        self.assertEqual(self.store.intents(), [])

    def test_backfill_admits_existing_match(self) -> None:
        self.baseline((upsert(),), backfill=True)
        self.assertEqual(len(self.store.intents()), 1)
        self.changes(upsert())
        self.assertEqual(len(self.store.intents()), 1)

    def test_false_to_true_admits_once_and_restart_retains_it(self) -> None:
        self.baseline((upsert(matched=False),))
        self.changes(upsert(revision=2))
        self.restart()
        self.changes(upsert(revision=3, description="still selected"))
        self.assertEqual(len(self.store.intents()), 1)

    def test_unknown_new_matching_collection_admits(self) -> None:
        self.baseline()
        self.changes(upsert())
        self.assertEqual(self.store.intents()[0]["collection_id"], "7")

    def test_still_matching_update_does_not_backfill(self) -> None:
        self.baseline((upsert(),))
        self.changes(upsert(revision=2))
        self.assertEqual(self.store.intents(), [])

    def test_departure_retains_admitted_placement(self) -> None:
        self.baseline((upsert(),), backfill=True)
        original = self.store.intents()
        self.changes(departure("7", 2, "collection_deleted"))
        self.assertEqual(self.store.intents(), original)

    def test_remove_then_readd_reuses_frozen_placement(self) -> None:
        self.baseline((upsert(),), backfill=True)
        original = self.store.intents()
        self.changes(upsert(revision=2, matched=False))
        self.changes(upsert(revision=3))
        self.assertEqual(self.store.intents(), original)

    def test_observed_baseline_can_be_ahead_of_feed(self) -> None:
        self.baseline((upsert(revision=5),))
        self.assertEqual(self.store.position().through, 0)
        self.changes(upsert(revision=1, matched=False), upsert(revision=2))
        self.assertEqual(self.store.position().through, 2)
        self.assertEqual(self.store.intents(), [])
        self.changes(upsert(revision=5))
        self.assertEqual(self.store.intents(), [])

    def test_equal_revision_conflict_rolls_back_everything(self) -> None:
        self.baseline((upsert(revision=5),))
        before = self.store.position()
        with self.assertRaises(AuthorityError):
            self.changes(upsert(revision=5, description="forged same revision"))
        self.assertEqual(self.store.position(), before)
        self.assertEqual(self.store.intents(), [])

    def test_same_revision_conflicting_match_truth_fails(self) -> None:
        self.baseline((upsert(),))
        with self.assertRaises(AuthorityError):
            self.changes(upsert(matched=False))

    def test_newer_revision_cannot_change_finalization_time(self) -> None:
        self.baseline((upsert(),))
        with self.assertRaises(AuthorityError):
            self.changes(upsert(revision=2, when="2025-12-25T08:00:12.123456789Z"))

    def test_newer_revision_cannot_change_archive_root(self) -> None:
        self.baseline((upsert(),))
        with self.assertRaises(AuthorityError):
            self.changes(upsert(revision=2, root="f" * 64))

    def test_departure_does_not_erase_immutable_identity_check(self) -> None:
        self.baseline((upsert(),))
        self.changes(departure("7", 2))
        with self.assertRaises(AuthorityError):
            self.changes(upsert(revision=3, root="f" * 64))

    def test_cursor_and_intent_commit_or_rollback_together(self) -> None:
        self.baseline()
        before = self.store.position()
        args = dict(source=SOURCE, view=VIEW, next_cursor="changes:1",
                    next_phase="following", through=1)
        with self.assertRaisesRegex(RuntimeError, "injected crash"):
            self.store.accept(before, (upsert(),), **args, fail_before_commit=True)
        self.restart()
        self.assertEqual(self.store.position(), before)
        self.assertEqual(self.store.intents(), [])
        self.store.accept(before, (upsert(),), **args)
        self.restart()
        self.assertEqual(self.store.position().through, 1)
        self.assertEqual(len(self.store.intents()), 1)

    def test_two_prepared_workers_cannot_both_commit(self) -> None:
        self.baseline()
        before = self.store.position()
        other = ModelStore(self.path)
        try:
            self.changes(upsert())
            with self.assertRaises(StaleProposal):
                other.accept(before, (upsert(),), source=SOURCE, view=VIEW,
                             next_cursor="other:1", next_phase="following", through=1)
            self.assertEqual(len(other.intents()), 1)
        finally:
            other.close()

    def test_delayed_page_cannot_cross_rebaseline(self) -> None:
        before = self.store.position()
        self.store.rebaseline(before, source=SOURCE, view=OTHER_VIEW, cursor="new:0")
        with self.assertRaises(StaleProposal):
            self.store.accept(before, (upsert(),), source=SOURCE, view=VIEW,
                              next_cursor="old:next", next_phase="catchup")
        self.assertEqual(self.store.position().view, OTHER_VIEW)

    def test_delayed_error_cannot_reset_new_generation(self) -> None:
        before = self.store.position()
        self.store.rebaseline(before, source=SOURCE, view=OTHER_VIEW, cursor="new:0")
        with self.assertRaises(StaleProposal):
            self.store.reset(before, "catalog_sync_view_changed")
        self.assertEqual(self.store.position().phase, "catalog")

    def test_page_authority_is_checked_before_decisions(self) -> None:
        before = self.store.position()
        for source, view in (("f" * 64, VIEW), (SOURCE, OTHER_VIEW)):
            with self.subTest(source=source, view=view), self.assertRaises(AuthorityError):
                self.store.accept(before, (upsert(),), source=source, view=view,
                                  next_cursor="next", next_phase="catchup")
        self.assertEqual(self.store.position(), before)

    def test_reset_preserves_intents_and_blocks_traversal(self) -> None:
        self.baseline((upsert(),), backfill=True)
        original = self.store.intents()
        self.store.reset(self.store.position(), "catalog_sync_history_expired")
        self.restart()
        with self.assertRaises(AuthorityError):
            self.changes(upsert(revision=2))
        self.assertEqual(self.store.intents(), original)

    def test_rebaseline_cannot_retarget_another_source(self) -> None:
        before = self.store.position()
        with self.assertRaises(AuthorityError):
            self.store.rebaseline(before, source="f" * 64, view=VIEW, cursor="other:0")
        self.assertEqual(self.store.position(), before)

    def test_explicit_same_source_rebaseline_keeps_paths(self) -> None:
        self.baseline((upsert(),), backfill=True)
        original = self.store.intents()
        self.store.rebaseline(self.store.position(), source=SOURCE, view=OTHER_VIEW,
                              cursor="fresh:0", backfill=True)
        self.store.accept(self.store.position(), (upsert(),), source=SOURCE, view=OTHER_VIEW,
                          next_cursor="fresh:catchup", next_phase="catchup")
        self.assertEqual(self.store.intents(), original)

    def test_timestamp_collision_is_disambiguated_without_renaming(self) -> None:
        self.baseline((upsert("7"), upsert("8", 2)), backfill=True)
        rows = self.store.intents()
        self.assertEqual(rows[0]["relative_path"], placement(WHEN))
        expected_suffix = digest({"source_identity": SOURCE, "collection_id": "8",
                                  "archive_root_sha256": upsert("8", 2).document["archive_root_sha256"]})
        self.assertEqual(rows[1]["relative_path"], placement(WHEN) + "--" + expected_suffix)
        self.restart()
        self.changes(upsert("7", 1), upsert("8", 2))
        self.assertEqual(self.store.intents(), rows)

    def test_collision_allocation_is_root_local_not_a_global_order_claim(self) -> None:
        self.baseline()
        self.changes(upsert("8", 1), upsert("7", 2))
        # This intentionally models first reservation, not pure identity-to-path hashing.
        paths = {r["collection_id"]: r["relative_path"] for r in self.store.intents()}
        self.assertEqual(paths["8"], placement(WHEN))
        self.assertIn("--", paths["7"])

    def test_full_digest_collision_fails_instead_of_overwriting(self) -> None:
        self.baseline()
        before = self.store.position()
        with patch("model.digest", return_value="f" * 64), self.assertRaises(AuthorityError):
            self.changes(upsert("7", 1), upsert("8", 2), upsert("9", 3))
        self.assertEqual(self.store.position(), before)
        self.assertEqual(self.store.intents(), [])

    def test_eviction_survives_rematch(self) -> None:
        self.baseline((upsert(),), backfill=True)
        self.store.suppress("7")
        self.changes(upsert(revision=2, matched=False))
        self.changes(upsert(revision=3))
        self.restart()
        self.assertEqual(self.store.intents()[0]["state"], "evicted")
        self.assertEqual(self.store.intents()[0]["suppressed"], 1)

    def test_empty_advancing_page_commits_its_cursor(self) -> None:
        self.baseline()
        self.changes(through=20)
        self.assertEqual(self.store.position().through, 20)
        self.assertEqual(self.store.intents(), [])

    def test_feed_watermark_does_not_move_backwards(self) -> None:
        self.baseline()
        self.changes(through=20)
        with self.assertRaises(ValueError):
            self.changes(through=19)

    def test_valid_logical_extent_exceeds_page_size(self) -> None:
        self.baseline()
        for start in range(1, 1002, 100):
            items = tuple(upsert(str(i), i) for i in range(start, min(start + 100, 1002)))
            self.changes(*items)
        self.assertEqual(len(self.store.intents()), 1001)
        self.assertEqual(self.store.position().through, 1001)

    def test_existing_state_is_not_silently_initialized(self) -> None:
        before = self.store.position()
        with self.assertRaises(FileExistsError):
            ModelStore.initialize(self.path, source=SOURCE, view=VIEW, tag="other")
        self.assertEqual(self.store.position(), before)

    def test_missing_state_is_not_created_by_runtime(self) -> None:
        missing = self.path.with_name("missing.sqlite3")
        with self.assertRaises(sqlite3.OperationalError):
            ModelStore(missing)
        self.assertFalse(missing.exists())


class RunBoundaryTests(unittest.TestCase):
    def test_only_explicit_caught_up_ends_the_horizon(self) -> None:
        self.assertFalse(catalog_run_boundary("changes", False))
        self.assertTrue(catalog_run_boundary("changes", True))
        for kind in ("checkpoint", "catalog", "reset"):
            self.assertFalse(catalog_run_boundary(kind, None))

    def test_phase_or_implicit_truth_cannot_substitute_for_caught_up(self) -> None:
        for kind, signal in (("following", None), ("changes", None),
                             ("changes", 1), ("catalog", True)):
            with self.subTest(kind=kind, signal=signal), self.assertRaises(ValueError):
                catalog_run_boundary(kind, signal)


class SchedulingTests(unittest.TestCase):
    def test_idle_between_invocations_is_not_unhealthy(self) -> None:
        self.assertEqual(schedule_health(installed=True, enabled=True, active=False,
                                         registration_valid=True), "idle")

    def test_registration_health_is_separate_from_activity(self) -> None:
        for flags, expected in (
            (dict(installed=False, enabled=False, active=False, registration_valid=False), "absent"),
            (dict(installed=True, enabled=False, active=False, registration_valid=True), "disabled"),
            (dict(installed=True, enabled=True, active=True, registration_valid=True), "running"),
            (dict(installed=True, enabled=True, active=False, registration_valid=False), "failed"),
            (dict(installed=True, enabled=True, active=False, registration_valid=True, overdue=True), "overdue"),
        ):
            with self.subTest(flags=flags):
                self.assertEqual(schedule_health(**flags), expected)


if __name__ == "__main__":
    unittest.main()
