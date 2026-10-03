from __future__ import annotations

import unittest

from model import (
    ArchiveCopy,
    ArchiveObject,
    CachePlacement,
    InvariantViolation,
    PayloadIdentity,
    SelectionError,
    SelectionPolicy,
    plan_retrieval,
)

COLLECTION_ID = 42


def pack(object_id: str = "pack-0", *, variant: str = "a") -> ArchiveObject:
    return ArchiveObject(
        PayloadIdentity(
            object_id=object_id,
            kind="pack",
            plaintext_bytes=100,
            stored_bytes=140,
            age_state_sha256=(variant * 64)[:64],
            pack_plan_sha256=("1" if variant == "a" else "2") * 64,
            pack_index_sha256=("3" if variant == "a" else "4") * 64,
        )
    )


def segment(object_id: str = "segment-0", *, offset: int = 0) -> ArchiveObject:
    return ArchiveObject(
        PayloadIdentity(
            object_id=object_id,
            kind="segment",
            plaintext_bytes=50,
            stored_bytes=90,
            age_state_sha256="5" * 64,
            segment_artifact_id="6" * 64,
            segment_artifact_bytes=100,
            segment_artifact_sha256="7" * 64,
            segment_artifact_offset=offset,
            segment_bytes=50,
        )
    )


def copy(
    store: str,
    incarnation: str,
    *,
    read_mode: str = "immediate",
    usable: bool = True,
    complete: bool = True,
    retiring: bool = False,
    objects: tuple[ArchiveObject, ...] | None = None,
) -> ArchiveCopy:
    return ArchiveCopy(
        store=store,
        incarnation_id=incarnation,
        read_mode=read_mode,  # type: ignore[arg-type]
        usable_binding=usable,
        complete=complete,
        retiring=retiring,
        objects=objects or (pack(),),
    )


def cached(
    *,
    source_store: str,
    source_incarnation: str,
    cache_store: str = "local-cache",
    cache_incarnation: str = "cache-1",
    object_id: str = "pack-0",
    usable: bool = True,
) -> CachePlacement:
    return CachePlacement(
        collection_id=COLLECTION_ID,
        object_id=object_id,
        source_store=source_store,
        source_incarnation_id=source_incarnation,
        cache_store=cache_store,
        cache_incarnation_id=cache_incarnation,
        usable_cache_binding=usable,
    )


class RetrievalSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = SelectionPolicy(
            archive_read_order=("volume", "cloud"),
            cache_store_order=("local-cache", "elastic-cache"),
        )

    def test_cache_from_unreachable_higher_priority_archive_beats_selected_cloud(self) -> None:
        copies = (
            copy("volume", "volume-1", read_mode="restore_required", usable=False),
            copy("cloud", "cloud-1", read_mode="immediate"),
        )
        plan = plan_retrieval(
            collection_id=COLLECTION_ID,
            copies=copies,
            placements=(cached(source_store="volume", source_incarnation="volume-1"),),
            object_ids=("pack-0",),
            policy=self.policy,
        )
        item = plan.objects[0]
        self.assertEqual(plan.archive_source_store, "cloud")
        self.assertEqual(item.read_mode, "cache")
        self.assertEqual(item.archive_source_store, "cloud")
        self.assertEqual(item.cache_source_store, "volume")
        self.assertFalse(plan.requires_restore)

    def test_explicit_source_is_strict_but_cache_still_wins(self) -> None:
        copies = (
            copy("volume", "volume-1", read_mode="restore_required"),
            copy("cloud", "cloud-1", read_mode="immediate"),
        )
        plan = plan_retrieval(
            collection_id=COLLECTION_ID,
            copies=copies,
            placements=(cached(source_store="volume", source_incarnation="volume-1"),),
            object_ids=("pack-0",),
            policy=self.policy,
            requested_source_store="cloud",
        )
        item = plan.objects[0]
        self.assertEqual(plan.requested_source_store, "cloud")
        self.assertEqual(item.archive_source_store, "cloud")
        self.assertEqual(item.cache_source_store, "volume")
        self.assertEqual(item.read_mode, "cache")

    def test_explicit_unusable_source_does_not_fall_back_even_with_other_archive(self) -> None:
        copies = (
            copy("volume", "volume-1", usable=False),
            copy("cloud", "cloud-1"),
        )
        with self.assertRaisesRegex(SelectionError, "requested archive source"):
            plan_retrieval(
                collection_id=COLLECTION_ID,
                copies=copies,
                placements=(cached(source_store="cloud", source_incarnation="cloud-1"),),
                object_ids=("pack-0",),
                policy=self.policy,
                requested_source_store="volume",
            )

    def test_cache_store_order_wins_over_archive_source_order(self) -> None:
        copies = (
            copy("volume", "volume-1"),
            copy("cloud", "cloud-1"),
        )
        placements = (
            cached(
                source_store="volume",
                source_incarnation="volume-1",
                cache_store="elastic-cache",
                cache_incarnation="elastic-1",
            ),
            cached(
                source_store="cloud",
                source_incarnation="cloud-1",
                cache_store="local-cache",
                cache_incarnation="local-1",
            ),
        )
        plan = plan_retrieval(
            collection_id=COLLECTION_ID,
            copies=copies,
            placements=placements,
            object_ids=("pack-0",),
            policy=self.policy,
        )
        self.assertEqual(plan.objects[0].cache_store, "local-cache")
        self.assertEqual(plan.objects[0].cache_source_store, "cloud")

    def test_unusable_preferred_cache_falls_through_to_next_usable_cache(self) -> None:
        copies = (copy("volume", "volume-1"), copy("cloud", "cloud-1"))
        placements = (
            cached(
                source_store="volume",
                source_incarnation="volume-1",
                cache_store="local-cache",
                usable=False,
            ),
            cached(
                source_store="cloud",
                source_incarnation="cloud-1",
                cache_store="elastic-cache",
                cache_incarnation="elastic-1",
            ),
        )
        plan = plan_retrieval(
            collection_id=COLLECTION_ID,
            copies=copies,
            placements=placements,
            object_ids=("pack-0",),
            policy=self.policy,
        )
        self.assertEqual(plan.objects[0].cache_store, "elastic-cache")

    def test_retiring_cache_source_is_not_reused(self) -> None:
        copies = (
            copy("volume", "volume-1", retiring=True),
            copy("cloud", "cloud-1", read_mode="immediate"),
        )
        plan = plan_retrieval(
            collection_id=COLLECTION_ID,
            copies=copies,
            placements=(cached(source_store="volume", source_incarnation="volume-1"),),
            object_ids=("pack-0",),
            policy=self.policy,
        )
        self.assertEqual(plan.archive_source_store, "cloud")
        self.assertEqual(plan.objects[0].read_mode, "immediate")
        self.assertIsNone(plan.objects[0].cache_source_store)

    def test_cross_copy_payload_mismatch_fails_closed(self) -> None:
        copies = (
            copy("volume", "volume-1", objects=(pack(variant="b"),)),
            copy("cloud", "cloud-1", objects=(pack(variant="a"),)),
        )
        with self.assertRaisesRegex(InvariantViolation, "copy-invariant sealed payload identity"):
            plan_retrieval(
                collection_id=COLLECTION_ID,
                copies=copies,
                placements=(cached(source_store="volume", source_incarnation="volume-1"),),
                object_ids=("pack-0",),
                policy=SelectionPolicy(
                    archive_read_order=("cloud", "volume"),
                    cache_store_order=("local-cache",),
                ),
            )

    def test_partial_cache_coverage_only_leaves_miss_restore_required(self) -> None:
        objects = (pack("pack-0"), segment("segment-1"))
        copies = (
            copy(
                "volume",
                "volume-1",
                read_mode="restore_required",
                objects=objects,
            ),
        )
        plan = plan_retrieval(
            collection_id=COLLECTION_ID,
            copies=copies,
            placements=(cached(source_store="volume", source_incarnation="volume-1"),),
            object_ids=("pack-0", "segment-1"),
            policy=self.policy,
        )
        self.assertEqual([item.read_mode for item in plan.objects], ["cache", "restore_required"])
        self.assertTrue(plan.requires_restore)

    def test_full_cache_coverage_of_restore_required_source_needs_no_restore(self) -> None:
        objects = (pack("pack-0"), segment("segment-1"))
        copies = (
            copy(
                "volume",
                "volume-1",
                read_mode="restore_required",
                objects=objects,
            ),
        )
        placements = (
            cached(source_store="volume", source_incarnation="volume-1", object_id="pack-0"),
            cached(
                source_store="volume",
                source_incarnation="volume-1",
                object_id="segment-1",
                cache_store="elastic-cache",
                cache_incarnation="elastic-1",
            ),
        )
        plan = plan_retrieval(
            collection_id=COLLECTION_ID,
            copies=copies,
            placements=placements,
            object_ids=("pack-0", "segment-1"),
            policy=self.policy,
        )
        self.assertFalse(plan.requires_restore)
        self.assertTrue(all(item.read_mode == "cache" for item in plan.objects))

    def test_cache_source_incarnation_is_fenced(self) -> None:
        copies = (
            copy("volume", "volume-new", usable=False),
            copy("cloud", "cloud-1"),
        )
        plan = plan_retrieval(
            collection_id=COLLECTION_ID,
            copies=copies,
            placements=(cached(source_store="volume", source_incarnation="volume-old"),),
            object_ids=("pack-0",),
            policy=self.policy,
        )
        self.assertEqual(plan.objects[0].read_mode, "immediate")

    def test_planned_archive_and_cache_sources_are_separate_durable_choices(self) -> None:
        copies = (copy("volume", "volume-1"), copy("cloud", "cloud-1"))
        plan = plan_retrieval(
            collection_id=COLLECTION_ID,
            copies=copies,
            placements=(cached(source_store="cloud", source_incarnation="cloud-1"),),
            object_ids=("pack-0",),
            policy=self.policy,
        )
        item = plan.objects[0]
        self.assertEqual((item.archive_source_store, item.archive_source_incarnation_id), ("volume", "volume-1"))
        self.assertEqual((item.cache_source_store, item.cache_source_incarnation_id), ("cloud", "cloud-1"))
        self.assertEqual((item.cache_store, item.cache_incarnation_id), ("local-cache", "cache-1"))


if __name__ == "__main__":
    unittest.main()
