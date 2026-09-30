from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import Any

from a_stove0_filename_prefix_sidecar_observer import LocatorEvidence, compare_filenames
from riverhog_provenance_contracts import SOURCE_NAMING_VIEW_SCHEME

VIEW_A = "urn:uuid:11111111-1111-4111-8111-111111111111"
VIEW_B = "urn:uuid:22222222-2222-4222-8222-222222222222"


def _locator(
    subject_id: str,
    name: bytes,
    *,
    context_id: str,
    view_ids: Sequence[str] = (),
    syntax: str = "posix",
) -> LocatorEvidence:
    return LocatorEvidence(
        subject_id=subject_id,
        locator={
            "kind": "filesystem_path",
            "form": "absolute",
            "syntax": syntax,
            "name": {
                "kind": "bytes",
                "encoding": "utf-8" if syntax == "posix" else "utf-16le",
                "bytes": {
                    "encoding": "base64",
                    "data": base64.b64encode(name).decode(),
                    "byte_length": str(len(name)),
                },
            },
        },
        context_endpoint={"journal_id": f"journal-{subject_id}", "context_id": context_id},
        context_identifiers=tuple(
            {
                "scheme": SOURCE_NAMING_VIEW_SCHEME,
                "scope": "global",
                "value": {"kind": "text", "text": view_id},
            }
            for view_id in view_ids
        ),
        context_support={"kind": "context", "subject": subject_id, "id": context_id},
        locator_support={"kind": "locator", "subject": subject_id, "id": name.hex()},
    )


def _compare(
    first: LocatorEvidence, second: LocatorEvidence
) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    return compare_filenames(
        {"primary": (first,), "sidecar": (second,)},
        primary_ids=("primary",),
        sidecar_ids=("sidecar",),
        sidecar_suffixes=(".XMP", ".xmp"),
    )


def test_distinct_journals_and_contexts_share_explicit_source_view_with_exact_support() -> None:
    first = _locator("primary", b"/camera/clip.mp4", context_id="context-a", view_ids=(VIEW_A,))
    second = _locator("sidecar", b"/camera/clip.xmp", context_id="context-b", view_ids=(VIEW_A,))
    statuses, candidates = _compare(first, second)
    assert [row.status for row in statuses] == ["usable", "usable"]
    assert [(row.primary_id, row.sidecar_id, row.rule) for row in candidates] == [
        ("primary", "sidecar", "stem")
    ]
    assert all(
        support in candidates[0].support
        for support in (
            first.context_support,
            first.locator_support,
            second.context_support,
            second.locator_support,
        )
    )


def test_equal_names_from_different_source_views_do_not_pair() -> None:
    first = _locator("primary", b"/camera/clip.mp4", context_id="context-a", view_ids=(VIEW_A,))
    second = _locator("sidecar", b"/camera/clip.xmp", context_id="context-b", view_ids=(VIEW_B,))
    statuses, candidates = _compare(first, second)
    assert [row.status for row in statuses] == ["usable", "usable"]
    assert candidates == ()


def test_declared_uppercase_suffix_is_compared_in_original_source_units() -> None:
    first = _locator("primary", b"/camera/clip.mp4", context_id="context-a", view_ids=(VIEW_A,))
    second = _locator("sidecar", b"/camera/clip.XMP", context_id="context-b", view_ids=(VIEW_A,))
    _, candidates = _compare(first, second)
    assert [(row.primary_id, row.sidecar_id, row.rule) for row in candidates] == [
        ("primary", "sidecar", "stem")
    ]


def test_missing_or_conflicting_view_is_explicitly_incomplete() -> None:
    first = _locator("primary", b"/camera/clip.mp4", context_id="context-a", view_ids=(VIEW_A,))
    missing = _locator("sidecar", b"/camera/clip.xmp", context_id="context-b")
    statuses, candidates = _compare(first, missing)
    assert statuses[1].status == "insufficient"
    assert candidates == ()
    conflicting = _locator(
        "sidecar", b"/camera/clip.xmp", context_id="context-b", view_ids=(VIEW_A, VIEW_B)
    )
    statuses, candidates = _compare(first, conflicting)
    assert statuses[1].status == "ambiguous"
    assert candidates == ()


def test_mixed_scoped_and_unscoped_locators_cannot_silently_select_one() -> None:
    first = _locator("primary", b"/camera/clip.mp4", context_id="context-a", view_ids=(VIEW_A,))
    scoped = _locator("sidecar", b"/camera/clip.xmp", context_id="context-b", view_ids=(VIEW_A,))
    unscoped = _locator("sidecar", b"/camera/clip.xmp", context_id="context-c")
    statuses, candidates = compare_filenames(
        {"primary": (first,), "sidecar": (scoped, unscoped)},
        primary_ids=("primary",),
        sidecar_ids=("sidecar",),
        sidecar_suffixes=(".XMP", ".xmp"),
    )
    assert statuses[1].status == "insufficient"
    assert candidates == ()


def test_exact_shared_context_can_support_pair_without_a_view_identifier() -> None:
    first = _locator("primary", b"/camera/clip.mp4", context_id="shared")
    second = _locator("sidecar", b"/camera/clip.mp4.xmp", context_id="shared")
    shared_endpoint: Mapping[str, Any] = {"journal_id": "shared", "context_id": "shared"}
    first = LocatorEvidence(
        first.subject_id,
        first.locator,
        shared_endpoint,
        first.context_identifiers,
        first.context_support,
        first.locator_support,
    )
    second = LocatorEvidence(
        second.subject_id,
        second.locator,
        shared_endpoint,
        second.context_identifiers,
        second.context_support,
        second.locator_support,
    )
    statuses, candidates = _compare(first, second)
    assert [row.status for row in statuses] == ["insufficient", "insufficient"]
    assert [row.rule for row in candidates] == ["full-leaf"]


def test_source_root_and_parent_units_are_exact() -> None:
    first = _locator("primary", b"/camera/clip.mp4", context_id="context-a", view_ids=(VIEW_A,))
    second = _locator("sidecar", b"/other/clip.xmp", context_id="context-b", view_ids=(VIEW_A,))
    _, candidates = _compare(first, second)
    assert candidates == ()


def test_windows_drive_unc_and_extended_roots_keep_exact_spelling() -> None:
    first = _locator(
        "primary",
        "C:\\camera\\clip.mov".encode("utf-16le"),
        context_id="context-a",
        view_ids=(VIEW_A,),
        syntax="windows",
    )
    second = _locator(
        "sidecar",
        "C:\\camera\\clip.xmp".encode("utf-16le"),
        context_id="context-b",
        view_ids=(VIEW_A,),
        syntax="windows",
    )
    _, candidates = _compare(first, second)
    assert [row.rule for row in candidates] == ["stem"]
    lowercase_drive = _locator(
        "sidecar",
        "c:\\camera\\clip.xmp".encode("utf-16le"),
        context_id="context-b",
        view_ids=(VIEW_A,),
        syntax="windows",
    )
    assert _compare(first, lowercase_drive)[1] == ()
    extended = _locator(
        "sidecar",
        "\\\\?\\C:\\camera\\clip.xmp".encode("utf-16le"),
        context_id="context-b",
        view_ids=(VIEW_A,),
        syntax="windows",
    )
    assert _compare(first, extended)[1] == ()
    unc_primary = _locator(
        "primary",
        "\\\\server\\share\\clip.mov".encode("utf-16le"),
        context_id="context-a",
        view_ids=(VIEW_A,),
        syntax="windows",
    )
    unc_sidecar = _locator(
        "sidecar",
        "\\\\server\\share\\clip.xmp".encode("utf-16le"),
        context_id="context-b",
        view_ids=(VIEW_A,),
        syntax="windows",
    )
    assert [row.rule for row in _compare(unc_primary, unc_sidecar)[1]] == ["stem"]


def test_filename_candidates_do_not_use_arbitrary_prefix_or_stream_suffix() -> None:
    first = _locator("primary", b"/camera/clip2.mov", context_id="a", view_ids=(VIEW_A,))
    sidecar = _locator("sidecar", b"/camera/clip.xmp", context_id="b", view_ids=(VIEW_A,))
    assert _compare(first, sidecar)[1] == ()
    stream = _locator(
        "sidecar",
        "C:\\camera\\clip.xmp:stream".encode("utf-16le"),
        context_id="b",
        view_ids=(VIEW_A,),
        syntax="windows",
    )
    windows_primary = _locator(
        "primary",
        "C:\\camera\\clip.mov".encode("utf-16le"),
        context_id="a",
        view_ids=(VIEW_A,),
        syntax="windows",
    )
    statuses, candidates = _compare(windows_primary, stream)
    assert statuses[1].status == "unsupported"
    assert candidates == ()
