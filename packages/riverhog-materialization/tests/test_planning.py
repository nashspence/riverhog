from __future__ import annotations

import pytest
from riverhog_materialization import (
    DestinationRules,
    MemberAdvice,
    escape_component,
    plan_materialization,
)

A = "a" * 64
B = "b" * 64
C = "c" * 64


def _rules(
    *,
    windows_names: bool = False,
    case_sensitive: bool = True,
    unicode_equivalence: str = "exact",
    component_bytes: int = 255,
    relative_path_bytes: int = 4096,
) -> DestinationRules:
    return DestinationRules(
        windows_names=windows_names,
        case_sensitive=case_sensitive,
        unicode_equivalence=unicode_equivalence,  # type: ignore[arg-type]
        component_bytes=component_bytes,
        relative_path_bytes=relative_path_bytes,
    )


def _member(artifact_id: str, *components: str) -> MemberAdvice:
    return MemberAdvice(
        artifact_id=artifact_id,
        materialization_hint={"components": list(components)} if components else None,
    )


def test_hints_and_id_fallback_are_disjoint_and_exact() -> None:
    planned = plan_materialization(
        [_member(B), _member(A, "Camera", "clip.mkv")],
        rules=_rules(),
    )
    assert [row.artifact_id for row in planned] == [A, B]
    assert planned[0].components == ("files", "Camera", "clip.mkv")
    assert planned[0].reason == "hint"
    assert planned[1].components == ("artifacts", "bb", B)
    assert planned[1].reason == "no-hint"
    assert planned[0].sidecar_components == (
        "provenance",
        "primary",
        "aa",
        A + ".fprov.jsonseq",
    )
    bypass = plan_materialization(
        [_member(A, "Camera", "clip.mkv")], rules=_rules(), mode="id-layout"
    )
    assert bypass[0].components == ("artifacts", "aa", A)
    assert bypass[0].reason == "id-layout"
    assert bypass[0].materialization_hint == ("Camera", "clip.mkv")


def test_all_same_leaf_and_file_directory_collisions_fall_back() -> None:
    planned = plan_materialization(
        [_member(A, "same"), _member(B, "same"), _member(C, "same", "child")],
        rules=_rules(),
    )
    assert all(
        row.components == ("artifacts", row.artifact_id[:2], row.artifact_id) for row in planned
    )
    assert all(row.reason == "destination-collision" for row in planned)


def test_directory_aliases_fall_back_even_when_leaves_differ() -> None:
    planned = plan_materialization(
        [_member(A, "Photos", "one"), _member(B, "photos", "two")],
        rules=_rules(case_sensitive=False),
    )
    assert {row.reason for row in planned} == {"destination-collision"}
    planned = plan_materialization(
        [_member(A, "e\u0301", "one"), _member(B, "é", "two")],
        rules=_rules(unicode_equivalence="NFC"),
    )
    assert {row.reason for row in planned} == {"destination-collision"}
    distinct = plan_materialization(
        [_member(A, "Photos", "one"), _member(B, "photos", "two")],
        rules=_rules(),
    )
    assert {row.reason for row in distinct} == {"hint"}


def test_windows_escaping_is_reversible_and_preserves_advice() -> None:
    rules = _rules(windows_names=True, case_sensitive=False)
    assert escape_component("CON.txt", rules) == "%43ON.txt"
    assert escape_component("COM¹", rules) == "%43OM¹"
    assert escape_component("a:b%. ", rules) == "a%3Ab%25%2E%20"
    planned = plan_materialization([_member(A, "CON.txt")], rules=rules)
    assert planned[0].components == ("files", "%43ON.txt")
    assert planned[0].materialization_hint == ("CON.txt",)
    assert planned[0].reason == "escaped-hint"


def test_unrepresentable_hint_falls_back_but_mandatory_layout_must_fit() -> None:
    planned = plan_materialization(
        [_member(A, "nested", "x" * 100)],
        rules=_rules(component_bytes=80),
    )
    assert planned[0].reason == "destination-limits"
    assert planned[0].components == ("artifacts", "aa", A)
    with pytest.raises(ValueError, match="mandatory ID and provenance layout"):
        plan_materialization([_member(A, "x")], rules=_rules(component_bytes=64))


@pytest.mark.parametrize(
    "hint",
    [
        {"components": [".."]},
        {"components": ["a/b"]},
        {"components": ["x"], "path": "x"},
        {"components": []},
    ],
)
def test_invalid_hint_is_an_error_not_an_absence(hint: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        plan_materialization([MemberAdvice(A, hint)], rules=_rules())


def test_duplicate_member_is_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        plan_materialization([_member(A, "one"), _member(A, "two")], rules=_rules())
