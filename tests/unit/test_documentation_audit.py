"""Coverage, contextual impact and actual destination parity share one retained record."""

import copy
import sys
from pathlib import Path

import pytest

from tests.documentation_fixtures import closure, document

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from contract_atlas.documentation_audit import (  # noqa: E402
    build_audit,
    build_snapshot,
    check_record,
    terminal_summary,
)
from contract_atlas.documentation_markdown import compile_corpus  # noqa: E402
from contract_atlas.documentation_requirements import build_requirements, target_key  # noqa: E402
from contract_atlas.model import ContractAtlasError, canonical_sha256  # noqa: E402


def snapshot(code=None, *, prose="Existing accurate prose.", observed=True, extra_files=None):
    code = closure() if code is None else code
    requirements = build_requirements(code)
    entries = [
        {"target": row["target"], "summary": prose}
        for row in requirements["subjects"].values()
        if row["rule"] == "authored"
    ]
    compiled = compile_corpus(
        {"reference.md": document(entries), **(extra_files or {})}, requirements
    )
    return build_snapshot(
        code,
        compiled,
        requirements,
        identity={"source_sha": "a" * 40, "documentation_commit": "b" * 40},
        expected={"fixture/help": prose},
        observed={"fixture/help": prose} if observed else {},
        artifacts={"fixture.whl": "e" * 64},
    )


def custody(baseline):
    return {
        "kind": "selected-prepared-candidate",
        "snapshot_sha256": canonical_sha256(baseline),
        "manifest_sha256": "d" * 64,
        "source_sha": "a" * 40,
        "verification": {
            "method": "coordinator-selected exact local prepared inventory",
            "locator": "/synthetic/candidate",
        },
    }


def compare(before, after, *, stage="prepared"):
    return build_audit(after, stage=stage, baseline=before, baseline_custody=custody(before))


def test_genuine_initial_review_is_full_and_not_a_green_empty_diff():
    record = build_audit(snapshot(), stage="prepared", initial=True)
    assert record["state"] == "REVIEW" and record["comparison"] == "initial-review"
    assert record["deltas"] and all(d["change"] == "initial" for d in record["deltas"])
    assert check_record(record, prepared=True) == 0
    assert "human approval" in terminal_summary(record)


def test_unavailable_requested_baseline_never_becomes_initial():
    with pytest.raises(ContractAtlasError, match="baseline unavailable"):
        build_audit(snapshot(), stage="prepared")
    before = snapshot()
    untrusted = custody(before)
    untrusted.pop("verification")
    with pytest.raises(ContractAtlasError, match="custody"):
        build_audit(snapshot(), stage="prepared", baseline=before, baseline_custody=untrusted)


@pytest.mark.parametrize(
    "meaning,prose,expected",
    [(True, False, "meaning-only"), (False, True, "prose-only"), (True, True, "meaning-and-prose")],
)
def test_stable_subject_drift_distinguishes_meaning_and_effective_prose(meaning, prose, expected):
    before = snapshot()
    changed = closure()
    if meaning:
        changed["external_contract"]["cli"]["fixture"]["parameters"][0]["default"] = "different"
    after = snapshot(
        changed, prose="Changed editorial expression." if prose else "Existing accurate prose."
    )
    record = compare(before, after)
    assert record["state"] == "REVIEW"
    assert any(d["change"] == expected for d in record["deltas"])
    assert record["contract_delta"] if meaning else not record["contract_delta"]
    assert check_record(record, prepared=True) == 0


def test_parent_context_changes_flag_an_unchanged_schema_field():
    before = snapshot()
    changed = closure()
    changed["external_contract"]["schemas"]["test"]["additionalProperties"] = False
    after = snapshot(changed)
    record = compare(before, after)
    field = target_key({"element_id": "schema:fixture", "pointer": "/properties/limit"})
    delta = next(d for d in record["deltas"] if d["subject"] == field)
    assert delta["change"] == "meaning-only"
    before_row = record["baseline"]["subjects"][delta["subject"]]
    after_row = record["current"]["subjects"][delta["subject"]]
    assert before_row["meaning"]["owned"] == after_row["meaning"]["owned"]
    assert before_row["meaning"]["dependencies"] != after_row["meaning"]["dependencies"]


def test_removed_requirements_and_relaxations_stay_in_the_denominator_history():
    before = snapshot()
    changed = closure()
    del changed["external_contract"]["schemas"]["test"]["properties"]["limit"]
    after = snapshot(changed)
    removed = compare(before, after)
    assert any(d["change"] == "removed" and d["before"] is not None for d in removed["deltas"])
    after = snapshot()
    row = next(s for s in after["subjects"].values() if s["requirement"]["rule"] == "authored")
    row["requirement"]["rule"] = "structural"
    relaxed = compare(before, after)
    assert any(f["code"] == "requirement-changed" for f in relaxed["findings"])
    assert relaxed["state"] == "REVIEW"


def test_global_changes_and_incompatible_profiles_cannot_claim_unchanged():
    before = snapshot()
    changed = closure()
    changed["boundaries"]["global"] = "new declaration"
    record = compare(before, snapshot(changed))
    assert any(f["code"] == "global-meaning-changed" for f in record["findings"])
    after = snapshot()
    after["profile"] = "different-fingerprint-policy"
    record = compare(before, after)
    assert record["comparison"] == "incomparable"
    assert all(d["change"] == "unknown-meaning" for d in record["deltas"])
    assert record["current"]["scopes"] and record["baseline"]["scopes"]


def test_preview_unperformed_native_checks_are_explicit_and_cannot_qualify_preparation():
    record = build_audit(snapshot(observed=False), stage="preview", initial=True)
    assert record["state"] == "REVIEW" and record["checks"]["native"] == "not-performed"
    assert record["destinations"][0]["status"] == "unverified"
    assert check_record(record) == 0 and check_record(record, prepared=True) == 2
    prepared = build_audit(snapshot(observed=False), stage="prepared", initial=True)
    assert prepared["state"] == "FAIL" and check_record(prepared, prepared=True) == 2


@pytest.mark.parametrize("stage", ["preview", "prepared"])
def test_observed_mismatch_fails_in_every_stage(stage):
    current = snapshot()
    current["observed"]["fixture/help"] = "different native output"
    record = build_audit(current, stage=stage, initial=True)
    assert record["state"] == "FAIL" and check_record(record) == 2


def test_audit_state_and_findings_cannot_be_overridden_by_a_renderer_or_stamp():
    record = build_audit(snapshot(observed=False), stage="prepared", initial=True)
    for mutation in ("state", "findings", "counts"):
        changed = copy.deepcopy(record)
        changed[mutation] = "PASS" if mutation == "state" else [] if mutation == "findings" else {}
        with pytest.raises(ContractAtlasError, match="differ"):
            check_record(changed)
    bounded = terminal_summary(record, limit=1)
    assert len(record["findings"]) > 1 and "FAIL" in bounded
    bad = copy.deepcopy(record)
    bad["format"] = "unknown/v9"
    with pytest.raises(ContractAtlasError, match="unsupported"):
        check_record(bad)


def test_native_semantic_parity_keeps_large_integers_distinct_from_decimal_text():
    from contract_atlas.documentation_native import semantic_frame, verify_semantic_parity
    from contract_atlas.model import canonical_bytes

    integer = semantic_frame({"constant": 2**256 - 1})
    literal = semantic_frame({"constant": str(2**256 - 1)})
    assert integer["value"] == literal["value"]
    assert integer["unsafe_integer_paths"] == ["/constant"]
    assert literal["unsafe_integer_paths"] == []
    assert canonical_bytes(integer)
    with pytest.raises(ContractAtlasError, match="semantic facts"):
        verify_semantic_parity(integer, literal, destination="Python")


@pytest.mark.parametrize("subject_count", [1, 2])
def test_guide_changes_retain_actual_text_and_all_declared_associations(subject_count):
    from tests.documentation_fixtures import PARAMETER, ROOT_TARGET

    def guidance(text):
        return document(
            [ROOT_TARGET, PARAMETER][:subject_count],
            body="# Journey\n\n" + text,
            kind="guide",
            identifier="journey",
        )

    before = snapshot(extra_files={"journey.md": guidance("Old advice.")})
    after = snapshot(extra_files={"journey.md": guidance("New advice.")})
    record = compare(before, after)
    assert record["state"] == "REVIEW"
    assert any(f["code"] == "guide-changed" for f in record["findings"])
    assert "Old advice" in record["baseline"]["guides"]["journey"]["markdown"]
    assert "New advice" in record["current"]["guides"]["journey"]["markdown"]
    assert sum(d["change"] == "prose-only" for d in record["deltas"]) == subject_count
    assert check_record(record, prepared=True) == 0


def test_guide_file_moves_preserve_effective_prose_and_change_raw_input_identity():
    from tests.documentation_fixtures import ROOT_TARGET

    raw = document([ROOT_TARGET], kind="guide", identifier="journey")
    before = snapshot(extra_files={"before.md": raw})
    after = snapshot(extra_files={"after.md": raw})
    record = compare(before, after)
    assert before["source_files"] != after["source_files"]
    assert before["guides"] == after["guides"]
    assert all(d["change"] == "unchanged" for d in record["deltas"])
    assert record["guide_deltas"][0]["change"] == "unchanged"


def test_audit_links_readable_removed_subjects_and_retained_guide_comparisons():
    from contract_atlas.documentation_rendering import augment
    from contract_atlas.html_rendering import _element_file, _shell, validate_render

    from tests.documentation_fixtures import ROOT_TARGET

    before = snapshot(
        extra_files={
            "journey.md": document(
                [ROOT_TARGET], kind="guide", identifier="journey", body="# Journey\n\nOld advice."
            )
        }
    )
    code = closure()
    code["elements"] = [e for e in code["elements"] if e["id"] != "schema:fixture"]
    after = snapshot(
        code,
        extra_files={
            "journey.md": document(
                [ROOT_TARGET], kind="guide", identifier="journey", body="# Journey\n\nNew advice."
            )
        },
    )
    record = compare(before, after)
    files = {
        "style.css": b"",
        "modes.js": b"",
        "manifest.json": b"{}",
        "index.html": _shell(
            "Contract",
            "<p>Unchanged native value.</p>",
            canonical_sha256(code),
            audit=False,
            documentation=False,
        ),
        _element_file("cli:fixture"): _shell(
            "CLI",
            "<p>Unchanged native value.</p>",
            canonical_sha256(code),
            audit=False,
            documentation=False,
        ),
    }
    augment(files, code, None, record)
    assert _element_file("schema:fixture") not in files
    removed = next(
        raw.decode()
        for name, raw in files.items()
        if name.startswith("documentation-audit-removed-")
    )
    assert "Removed native subject" in removed and "Retained removed meaning" in removed
    assert "limit" in removed and "4" in removed
    guide = next(
        raw.decode() for name, raw in files.items() if name.startswith("documentation-audit-guide-")
    )
    assert "Old advice" in guide and "New advice" in guide
    assert files["index.html"].count(b"Unchanged native value.") == 1
    validate_render({"riverhog-v1/" + name: raw for name, raw in files.items()})
