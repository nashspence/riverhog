from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from contract_atlas.model import ContractAtlasError, canonical_sha256  # noqa: E402
from contract_atlas.review import compare_records, summary  # noqa: E402


def _closure(value: object = 1) -> dict[str, Any]:
    return {
        "format": "riverhog-contract-closure/v1",
        "series": "v1",
        "elements": [
            {
                "id": "config:example:width",
                "authority": "example",
                "interface": "configuration",
                "title": "width",
                "pointers": ["/external_contract/example/width"],
            }
        ],
        "external_contract": {"example": {"width": value}},
    }


def _audit(closure: dict[str, Any], status: str = "unestablished") -> dict[str, Any]:
    return {
        "format": "riverhog-contract-audit-record/v1",
        "closure_sha256": canonical_sha256(closure),
        "extent_analysis": {"decisions": []},
        "qualification": status,
    }


def _compare(
    before: dict[str, Any], after: dict[str, Any], *, status: str = "unestablished"
) -> dict[str, Any]:
    return compare_records(
        before, after, _audit(before), _audit(after, status), base_sha="a" * 40, head_sha="b" * 40
    )


def test_value_only_change_is_visible_with_identical_ids_and_metadata() -> None:
    before, after = _closure(1), _closure(2)
    assert before["elements"] == after["elements"]
    report = _compare(before, after)
    assert report["contract_changed"]
    assert report["counts"] == {"added": 0, "changed": 1, "removed": 0}
    change = report["elements"][0]
    assert change["before"]["values"]["/external_contract/example/width"] == 1
    assert change["after"]["values"]["/external_contract/example/width"] == 2
    assert report["global_changes"][0]["unattributed"] is False


def test_json_boolean_and_number_changes_are_not_lost_to_python_equality() -> None:
    assert _compare(_closure(True), _closure(1))["contract_changed"]


def test_global_change_without_a_declared_owner_is_explicit() -> None:
    before = _closure()
    after = copy.deepcopy(before)
    after["external_contract"]["unowned"] = {"new": "value"}
    report = _compare(before, after)
    assert report["contract_changed"] and not report["elements"]
    assert report["global_changes"] == [
        {
            "pointer": "/external_contract/unowned",
            "change": "added",
            "after": {"new": "value"},
            "unattributed": True,
        }
    ]


def test_audit_only_change_does_not_claim_a_contract_change() -> None:
    before = _closure()
    report = _compare(before, before, status="passed")
    assert not report["contract_changed"] and report["audit_changed"]
    assert report["audit_changes"] == [
        {
            "pointer": "/qualification",
            "change": "changed",
            "before": "unestablished",
            "after": "passed",
        }
    ]


def test_bounded_summary_retains_all_machine_changes_and_escapes_untrusted_labels() -> None:
    before = _closure()
    after = copy.deepcopy(before)
    for index in range(60):
        after["external_contract"]["example"][f"extra{index}"] = index
        after["elements"].append(
            {
                "id": f"extra:<script>{index}</script>`",
                "authority": "example",
                "interface": "configuration",
                "title": "extra",
                "pointers": [f"/external_contract/example/extra{index}"],
            }
        )
    report = _compare(before, after)
    rendered = summary(report, limit=3)
    assert report["counts"]["added"] == 60 and len(report["elements"]) == 60
    assert "Showing 3 of 60" in rendered and "diff.json" in rendered
    assert "<script>" not in rendered and "&#96;" in rendered
    assert rendered.count("- added:") == 3


@pytest.mark.parametrize("mutation", ["format", "duplicate", "pointer", "audit_binding"])
def test_unsupported_or_malformed_baselines_never_produce_an_empty_success(mutation: str) -> None:
    before = _closure()
    after = _closure()
    audit = _audit(before)
    if mutation == "format":
        before["format"] = "unsupported/v0"
    elif mutation == "duplicate":
        before["elements"] *= 2
    elif mutation == "pointer":
        before["elements"][0]["pointers"] = ["/missing"]
    else:
        audit["closure_sha256"] = "0" * 64
    with pytest.raises(ContractAtlasError):
        compare_records(before, after, audit, _audit(after), base_sha="a" * 40, head_sha="b" * 40)


def test_native_contract_default_mutation_retains_full_owned_preimages(
    generated_contract_closure: dict[str, Any],
) -> None:
    before = generated_contract_closure["bundle"].closure
    after = copy.deepcopy(before)
    symbol = "gogurt_core.GOGURT_ROUTE_PATTERN"
    after["external_contract"]["python"][symbol]["contract"]["value"] += "changed"
    report = _compare(before, after)
    expected = next(element["id"] for element in before["elements"] if element["title"] == symbol)
    assert expected in {element["id"] for element in report["elements"]}
    assert report["contract_changed"]
