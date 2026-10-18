from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from contract_atlas.model import (  # noqa: E402
    ContractAtlasError,
    DiscoveredContract,
    canonical_sha256,
)
from contract_atlas.records import (  # noqa: E402
    build_records,
    validate_audit_record,
    validate_closure,
)


def _changed(discovered: DiscoveredContract) -> DiscoveredContract:
    return DiscoveredContract(root=copy.deepcopy(discovered.root))


def test_full_candidate_partition_is_lossless_and_standalone(
    generated_contract_closure: dict[str, Any],
) -> None:
    closure, audit = build_records(generated_contract_closure["discovered"])

    validate_closure(closure)
    validate_audit_record(closure, audit)
    assert len(audit["element_overlays"]) == len(closure["elements"])
    assert set(closure["external_contract"]["extents"]) == {"format", "principles", "rules"}
    assert "requirement" not in closure["external_contract"]["extents"]["rules"]["schema-bound/v1"]


def test_audit_only_edit_does_not_change_closure(
    generated_contract_closure: dict[str, Any],
) -> None:
    original = generated_contract_closure["discovered"]
    changed = _changed(original)
    changed.root["trace"]["sources"][0]["source"]["path"] = "another-source.py"

    first_closure, first_audit = build_records(original)
    second_closure, second_audit = build_records(changed)

    assert first_closure == second_closure
    assert canonical_sha256(first_closure) == canonical_sha256(second_closure)
    assert first_audit != second_audit


def test_declared_promise_edit_changes_closure_and_invalidates_old_audit(
    generated_contract_closure: dict[str, Any],
) -> None:
    original = generated_contract_closure["discovered"]
    changed = _changed(original)
    changed.root["projection"]["external_contract"]["release"]["compatibility"]["cli"] += (
        " Changed."
    )

    first_closure, first_audit = build_records(original)
    second_closure, second_audit = build_records(changed)

    assert canonical_sha256(first_closure) != canonical_sha256(second_closure)
    validate_audit_record(second_closure, second_audit)
    with pytest.raises(ContractAtlasError, match="not bound"):
        validate_audit_record(second_closure, first_audit)


def test_tampered_audit_cannot_restore_or_rebind_fields(
    generated_contract_closure: dict[str, Any],
) -> None:
    closure, audit = build_records(generated_contract_closure["discovered"])
    tampered = copy.deepcopy(audit)
    tampered["extent_analysis"]["authoring_fields"]["schema-bound/v1"]["requirement"] = (
        "silent new condition"
    )
    with pytest.raises(ContractAtlasError, match="partition"):
        validate_audit_record(closure, tampered)

    tampered = copy.deepcopy(audit)
    tampered["element_overlays"][0]["title"] = "audit replacement"
    with pytest.raises(ContractAtlasError, match="replace a contract field"):
        validate_audit_record(closure, tampered)

    tampered = copy.deepcopy(audit)
    tampered["unclassified_field"] = "cannot silently disappear"
    with pytest.raises(ContractAtlasError, match="not bound"):
        validate_audit_record(closure, tampered)

    tampered = copy.deepcopy(audit)
    tampered["trace"]["segmented_extent_witnesses"][0]["subject_pointers"] = ["/absent"]
    with pytest.raises(ContractAtlasError, match="unbound contract subject"):
        validate_audit_record(closure, tampered)

    tampered = copy.deepcopy(audit)
    tampered["extent_analysis"]["decisions"][0]["source_pointer"] = "/absent"
    with pytest.raises(ContractAtlasError, match="partition"):
        validate_audit_record(closure, tampered)


def test_growth_implementation_relocation_changes_audit_only(generated_contract_closure):
    original = generated_contract_closure["discovered"]
    changed = _changed(original)
    row = next(iter(changed.root["trace"]["growth_registry"]["declarations"].values()))
    row["source"]["path"] = "relocated/owner.py"
    row["source"]["line"] += 100
    first_closure, first_audit = build_records(original)
    second_closure, second_audit = build_records(changed)
    assert first_closure == second_closure
    assert first_audit != second_audit


def test_real_primitive_execution_binds_to_audit_without_changing_closure(
    generated_contract_closure, executed_raw_source_growth
):
    from contract_atlas.records import bind_growth_execution

    discovered = _changed(generated_contract_closure["discovered"])
    # The isolated clean checkout contains the same current operation and test
    # bytes. Its Git identity belongs only in this Audit execution association.
    discovered.root["trace"]["growth_registry"]["source_identity"] = executed_raw_source_growth[
        "source"
    ]
    closure, audit = build_records(discovered)
    assert audit["growth_execution"] == {"status": "not-executed", "records": []}
    observed = bind_growth_execution(closure, audit, executed_raw_source_growth["records"])
    assert observed["growth_execution"]["status"] == "recorded"
    validate_audit_record(closure, observed)
    assert observed["closure_sha256"] == audit["closure_sha256"]
    assert audit["growth_execution"]["status"] == "not-executed"

    for defect in ("source", "digest", "delegate", "duplicate"):
        invalid = copy.deepcopy(observed)
        record = invalid["growth_execution"]["records"][0]
        if defect == "source":
            record["source"]["source_sha"] = "a" * 40
        elif defect == "digest":
            record["declaration"]["sha256"] = "a" * 64
        elif defect == "delegate":
            record["samples"][0]["delegates"] = {}
        else:
            invalid["growth_execution"]["records"].append(copy.deepcopy(record))
        with pytest.raises(ContractAtlasError, match="invalid execution"):
            validate_audit_record(closure, invalid)


def test_growth_promise_edit_invalidates_existing_audit(generated_contract_closure):
    closure, audit = build_records(generated_contract_closure["discovered"])
    changed = copy.deepcopy(closure)
    owner = changed["external_contract"]["growth"]["declarations"][
        "riverhog-client.raw-source-hashing.growth/v1"
    ]
    owner["bounds"]["payload_hash_input_octets"]["expression"]["terms"][0]["value"] = 3
    validate_closure(changed)
    assert canonical_sha256(changed) != canonical_sha256(closure)
    with pytest.raises(ContractAtlasError, match="not bound"):
        validate_audit_record(changed, audit)
