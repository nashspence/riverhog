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
    checked_contract_closure: dict[str, Any],
) -> None:
    closure, audit = build_records(checked_contract_closure["discovered"])

    validate_closure(closure)
    validate_audit_record(closure, audit)
    assert len(closure["elements"]) == 4255
    assert len(audit["element_overlays"]) == len(closure["elements"])
    assert set(closure["external_contract"]["extents"]) == {"format", "principles", "rules"}
    assert "requirement" not in closure["external_contract"]["extents"]["rules"]["schema-bound/v1"]
    assert len(audit["extent_analysis"]["decisions"]) == 2269


def test_audit_only_edit_does_not_change_closure(
    checked_contract_closure: dict[str, Any],
) -> None:
    original = checked_contract_closure["discovered"]
    changed = _changed(original)
    changed.root["trace"]["sources"][0]["source"]["path"] = "another-source.py"

    first_closure, first_audit = build_records(original)
    second_closure, second_audit = build_records(changed)

    assert first_closure == second_closure
    assert canonical_sha256(first_closure) == canonical_sha256(second_closure)
    assert first_audit != second_audit


def test_declared_promise_edit_changes_closure_and_invalidates_old_audit(
    checked_contract_closure: dict[str, Any],
) -> None:
    original = checked_contract_closure["discovered"]
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
    checked_contract_closure: dict[str, Any],
) -> None:
    closure, audit = build_records(checked_contract_closure["discovered"])
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
