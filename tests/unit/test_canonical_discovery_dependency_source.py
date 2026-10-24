from __future__ import annotations

import pytest
from riverhog_core.canonical_discovery_journal_work import QualifiedJournalSource, StaleJournalTask
from riverhog_provenance import (
    ProvenanceValidationError,
    append_assertion_batches,
    append_correction,
    assertion,
    assertion_reference,
    external_reference,
    new_id,
    reference,
    validate_journal,
)

from tests.support.canonical_discovery import native_scalar_fixture
from tests.support.canonical_discovery_work import ADMISSION


@pytest.fixture(scope="module")
def dependency_source():
    fixture = native_scalar_fixture(3)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as first:
        who = first.frames[0].document["recorded_by_agent_id"]
        artifact = first.graph["artifacts"][0]["id"]
    row = assertion(
        "extension",
        who,
        subject=reference(artifact, "artifact"),
        property="urn:test:admitted-documentary-dependency",
        value={"type": "text", "value": "historical"},
    )
    selected = append_assertion_batches(
        fixture.raw, ({"extensions": [row]},), recorded_by_agent_id=who, catalog=fixture.catalog
    )
    with validate_journal(selected, catalog=fixture.catalog) as before:
        dependency = external_reference(before, row["id"])
        target = assertion_reference(before, row["assertion_id"])
        anchor = before.anchor
    complete = append_correction(
        selected, (target,), reason="withdrawn", recorded_by_agent_id=who, catalog=fixture.catalog
    )
    return fixture, complete, dependency, anchor


def test_dependency_uses_native_original_address_inside_admitted_source(dependency_source):
    fixture, raw, dependency, expected = dependency_source
    with validate_journal(raw, catalog=fixture.catalog) as native:
        source = QualifiedJournalSource(native, ADMISSION)
    with source:
        assert source.reference_anchor(dependency) == expected
        assert (
            source.reference_for_assertion(
                dependency["assertion_id"],
                object_id=dependency["object_id"],
                object_type=dependency["object_type"],
            )
            == dependency
        )
        assert source.anchor_at(1) == expected
        changed = source.reference_anchor(dependency)
        changed["through"]["entry_id"] = "caller view"
        assert source.reference_anchor(dependency) == expected
    with pytest.raises(StaleJournalTask, match="closed"):
        source.reference_anchor(dependency)


def test_dependency_after_selected_source_terminal_is_rejected(dependency_source):
    fixture, raw, dependency, expected = dependency_source
    with validate_journal(raw, catalog=fixture.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION, through=0) as source:
            with pytest.raises(StaleJournalTask, match="exceeds"):
                source.reference_anchor(dependency)
            with pytest.raises(StaleJournalTask, match="exceeds"):
                source.reference_for_assertion(
                    dependency["assertion_id"],
                    object_id=dependency["object_id"],
                    object_type=dependency["object_type"],
                )
        with QualifiedJournalSource(native, ADMISSION, through=1) as source:
            assert source.reference_anchor(dependency) == expected


@pytest.mark.parametrize("fault", ["object", "type", "closed"])
def test_native_original_reference_construction_checks_exact_identity_and_lifetime(
    dependency_source, fault
):
    fixture, raw, dependency, _ = dependency_source
    with validate_journal(raw, catalog=fixture.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION) as source:
            if fault == "closed":
                source.close()
            with pytest.raises(
                StaleJournalTask if fault == "closed" else ProvenanceValidationError
            ):
                source.reference_for_assertion(
                    dependency["assertion_id"],
                    object_id=new_id() if fault == "object" else dependency["object_id"],
                    object_type="state" if fault == "type" else dependency["object_type"],
                )
