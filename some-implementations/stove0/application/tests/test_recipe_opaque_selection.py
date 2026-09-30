"""Exact member identity and fact-bound relation selection in recipes."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from stove0_core.recipes import _accepted_relationships, _document_matches_predicate, _subjects
from stove0_protocol import CollectionRootIdentityRef, WorkArtifactSubject
from stove0_recipe_config import (
    ArtifactAssociation,
    AssociationEvidenceSource,
    FactCondition,
    FactPredicate,
)


def _root() -> CollectionRootIdentityRef:
    return CollectionRootIdentityRef(
        collection_id="7",
        archive_root_sha256="1" * 64,
        artifact_set_identity="2" * 64,
    )


def _subject(member: str, role: str) -> WorkArtifactSubject:
    return WorkArtifactSubject(
        id="a-" + member * 32,
        role=role,
        collection=_root(),
        artifact_id=member * 64,
        bytes="4",
        sha256="f" * 64,
    )


def _source(contract: str) -> AssociationEvidenceSource:
    return AssociationEvidenceSource(
        observation_contract_id=contract,
        observation_contract_sha256="a" * 64,
        records_pointer="/rows",
        associated_pointer="/associated",
        primary_pointer="/primary",
        endpoint_mode="subject-id",
        primary_partition_pointer="/primary_subject_ids",
        associated_partition_pointer="/associated_subject_ids",
    )


def _evidence(
    contract: str,
    subjects: tuple[WorkArtifactSubject, ...],
    rows: list[dict[str, str]],
) -> Any:
    primary_ids = sorted(item.id for item in subjects if item.role == "fixture.primary/v1")
    associated_ids = sorted(item.id for item in subjects if item.role == "fixture.sidecar/v1")
    return SimpleNamespace(
        request=SimpleNamespace(
            observer_contract_id=contract,
            observer_contract_sha256="a" * 64,
            subjects=subjects,
            options={
                "primary_subject_ids": primary_ids,
                "associated_subject_ids": associated_ids,
            },
        ),
        result=SimpleNamespace(facts={"rows": rows}),
    )


def test_opaque_member_instances_never_collapse_by_equal_bytes() -> None:
    inventory = (
        {"collection": _root(), "artifact_id": "3" * 64, "bytes": 4, "sha256": "f" * 64},
        {"collection": _root(), "artifact_id": "4" * 64, "bytes": 4, "sha256": "f" * 64},
    )
    subjects = _subjects(inventory)
    assert len(subjects) == 2
    assert subjects[0].id != subjects[1].id
    assert {item.artifact_id for item in subjects} == {"3" * 64, "4" * 64}


def test_complete_negative_relation_allows_next_declared_source() -> None:
    primary = _subject("3", "fixture.primary/v1")
    sidecar = _subject("4", "fixture.sidecar/v1")
    subjects = (primary, sidecar)
    association = ArtifactAssociation(
        primary_role=primary.role,
        associated_roles=(sidecar.role,),
        sources=(_source("fixture.direct/v1"), _source("fixture.filename/v1")),
    )
    links, blocked = _accepted_relationships(
        (primary,),
        (sidecar,),
        association,
        (
            _evidence("fixture.direct/v1", subjects, []),
            _evidence(
                "fixture.filename/v1",
                subjects,
                [{"associated": sidecar.id, "primary": primary.id}],
            ),
        ),
    )
    assert links == {primary.id: [sidecar]}
    assert blocked == set()


def test_missing_or_partial_relation_evidence_cannot_be_a_negative() -> None:
    primary = _subject("3", "fixture.primary/v1")
    sidecar = _subject("4", "fixture.sidecar/v1")
    association = ArtifactAssociation(
        primary_role=primary.role,
        associated_roles=(sidecar.role,),
        sources=(_source("fixture.direct/v1"),),
    )
    with pytest.raises(ValueError, match="not accepted"):
        _accepted_relationships((primary,), (sidecar,), association, ())
    with pytest.raises(ValueError, match="subject partition|subject scope"):
        _accepted_relationships(
            (primary,),
            (sidecar,),
            association,
            (_evidence("fixture.direct/v1", (sidecar,), []),),
        )


def test_nested_metadata_rows_are_tested_without_position_or_filename_rules() -> None:
    document = {
        "subject_id": "a-" + "3" * 32,
        "facts": [
            {"name": "creator", "value": "Example"},
            {"name": "container-format", "value": "XMP"},
        ],
    }
    xmp = FactPredicate(
        observation_contract_id="fixture.metadata/v1",
        array_pointer="/facts",
        same_item=(FactCondition(pointer="/name", value="container-format"),),
        pointer="/value",
        operator="one-of",
        value=["XMP", "application/rdf+xml"],
    )
    assert _document_matches_predicate(xmp, document)
    assert not _document_matches_predicate(xmp, {**document, "facts": []})
    assert not _document_matches_predicate(
        xmp,
        {
            **document,
            "facts": [
                {"name": "creator", "value": "XMP"},
                {"name": "container-format", "value": "PNG"},
            ],
        },
    )
    with pytest.raises(ValueError, match="different shape"):
        _document_matches_predicate(xmp, {**document, "facts": {}})

    missing = FactPredicate(
        observation_contract_id="fixture.metadata/v1",
        array_pointer="/facts",
        pointer="/unknown",
        operator="exists",
        value=False,
    )
    assert _document_matches_predicate(missing, document)
