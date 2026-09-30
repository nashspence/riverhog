from __future__ import annotations

import hashlib
from collections.abc import Iterator

import pytest
from a_riverhog_direct_relations_contract_lib import DESCRIBES, RECONSTRUCTION_SUPPORT_FOR
from riverhog_client.canonical_completion import CompletionRecord, build_completion_journal
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_client.completion_records import CompletionRecords
from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.collection_record_preimages import canonical_record_sequence
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    CanonicalCorpusValidator,
    external_reference,
    new_id,
    software_agent_id,
    validate_journal,
)
from stove0_target_protocol import OutputArtifact
from stove0_target_support.relationships import completion_output_relationships


def _outputs() -> tuple[OutputArtifact, ...]:
    return tuple(
        OutputArtifact.model_validate(
            {
                "id": key,
                "role": "fixture.output/v1",
                "artifact_id": format(ordinal + 1, "064x"),
                "bytes": "1",
                "sha256": hashlib.sha256(b"x").hexdigest(),
                **relation,
            }
        )
        for ordinal, (key, relation) in enumerate(
            (
                ("bundle", {"reconstructs_output_id": "media"}),
                ("media", {}),
                ("xmp", {"describes_output_id": "media"}),
            )
        )
    )


def _spool(records: CompletionRecords, products: tuple[OutputArtifact, ...]) -> Iterator[bytes]:
    for product in products:
        primary = build_member_journal(
            member=ArtifactMemberIdentityDocument.model_validate(
                product.model_dump(mode="json", include={"artifact_id", "bytes", "sha256"})
            ),
            observation=BoundedSourceObserver().observe(BytesSource(b"x")),
            delivery_context_id=new_id(),
            attribution=ProducerAttribution(
                "fixture-target", "fixture", "1", "event", "fixture", {}, "a" * 64
            ),
            materialization_hint=None,
        )
        summary = validate_journal(primary.content, require_profiles=False)
        records.add_output(
            {
                "output_id": product.id,
                "artifact_id": str(product.artifact_id),
                "bytes": str(product.bytes),
                "sha256": product.sha256,
                "state": external_reference(summary, summary.states[0]["id"]),
            },
            {"artifact_id": str(product.artifact_id), "imports": {}},
        )
        yield primary.content
    records.add(
        "target-output-declarations",
        canonical_record_sequence(
            item.model_dump(mode="json", exclude_none=True) for item in products
        ),
    )


def test_late_output_relations_use_exact_states_and_leave_primary_journals_unchanged() -> None:
    products = _outputs()
    journal_id = new_id()
    agent = software_agent_id("fixture-target", "1")
    requirement = CollectionCompletionRequirementDocument(
        execution_id="a" * 64,
        execution_envelope_sha256="b" * 64,
        controller_evidence_sha256="c" * 64,
        record_kinds=COMPLETION_REQUIRED_RECORD_KINDS,
    )
    with CompletionRecords() as records:
        primaries = tuple(_spool(records, products))
        sealed = [
            CompletionRecord.from_bytes(kind, b"{}")
            for kind in requirement.record_kinds
            if kind != "target-output-declarations"
        ]
        sealed.append(records.record("target-output-declarations"))
        arguments = dict(
            requirement=requirement,
            records=sealed,
            journal_id=journal_id,
            recorded_at="2026-01-01T00:00:00Z",
            execution_sha256="d" * 64,
            output_bindings_sha256="e" * 64,
            input_history_bindings_sha256="f" * 64,
            disposition_set_sha256="0" * 64,
            producer_app="fixture-target",
            producer_version="1",
        )
        complete = build_completion_journal(
            **arguments,
            late_assertions=completion_output_relationships(records, journal_id, agent),
        )
        retry = build_completion_journal(
            **arguments,
            late_assertions=completion_output_relationships(records, journal_id, agent),
        )
        assert complete == retry
        summary = validate_journal(complete, require_profiles=False)
        claims = [
            row
            for row in summary.graph["extensions"]
            if row["property"]
            in {
                DESCRIBES,
                RECONSTRUCTION_SUPPORT_FOR,
            }
        ]
        assert len(claims) == 2
        assert {row["property"] for row in claims} == {DESCRIBES, RECONSTRUCTION_SUPPORT_FOR}
        for row in claims:
            assert row["subject"]["scope"] == "external"
            assert row["value"]["value"] == records.output_for_key("media")["state"]
        assert not summary.graph.get("relations")
        assert all(row["kind"] == "recording" for row in summary.graph["activities"])
        with CanonicalCorpusValidator() as corpus:
            for primary in primaries:
                corpus.add(validate_journal(primary, require_profiles=False))
            corpus.add(summary)
            corpus.validate()


@pytest.mark.parametrize("change", ["omitted", "duplicate", "substituted", "foreign-key"])
def test_late_output_relations_reject_incomplete_or_substituted_maps(change: str) -> None:
    products = _outputs()
    with CompletionRecords() as records:
        tuple(_spool(records, products))
        if change == "omitted":
            records._db.execute("DELETE FROM outputs WHERE output_id = 'xmp'")
        elif change == "duplicate":
            records._db.execute("DELETE FROM records WHERE kind = 'target-output-declarations'")
            records.add(
                "target-output-declarations",
                canonical_record_sequence(item.model_dump(mode="json") for item in products * 2),
            )
        elif change == "substituted":
            records._db.execute("UPDATE outputs SET output_id = 'other' WHERE output_id = 'media'")
        else:
            records._db.execute("DELETE FROM records WHERE kind = 'target-output-declarations'")
            records.add(
                "target-output-declarations",
                canonical_record_sequence(
                    {**item.model_dump(mode="json"), "describes_output_id": "outside-operation"}
                    if item.id == "xmp"
                    else item.model_dump(mode="json")
                    for item in products
                ),
            )
        with pytest.raises(ValueError, match="unproduced|duplicated|out of order"):
            tuple(
                completion_output_relationships(
                    records, new_id(), software_agent_id("fixture", "1")
                )
            )
