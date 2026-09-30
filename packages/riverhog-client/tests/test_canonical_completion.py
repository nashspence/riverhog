from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes, canonical_json_sha256
from riverhog_client.canonical_completion import CompletionRecord, build_completion_journal
from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.collection_completion_validation import validate_completion_preimages
from riverhog_protocol.collection_production_provenance import collection_production_contract
from riverhog_protocol.collection_record_preimages import (
    CollectionRecordPreimages,
    canonical_record_sequence,
)
from riverhog_protocol.collection_workflows import (
    ArtifactDisposition,
    ArtifactDispositionOutput,
    ArtifactDispositionSetIdentity,
)
from riverhog_provenance import reference, validate_journal
from riverhog_provenance_contracts import ContractCatalog


def _requirement() -> CollectionCompletionRequirementDocument:
    return CollectionCompletionRequirementDocument(
        execution_id="a" * 64,
        execution_envelope_sha256="b" * 64,
        controller_evidence_sha256="c" * 64,
        record_kinds=COMPLETION_REQUIRED_RECORD_KINDS,
    )


def _records() -> list[CompletionRecord]:
    raw = b'{"optional":null,"quality":1.2300}' + b"\n" * 150000
    return [
        CompletionRecord(name, len(raw), hashlib.sha256(raw).hexdigest(), lambda: (raw,))
        for name in COMPLETION_REQUIRED_RECORD_KINDS
    ]


def _completion(records: list[CompletionRecord]) -> bytes:
    return build_completion_journal(
        requirement=_requirement(),
        records=records,
        execution_sha256="d" * 64,
        output_bindings_sha256="e" * 64,
        input_history_bindings_sha256="f" * 64,
        disposition_set_sha256="0" * 64,
        producer_app="fixture",
        producer_version="1",
    )


def test_completion_retains_exact_sealed_preimages_and_uses_no_causal_generation() -> None:
    records = _records()
    summary = validate_journal(
        _completion(records), catalog=ContractCatalog((collection_production_contract(),))
    )
    assert summary.graph["activities"][0]["kind"] == "recording"
    assert not summary.graph.get("relations")
    subject = reference(summary.graph["activities"][0]["id"], "activity")
    with CollectionRecordPreimages(summary.graph["extensions"], subject=subject) as retained:
        retained.validate(expected_kinds=_requirement().record_kinds)
        assert b"".join(retained.chunks("target-execution")) == b"".join(records[-1].read())
    extensions = summary.graph["extensions"]
    fragment = next(row for row in extensions if row["property"].endswith("/record-fragment"))
    with CollectionRecordPreimages(
        (row for row in extensions if row is not fragment), subject=subject
    ) as retained:
        with pytest.raises(ValueError, match="contiguous|incomplete"):
            retained.validate(expected_kinds=_requirement().record_kinds)
    with pytest.raises(ValueError, match="duplicate"):
        with CollectionRecordPreimages((*extensions, fragment), subject=subject):
            pass


def test_completion_rejects_missing_or_changed_sealed_record() -> None:
    records = _records()
    with pytest.raises(ValueError, match="missing"):
        _completion(records[:-1])
    with pytest.raises(ValueError, match="sealed identity"):
        _completion([replace(records[0], sha256="0" * 64), *records[1:]])


def test_completion_checks_exact_output_import_and_disposition_correspondence() -> None:
    requirement = _requirement()
    disposition = ArtifactDisposition(1, "a" * 64, "b" * 64, "transformed")
    edge = ArtifactDispositionOutput(1, "a" * 64, "b" * 64, "c" * 64)
    identity = ArtifactDispositionSetIdentity(
        1,
        1,
        1,
        canonical_json_sha256(
            {
                "format": "riverhog-artifact-disposition-set/v1",
                "disposition_count": "1",
                "dispositions_sha256": hashlib.sha256(
                    canonical_json_bytes(disposition.as_dict()) + b"\n"
                ).hexdigest(),
                "output_edge_count": "1",
                "output_artifact_count": "1",
                "outputs_sha256": hashlib.sha256(
                    canonical_json_bytes(edge.as_dict()) + b"\n"
                ).hexdigest(),
            }
        ),
    )
    construction = {"format": "fixture-accepted-construction/v1", "optional": None}
    outputs = [{"artifact_id": "c" * 64, "state": {"exact": "accepted State"}}]
    imports = [{"artifact_id": "c" * 64, "imports": {"exact": "accepted extent"}}]
    controller = b'{"optional":null}'
    requirement = requirement.model_copy(
        update={"controller_evidence_sha256": hashlib.sha256(controller).hexdigest()}
    )
    preimages = {kind: b"{}" for kind in requirement.record_kinds}
    preimages.update(
        {
            "accepted-construction": canonical_json_bytes(construction),
            "controller": controller,
            "output-bindings": b"".join(canonical_record_sequence(outputs)),
            "input-history-bindings": b"".join(canonical_record_sequence(imports)),
            "disposition-identity": canonical_json_bytes(identity.as_dict()),
            "disposition-pages": b"".join(
                canonical_record_sequence(
                    (
                        {
                            "kind": "dispositions",
                            "page": {
                                "identity": identity.as_dict(),
                                "start_ordinal": "0",
                                "next_ordinal": None,
                                "dispositions": [disposition.as_dict()],
                            },
                        },
                        {
                            "kind": "outputs",
                            "page": {
                                "identity": identity.as_dict(),
                                "start_ordinal": "0",
                                "next_ordinal": None,
                                "outputs": [edge.as_dict()],
                            },
                        },
                    )
                )
            ),
        }
    )
    records = [
        CompletionRecord(
            kind, len(raw), hashlib.sha256(raw).hexdigest(), lambda content=raw: (content,)
        )
        for kind, raw in sorted(preimages.items())
    ]
    raw = build_completion_journal(
        requirement=requirement,
        records=records,
        execution_sha256=hashlib.sha256(preimages["target-execution"]).hexdigest(),
        output_bindings_sha256=hashlib.sha256(preimages["output-bindings"]).hexdigest(),
        input_history_bindings_sha256=hashlib.sha256(
            preimages["input-history-bindings"]
        ).hexdigest(),
        disposition_set_sha256=identity.sha256,
        producer_app="fixture",
        producer_version="1",
    )
    summary = validate_journal(raw, catalog=ContractCatalog((collection_production_contract(),)))
    fact = next(
        row
        for row in summary.graph["extensions"]
        if row["property"].endswith("/execution-completion")
    )
    arguments = dict(
        requirement=requirement,
        completion=fact["value"]["value"]["data"],
        extensions=summary.graph["extensions"],
        subject=fact["subject"],
        expected_construction=construction,
        expected_outputs=outputs,
        expected_imports=imports,
        disposition=identity,
    )
    validate_completion_preimages(**arguments)
    for changed in (
        {"expected_outputs": outputs + outputs},
        {"expected_imports": []},
        {"expected_construction": {"changed": True}},
    ):
        with pytest.raises(ValueError, match="correspondence|required record"):
            validate_completion_preimages(**{**arguments, **changed})

    assert json.loads(controller)["optional"] is None


def test_checkpointed_completion_reproduces_identical_bytes_after_restart() -> None:
    arguments = {
        "requirement": _requirement(),
        "records": _records(),
        "execution_sha256": "1" * 64,
        "output_bindings_sha256": "2" * 64,
        "input_history_bindings_sha256": "3" * 64,
        "disposition_set_sha256": "4" * 64,
        "producer_app": "fixture",
        "producer_version": "1",
        "journal_id": "urn:uuid:44444444-4444-4444-8444-444444444444",
        "recorded_at": "2026-09-30T00:00:00Z",
    }
    first = build_completion_journal(**arguments)
    assert build_completion_journal(**{**arguments, "records": _records()}) == first
    summary = validate_journal(first, require_profiles=False)
    assert all(
        frame.document["recorded_at"] == arguments["recorded_at"] for frame in summary.frames
    )
