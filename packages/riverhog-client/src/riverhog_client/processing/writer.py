"""Claim-bound publication of exactly one finalized derived collection."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import replace
from tempfile import TemporaryFile
from typing import Any, cast

from riverhog_archive_contracts import (
    HistoryJournalAnchor,
    MemberHistoryBuilder,
    MemberHistoryPrimary,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol import ArtifactId
from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionPublicationReceiptDocument,
    CollectionCompletionRecordingRequestDocument,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_MEMBER_ROLE,
    validate_member_completion_requirement,
)
from riverhog_protocol.collection_record_preimages import (
    canonical_record_sequence,
    completion_record_inventory_sha256,
)
from riverhog_protocol.collection_workflows import (
    ArtifactDispositionSetIdentity,
    CollectionDerivation,
    JsonValue,
    canonical_json_sha256,
)
from riverhog_provenance import (
    external_reference,
    selected_delivery_occurrence,
    software_agent_id,
    validate_journal_chunks,
)

from riverhog_client.canonical_completion import CompletionRecord, write_completion_journal
from riverhog_client.completion_records import CompletionRecords
from riverhog_client.processing.history_transfer import CanonicalHistoryTransfer
from riverhog_client.processing.models import (
    DerivedCollectionReceipt,
    DerivedCollectionSpec,
)
from riverhog_client.processing.provenance import ClaimedProvenance
from riverhog_client.producer import (
    IncrementalCollectionProducer,
    ProducerArtifactCustody,
    ProducerArtifactIdentity,
    ProducerInput,
)

type CompletionAssertions = Callable[[CompletionRecords, str, str], Iterable[Mapping[str, Any]]]


def _sha256(value: str, label: str) -> str:
    normalized = value.casefold()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return normalized


class DerivedCollectionWriter:
    """Publish one output collection through a scoped processing capability.

    The writer depends only on controller-sealed identities and evidence. It does
    not import an orchestration application, inspect contents, or choose archive
    object locations.
    """

    def __init__(
        self,
        api: Any,
        *,
        spec: DerivedCollectionSpec,
        claim_id: str,
        fence: int,
        work_id: str,
        execution_id: str,
        controller_evidence: Mapping[str, object],
        producer_app: str,
        producer_version: str = "development",
    ) -> None:
        if not claim_id or claim_id != claim_id.strip():
            raise ValueError("derived collection writer requires a canonical claim id")
        if isinstance(fence, bool) or fence < 1:
            raise ValueError("derived collection writer requires a positive fence")
        evidence = dict(controller_evidence)
        if not evidence:
            raise ValueError("derived collection writer requires controller evidence")
        self.api = api
        self.spec = spec
        self.claim_id = claim_id
        self.fence = fence
        self.work_id = _sha256(work_id, "work identity")
        self.execution_id = _sha256(execution_id, "execution identity")
        self.controller_evidence = evidence
        self.controller_evidence_sha256 = canonical_json_sha256(evidence)
        self.producer_app = producer_app
        self.producer_version = producer_version
        claim = api.get_processing_claim(claim_id)
        plan = claim.plan
        if plan is None or plan.execution_id != self.execution_id:
            raise ValueError("derived collection writer requires the sealed claim plan")
        if plan.output_policy != spec.output_policy:
            raise ValueError(
                "derived collection writer output policy differs from the sealed claim"
            )
        self.input_set_sha256 = plan.inputs.sha256
        self.artifact_set_sha256 = plan.artifacts.sha256

    def replace_api(self, api: Any) -> None:
        self.api = api

    def publish(
        self,
        outputs: Sequence[ProducerInput],
        *,
        identities: Mapping[ArtifactId, ProducerArtifactIdentity],
        source_histories: Mapping[ArtifactId, Iterable[ClaimedProvenance]],
        history_extent: str,
        completion_records: Iterable[CompletionRecord],
        execution_envelope_sha256: str,
        execution_sha256: str,
        disposition_set: ArtifactDispositionSetIdentity,
        source_context: Mapping[str, object] | None = None,
        poll_seconds: float = 2.0,
        timeout_seconds: float = 24 * 60 * 60,
    ) -> DerivedCollectionReceipt:
        if not outputs or len({value.artifact_id for value in outputs}) != len(outputs):
            raise ValueError("derived outputs must be nonempty and unique")
        if {value.artifact_id for value in outputs} != set(identities) or set(identities) != set(
            source_histories
        ):
            raise ValueError("derived output declarations lack exact input-history correspondence")
        writer = IncrementalDerivedCollectionWriter(
            self.api,
            spec=self.spec,
            claim_id=self.claim_id,
            fence=self.fence,
            work_id=self.work_id,
            execution_id=self.execution_id,
            controller_evidence=self.controller_evidence,
            producer_app=self.producer_app,
            producer_version=self.producer_version,
            execution_envelope_sha256=execution_envelope_sha256,
            source_context=source_context,
        )
        try:
            for output in outputs:
                if output.output_id is None:
                    raise ValueError("derived output has no accepted semantic output key")
                writer.append(
                    output,
                    identity=identities[output.artifact_id],
                    output_id=output.output_id,
                    source_histories=source_histories[output.artifact_id],
                    history_extent=history_extent,
                )
            return writer.finish(
                execution_sha256=execution_sha256,
                disposition_set=disposition_set,
                completion_records=completion_records,
                poll_seconds=poll_seconds,
                timeout_seconds=timeout_seconds,
            )
        finally:
            writer.stop()


class IncrementalDerivedCollectionWriter:
    """Publish exact derived artifacts as they finalize, then explicitly seal once."""

    def __init__(
        self,
        api: Any,
        *,
        spec: DerivedCollectionSpec,
        claim_id: str,
        fence: int,
        work_id: str,
        execution_id: str,
        controller_evidence: Mapping[str, object],
        producer_app: str,
        producer_version: str,
        execution_envelope_sha256: str,
        source_context: Mapping[str, object] | None = None,
    ) -> None:
        self.api = api
        self.producer_app = producer_app
        self.producer_version = producer_version
        self.spec = spec
        self.claim_id = claim_id
        self.fence = fence
        self.work_id = _sha256(work_id, "work identity")
        self.execution_id = _sha256(execution_id, "execution identity")
        self.controller_evidence = dict(controller_evidence)
        self.controller_evidence_sha256 = canonical_json_sha256(self.controller_evidence)
        self.execution_envelope_sha256 = _sha256(
            execution_envelope_sha256,
            "execution envelope identity",
        )
        claim = api.get_processing_claim(claim_id)
        plan = claim.plan
        if plan is None or plan.execution_id != self.execution_id:
            raise ValueError("incremental writer requires the sealed claim plan")
        if plan.output_policy != spec.output_policy:
            raise ValueError("incremental writer output policy differs from the sealed claim")
        self.input_set_sha256 = plan.inputs.sha256
        self.artifact_set_sha256 = plan.artifacts.sha256
        self.requirement = CollectionCompletionRequirementDocument(
            execution_id=self.execution_id,
            execution_envelope_sha256=self.execution_envelope_sha256,
            controller_evidence_sha256=self.controller_evidence_sha256,
            record_kinds=COMPLETION_REQUIRED_RECORD_KINDS,
        )
        self.producer = IncrementalCollectionProducer(
            api,
            producer_app=producer_app,
            adapter_id="riverhog-derived-collection/v1",
            adapter_version=producer_version,
            ingest_source=f"processing:{self.execution_id}",
            completion_requirement=self.requirement,
            source_event_id=self.execution_id,
            source_context={
                **dict(source_context or {}),
                "claim_id": claim_id,
                "fence": fence,
                "work_id": self.work_id,
                "execution_id": self.execution_id,
                "execution_envelope_sha256": self.execution_envelope_sha256,
            },
            idempotency_key=self.execution_id,
            archive_store=spec.output_policy.archive_store,
            use_cache=spec.output_policy.use_cache,
            copy_to=spec.output_policy.copy_to,
            tags=spec.output_policy.tags,
            event_context={
                "initiator": {
                    "app": producer_app,
                    "claim_id": claim_id,
                    "fence": fence,
                    "work_id": self.work_id,
                    "execution_id": self.execution_id,
                }
            },
        )

        # Reuse only authenticated destination-prefix comparisons. Each append
        # still selects and verifies its own exact, claim-scoped source history.
        self._history_transfer = CanonicalHistoryTransfer(self.api, self.producer.collection_id)

    def heartbeat(self) -> None:
        self.producer.heartbeat()

    def stop(self) -> None:
        self.producer.stop()

    def append(
        self,
        source: ProducerInput,
        *,
        identity: ProducerArtifactIdentity,
        output_id: str,
        source_histories: Iterable[ClaimedProvenance],
        history_extent: str,
    ) -> tuple[ProducerArtifactCustody, ...]:
        if source.artifact_id != identity.artifact_id:
            raise ValueError("incremental transform source artifact differs from its identity")
        # This bounded per-output declaration is provided by the selected target;
        # neither workspace names nor provenance lookup choose its causal inputs.
        imports = tuple(
            self._history_transfer.accept(value, extent=history_extent) for value in source_histories
        )
        if not imports:
            raise ValueError("a derived output requires explicitly selected input history")
        source = replace(
            source,
            output_id=output_id,
            causal_input_states=tuple(imported.input_state for imported in imports),
        )
        receipts = self.producer.append_inputs(
            [source], expected_identities={identity.artifact_id: identity}
        )
        primary = self.api.get_collection_upload_session_artifact_provenance_binding(
            self.producer.collection_id, identity.artifact_id
        )
        with MemberHistoryBuilder(
            artifact_id=str(identity.artifact_id),
            bytes=identity.bytes,
            sha256=identity.sha256,
            primary=MemberHistoryPrimary(
                HistoryJournalAnchor.from_mapping(primary.journal.model_dump(mode="json")),
                primary.delivery_association_id,
            ),
        ) as builder:
            for imported in imports:
                builder.add_import(imported)
            _, history = builder.seal()
            for page in builder.pages(history.imports):
                self.api.stage_collection_upload_session_history_structure(
                    self.producer.collection_id, page.to_json_bytes()
                )
            self.api.set_collection_upload_session_member_history_inputs(
                self.producer.collection_id, identity.artifact_id, history.imports
            )
        return (*receipts, *self.producer.reconcile_custody())

    def finish(
        self,
        *,
        execution_sha256: str,
        disposition_set: ArtifactDispositionSetIdentity,
        completion_records: Iterable[CompletionRecord],
        completion_assertions: CompletionAssertions | None = None,
        poll_seconds: float = 2.0,
        timeout_seconds: float = 24 * 60 * 60,
    ) -> DerivedCollectionReceipt:
        derivation = CollectionDerivation(
            execution_id=self.execution_id,
            claim_id=self.claim_id,
            fence=self.fence,
            recipe=self.spec.recipe,
            operation=self.spec.operation,
            input_set_sha256=self.input_set_sha256,
            artifact_set_sha256=self.artifact_set_sha256,
            execution_envelope_sha256=self.execution_envelope_sha256,
            execution_sha256=_sha256(execution_sha256, "execution evidence identity"),
            controller_evidence=cast(dict[str, JsonValue], self.controller_evidence),
            controller_evidence_sha256=self.controller_evidence_sha256,
            disposition_set=disposition_set,
        )
        self._seal_completion(
            execution_sha256, disposition_set, completion_records, completion_assertions
        )
        produced = self.producer.finish(
            poll_seconds=poll_seconds,
            timeout_seconds=timeout_seconds,
        )
        return DerivedCollectionReceipt(
            collection_id=produced.collection_id,
            archive_root_sha256=produced.archive_root_sha256,
            artifact_set_identity=produced.artifact_set_identity,
            derivation=derivation,
        )

    def _seal_completion(
        self,
        execution_sha256: str,
        disposition: ArtifactDispositionSetIdentity,
        supplied: Iterable[CompletionRecord],
        completion_assertions: CompletionAssertions | None,
    ) -> None:
        session = self.api.get_collection_upload_session(self.producer.collection_id)
        with CompletionRecords() as records:
            for record in supplied:
                retained = records.add(record.kind, record.read())
                if (retained.bytes, retained.sha256) != (record.bytes, record.sha256):
                    raise ValueError("sealed operation preimage changed before completion")
            records.add("controller", (canonical_json_bytes(self.controller_evidence),))
            records.add(
                "accepted-construction",
                (
                    canonical_json_bytes(
                        {
                            "format": "riverhog-execution-construction/v1",
                            "collection_id": str(self.producer.collection_id),
                            "construction_identity_sha256": session["construction_identity_sha256"],
                            "delivery_context_id": self.producer.delivery_context_id,
                            "completion_requirement": self.requirement.model_dump(mode="json"),
                        }
                    ),
                ),
            )
            records.add("disposition-identity", (canonical_json_bytes(disposition.as_dict()),))
            if records.record("target-execution").sha256 != execution_sha256:
                raise ValueError("target execution digest lacks its exact sealed preimage")
            if session["state"] == "finalized":
                receipt = CollectionCompletionPublicationReceiptDocument.model_validate(
                    session["completion_receipt"]
                )
                if receipt.requirement_sha256 != self.requirement.identity:
                    raise ValueError("published completion requirement differs from this execution")
                provided = {record.kind: record for record in records.records()}
                expected = set(receipt.records) - {
                    "disposition-pages",
                    "output-bindings",
                    "input-history-bindings",
                }
                if set(provided) != expected:
                    raise ValueError(
                        "published completion retry lacks its exact supplied preimages"
                    )
                for kind, record in provided.items():
                    accepted = receipt.records[kind]
                    if (record.sha256, str(record.bytes)) != (accepted.sha256, accepted.bytes):
                        raise ValueError("published completion retry changes accepted evidence")
                return
            records.add(
                "disposition-pages", canonical_record_sequence(self._disposition_pages(disposition))
            )
            self._record_output_bindings(records)
            records.add("output-bindings", canonical_record_sequence(records.output_rows()))
            records.add(
                "input-history-bindings",
                canonical_record_sequence(records.output_rows(imports=True)),
            )
            if records.record("target-execution").sha256 != execution_sha256:
                raise ValueError("target execution digest lacks its exact sealed preimage")
            recording = self.api.reserve_collection_upload_session_completion_recording(
                self.producer.collection_id,
                CollectionCompletionRecordingRequestDocument(
                    requirement_sha256=self.requirement.identity,
                    records_sha256=completion_record_inventory_sha256(
                        (record.kind, record.sha256, record.bytes) for record in records.records()
                    ),
                ),
            )
            with TemporaryFile("w+b") as journal:
                summary = write_completion_journal(
                    journal,
                    requirement=self.requirement,
                    records=records.records(),
                    journal_id=recording.journal_id,
                    recorded_at=recording.recorded_at,
                    late_assertions=(
                        completion_assertions(
                            records,
                            recording.journal_id,
                            software_agent_id(self.producer_app, self.producer_version),
                        )
                        if completion_assertions is not None
                        else ()
                    ),
                    execution_sha256=execution_sha256,
                    output_bindings_sha256=records.record("output-bindings").sha256,
                    input_history_bindings_sha256=records.record("input-history-bindings").sha256,
                    disposition_set_sha256=disposition.sha256,
                    producer_app=self.producer_app,
                    producer_version=self.producer_version,
                )
                self.api.upload_collection_upload_session_provenance_journal(
                    self.producer.collection_id,
                    summary.journal_id,
                    content=iter(lambda: journal.read(1024 * 1024), b""),
                    byte_count=summary.journal_bytes,
                    sha256=summary.journal_sha256,
                    selection_role="completion",
                )

    def _disposition_pages(
        self, identity: ArtifactDispositionSetIdentity
    ) -> Iterator[Mapping[str, Any]]:
        for kind, read in (
            ("dispositions", self.api.list_processing_claim_dispositions),
            ("outputs", self.api.list_processing_claim_disposition_outputs),
        ):
            ordinal = 0
            while True:
                page = read(self.claim_id, identity_sha256=identity.sha256, start_ordinal=ordinal)
                yield {"kind": kind, "page": page.model_dump(mode="json")}
                if page.next_ordinal is None:
                    break
                if page.next_ordinal <= ordinal:
                    raise ValueError("disposition continuation does not advance")
                ordinal = page.next_ordinal

    def _record_output_bindings(self, records: CompletionRecords) -> None:
        token = None
        while True:
            page = self.api.list_collection_upload_session_artifacts(
                self.producer.collection_id, page_size=100, page_token=token
            )
            for member in page["artifacts"]:
                artifact_id = ArtifactId(member["artifact_id"])
                primary = self.api.get_collection_upload_session_artifact_provenance_binding(
                    self.producer.collection_id, artifact_id
                )
                with self.api.stream_collection_upload_session_provenance_journal(
                    self.producer.collection_id, primary.journal.journal_id
                ) as chunks:
                    summary = validate_journal_chunks(
                        chunks,
                        expected_anchor=primary.journal.model_dump(mode="json"),
                        require_exact_tail=True,
                        require_profiles=False,
                    )
                state, _ = selected_delivery_occurrence(
                    summary,
                    binding=primary.model_dump(mode="json"),
                    artifact_id=str(artifact_id),
                    byte_count=int(member["bytes"]),
                    sha256=member["sha256"],
                    member_role=COLLECTION_MEMBER_ROLE,
                )
                output_id = validate_member_completion_requirement(
                    summary.graph_validation.view,
                    delivery_context_id=self.producer.delivery_context_id,
                    state_id=state["id"],
                    requirement=self.requirement,
                )
                imports = self.api.get_collection_upload_session_member_history_inputs(
                    self.producer.collection_id, artifact_id
                )
                records.add_output(
                    {
                        "artifact_id": str(artifact_id),
                        "bytes": str(member["bytes"]),
                        "sha256": member["sha256"],
                        "output_id": output_id,
                        "primary": {
                            "journal": primary.journal.model_dump(mode="json"),
                            "delivery_association_id": primary.delivery_association_id,
                        },
                        "state": external_reference(summary, state["id"]),
                    },
                    {"artifact_id": str(artifact_id), "imports": imports.model_dump(mode="json")},
                )
            token = page.get("next_page_token")
            if token is None:
                return


__all__ = ["DerivedCollectionWriter", "IncrementalDerivedCollectionWriter"]
