"""Exact prepared Native construction and replay through existing upload calls."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from pydantic import TypeAdapter
from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol import (
    ArtifactMaterializationDecisionBatchDocument,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingBatchDocument,
)
from riverhog_protocol.errors import NotFound
from riverhog_provenance import ObservationResult
from riverhog_provenance_contracts import ContractCatalog, ExternalReference, ProvenanceId

from riverhog_client._prepared_primary_documents import (
    ByteCommitment,
    ByteFile,
    PrimaryObservation,
    Sha256,
    primary_control_bytes,
)
from riverhog_client._prepared_primary_store import (
    PrimaryPreparation,
    PrimaryRecord,
    PrimaryStore,
)
from riverhog_client._prepared_primary_types import PrimaryStateCapacity, PrimaryStateError
from riverhog_client.canonical_production import (
    ProducerAttribution,
    build_member_journal,
    member_materialization_decision,
)
from riverhog_client.client import ApiClient

_REPLAY_BLOCK = 128 * 1024


@dataclass(frozen=True, slots=True)
class DeclaredPrimaryObservation:
    """An app-selected lazy observer with explicit stable semantic identity."""

    observer: str
    configuration: Mapping[str, Any]
    catalog_sha256: str
    factory: Callable[[], ObservationResult]


@dataclass(frozen=True, slots=True)
class RetainedPrimaryObservation:
    """A checkpoint repeats its original selection; it cannot invent missing preimages."""

    inputs: Mapping[str, Any]


def retained_primary_observation(
    observation: ObservationResult | DeclaredPrimaryObservation | RetainedPrimaryObservation | None,
) -> RetainedPrimaryObservation:
    """Freeze the three-mode descriptor without invoking a lazy factory."""
    if isinstance(observation, RetainedPrimaryObservation):
        selection = dict(observation.inputs)
    elif isinstance(observation, ObservationResult):
        selection = {"mode": "supplied", **_observation_identity(observation)}
    elif isinstance(observation, DeclaredPrimaryObservation):
        selection = {
            "mode": "lazy",
            "observer": observation.observer,
            "configuration": dict(observation.configuration),
            "catalog_sha256": observation.catalog_sha256,
        }
    else:
        selection = {
            "mode": "automatic",
            "observer": "riverhog-provenance/BoundedSourceObserver",
            "catalog_sha256": ContractCatalog().checkpoint_sha256(),
        }
    selection = json.loads(canonical_json_bytes(selection))
    mode = selection.get("mode")
    common = {"mode", "catalog_sha256"}
    identities = {
        "artifact_id",
        "occurrence_id",
        "state_id",
        "capture_id",
        "observation_id",
        "observer_agent_id",
    }
    if mode == "automatic":
        expected = common | {"observer"}
        if selection.get("observer") != "riverhog-provenance/BoundedSourceObserver":
            raise PrimaryStateError("automatic observation changed its declared observer")
    elif mode == "lazy":
        expected = common | {"observer", "configuration"}
        if not isinstance(selection.get("observer"), str) or not selection["observer"]:
            raise PrimaryStateError("lazy observation requires its original observer")
        if not isinstance(selection.get("configuration"), dict):
            raise PrimaryStateError("lazy observation requires its original configuration")
    elif mode == "supplied":
        expected = common | identities | {"graph"}
        for name in identities:
            TypeAdapter(ProvenanceId).validate_python(selection.get(name))
        ByteCommitment.model_validate(selection.get("graph"))
    else:
        raise PrimaryStateError("restored primary observation has no original selection mode")
    if set(selection) != expected:
        raise PrimaryStateError("observation selection has unknown or missing fields")
    TypeAdapter(Sha256).validate_python(selection.get("catalog_sha256"))
    return RetainedPrimaryObservation(selection)


def _observation_identity(observation: ObservationResult) -> dict[str, Any]:
    return {
        "artifact_id": observation.artifact_id,
        "occurrence_id": observation.occurrence_id,
        "state_id": observation.state_id,
        "capture_id": observation.capture_id,
        "observation_id": observation.observation_id,
        "observer_agent_id": observation.observer_agent_id,
        "catalog_sha256": observation.catalog.checkpoint_sha256(),
        "graph": {
            "bytes": str(len(observation.graph_json)),
            "sha256": hashlib.sha256(observation.graph_json).hexdigest(),
        },
    }


class PrimaryPublisher:
    """The shared producer's prepared-byte owner; payload/history keep their owners."""

    def __init__(self, api: ApiClient, store: PrimaryStore) -> None:
        self.api = api
        self.store = store

    def _require_open_session(self) -> None:
        retained = self.store.session
        current = self.api.get_collection_upload_session(retained.collection_id)
        if (
            current.get("state") != "open"
            or current.get("collection_id") != str(retained.collection_id)
            or current.get("delivery_context_id") != retained.delivery_context_id
            or current.get("construction_identity_sha256")
            != retained.server_construction_identity_sha256
        ):
            raise PrimaryStateError(
                "prepared-primary accepted open session is unavailable or changed"
            )

    def prepare(
        self,
        *,
        member: ArtifactMemberIdentityDocument,
        attribution: ProducerAttribution,
        observation: ObservationResult
        | DeclaredPrimaryObservation
        | RetainedPrimaryObservation
        | None,
        automatic_observation: Callable[[], ObservationResult],
        materialization_hint: tuple[str, ...] | None,
        allow_missing_materialization_hint: bool,
        causal_input_states: Sequence[Mapping[str, Any]] = (),
        output_id: str | None = None,
    ) -> PrimaryRecord:
        """Compare the full retry descriptor before observation, authorship or registration."""
        session = self.store.session
        requirement = attribution.completion_requirement
        if requirement is not None:
            requirement = type(requirement).model_validate_json(primary_control_bytes(requirement))
        requirement_identity = None if requirement is None else requirement.identity
        if (
            attribution.construction_identity != session.attribution_construction_identity_sha256
            or requirement_identity != session.completion_requirement_sha256
        ):
            raise PrimaryStateError(
                "prepared-primary attribution or completion requirement changed"
            )
        # Freeze mutable caller values before both comparison and Native authorship.
        source_context = json.loads(canonical_json_bytes(dict(attribution.source_context)))
        attribution = replace(
            attribution, source_context=source_context, completion_requirement=requirement
        )
        causal = tuple(
            ExternalReference.model_validate(value).model_dump(mode="json")
            for value in causal_input_states
        )
        decision = member_materialization_decision(
            artifact_id=member.artifact_id,
            materialization_hint=materialization_hint,
            allow_missing_materialization_hint=allow_missing_materialization_hint,
        )
        if isinstance(observation, ObservationResult):
            selection: dict[str, Any] = {"mode": "supplied", **_observation_identity(observation)}
            supplied = observation

            def observe() -> ObservationResult:
                return supplied

            catalog_pin = observation.catalog.checkpoint_sha256()
        elif isinstance(observation, DeclaredPrimaryObservation):
            if not observation.observer:
                raise ValueError("a lazy primary observer requires its stable identity")
            catalog_pin = observation.catalog_sha256
            selection = {
                "mode": "lazy",
                "observer": observation.observer,
                "configuration": dict(observation.configuration),
                "catalog_sha256": catalog_pin,
            }
            observe = observation.factory
        elif isinstance(observation, RetainedPrimaryObservation):
            selection = dict(retained_primary_observation(observation).inputs)
            if selection.get("mode") not in {"automatic", "supplied", "lazy"}:
                raise PrimaryStateError(
                    "restored primary observation has no original selection mode"
                )
            restored_pin = selection.get("catalog_sha256")
            if (
                not isinstance(restored_pin, str)
                or len(restored_pin) != 64
                or any(value not in "0123456789abcdef" for value in restored_pin)
            ):
                raise PrimaryStateError("restored primary observation has no exact catalog pin")
            catalog_pin = restored_pin

            def observe() -> ObservationResult:
                if selection["mode"] == "automatic":
                    return automatic_observation()
                raise PrimaryStateError(
                    "restored supplied or lazy observation has no original preparation preimages"
                )
        else:
            catalog_pin = ContractCatalog().checkpoint_sha256()
            selection = {
                "mode": "automatic",
                "observer": "riverhog-provenance/BoundedSourceObserver",
                "catalog_sha256": catalog_pin,
            }
            observe = automatic_observation
        request = canonical_json_bytes(
            {
                "format": "riverhog-prepared-primary-inputs/v1",
                "member": member.model_dump(mode="json"),
                "producer": {
                    "producer_app": attribution.producer_app,
                    "adapter_id": attribution.adapter_id,
                    "adapter_version": attribution.adapter_version,
                    "source_event_id": attribution.source_event_id,
                    "ingest_source": attribution.ingest_source,
                    "source_context": source_context,
                    "construction_identity": attribution.construction_identity,
                    "completion_requirement": None
                    if requirement is None
                    else requirement.model_dump(mode="json"),
                },
                "observation": selection,
                "output_id": output_id,
                "causal_input_states": list(causal),
                "materialization_hint": None
                if materialization_hint is None
                else list(materialization_hint),
                "allow_missing_materialization_hint": allow_missing_materialization_hint,
            }
        )

        def author(admission: int) -> PrimaryPreparation:
            # A 404 alone might name a missing session, not an absent member.
            self._require_open_session()
            try:
                self.api.get_collection_upload_session_artifact(
                    session.collection_id, member.artifact_id
                )
            except NotFound:
                pass
            else:
                raise PrimaryStateError("remote member has no original local preparation")
            selected = observe()
            if selected.catalog.checkpoint_sha256() != catalog_pin:
                raise PrimaryStateError("primary observer returned another contract catalog")
            graph = selected.graph_json
            observation_control = PrimaryObservation.model_validate(
                {
                    **{
                        key: value
                        for key, value in _observation_identity(selected).items()
                        if key != "graph"
                    },
                    "graph": ByteFile.model_validate(
                        {
                            "name": "graph",
                            "bytes": str(len(graph)),
                            "sha256": hashlib.sha256(graph).hexdigest(),
                        }
                    ),
                }
            )
            selected_bytes = primary_control_bytes(observation_control)
            decision_bytes = primary_control_bytes(decision)
            if len(graph) + len(selected_bytes) + len(decision_bytes) > admission:
                raise PrimaryStateCapacity("primary observation exceeds preparation admission")
            produced = build_member_journal(
                member=member,
                observation=selected,
                delivery_context_id=session.delivery_context_id,
                attribution=attribution,
                materialization_hint=materialization_hint,
                causal_input_states=causal,
                output_id=output_id,
            )
            return PrimaryPreparation(
                selected_bytes,
                graph,
                produced.content,
                decision_bytes,
                produced.binding,
                requirement_identity,
            )

        return self.store.prepare_or_load(member, request, author)

    def replay(self, record: PrimaryRecord) -> PrimaryRecord:
        """Use only the retained winner; never author a replacement after uncertainty."""
        current = self.store.load(record.prepared.member)
        if current is None or current.prepared != record.prepared:
            raise PrimaryStateError("prepared-primary replay has no retained winner")
        record = current
        if record.decision_accepted is not None:
            # This immutable acknowledgement already settles primary and decision replay.
            return record
        self._require_open_session()
        collection_id = self.store.session.collection_id
        prepared = record.prepared
        try:
            accepted = self.api.get_collection_upload_session_artifact_provenance_binding(
                collection_id, prepared.member.artifact_id
            )
        except NotFound:
            accepted = None
        if accepted is not None and accepted != prepared.primary:
            raise PrimaryStateError("remote primary differs from the entire retained binding")
        if accepted is None:
            with self.store.byte_reader(record, "journal") as stream:
                self.api.upload_collection_upload_session_provenance_journal(
                    collection_id,
                    prepared.primary.journal.journal_id,
                    content=iter(lambda: stream.read(_REPLAY_BLOCK), b""),
                    byte_count=prepared.journal.bytes,
                    sha256=prepared.journal.sha256,
                )
            requested = CollectionArtifactProvenanceBindingBatchDocument(
                bindings=[prepared.primary]
            )
            bound = self.api.bind_collection_upload_session_artifact_provenance(
                collection_id, requested
            )
            if bound != requested:
                raise PrimaryStateError("primary binding response differs from the retained winner")
        with self.store.byte_reader(record, "decision") as stream:
            decision = ArtifactMaterializationDecisionBatchDocument.model_validate_json(
                stream.read()
            )
        response = self.api.set_collection_upload_session_materialization_decisions(
            collection_id, decision
        )
        if response != decision:
            raise PrimaryStateError("materialization response differs from the retained decision")
        return self.store.accept_decision(record, primary_control_bytes(response))
