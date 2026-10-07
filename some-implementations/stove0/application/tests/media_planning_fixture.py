"""Current media witnesses use genuine observer contracts and one compiled planner."""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

from a_stove0_ffprobe_streams_contract_lib import FFPROBE_STREAMS_SEMANTIC_VALIDATOR
from a_stove0_filename_prefix_sidecar_evidence_contract_lib import FILENAME_SEMANTIC_VALIDATOR
from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_SEMANTIC_VALIDATOR,
)
from a_stove0_media_archive_contract_lib import AUDIO_ARCHIVE_OPERATION, AV1_OPUS_ARCHIVE_OPERATION
from a_stove0_media_metadata_contract_lib import (
    MEDIA_METADATA_SEMANTIC_VALIDATOR,
    MediaArtifactFacts,
    MediaFactEvidence,
    MediaMetadataFact,
    MediaMetadataFacts,
)
from a_stove0_riverhog_provenance_evidence_contract_lib import CORE_PROVENANCE_SEMANTIC_VALIDATOR
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    PortableCollectionHeader,
    PortableCollectionInventoryAuthority,
    PortableCollectionInventoryPage,
)
from riverhog_protocol.errors import NotFound
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_core.recipes import RecipePlanner
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    SemanticValidatorRegistry,
)
from stove0_observer_support import ContentObservationResultBuilder
from stove0_protocol import CollectionRootIdentityRef, JsonSchemaValidationProfile
from stove0_recipe_config import CompiledRecipeCatalog, load_recipe_catalog
from stove0_target_protocol import TargetDescriptor, TargetDescriptorPayload, TargetOperationSupport


def _sha(c):
    return c * 64


_PAYLOAD_SHA = hashlib.sha256(b"abc").hexdigest()

_FIXTURE_NAMES = {
    f"{1:064x}": "camera/source.mp4",
    f"{2:064x}": "camera/source.json",
    f"{3:064x}": "audio/take.XMP",
    f"{4:064x}": "audio/take.wav",
    f"{5:064x}": "audio/take.xmp",
    f"{6:064x}": "notes/retained.txt",
    f"{7:064x}": "video/clip.mov",
    f"{8:064x}": "video/clip.xmp",
    f"{9:064x}": "camera/clip.mov",
    f"{10:064x}": "camera/clip.xmp",
    f"{11:064x}": "camera/retained.txt",
}


class CatalogApi:
    def get_collection_derivation(self, collection_id: int) -> dict[str, object]:
        assert collection_id == 11
        raise NotFound("collection has no derivation")

    def get_collection(self, collection_id: int) -> dict[str, object]:
        assert collection_id == 11
        return {
            "id": 11,
            "archive_root_sha256": _sha("1"),
            "artifact_set_identity": _sha("2"),
        }

    def search(self, **_kwargs: object) -> dict[str, object]:
        return {
            "artifacts": [
                {
                    "artifact_id": f"{1:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                }
            ]
        }

    def get_portable_collection_inventory(
        self,
        collection_id: int,
        **kwargs: object,
    ) -> PortableCollectionInventoryPage:
        assert collection_id == 11
        assert kwargs["cursor"] is None
        files = self.search(**kwargs).get("artifacts")
        if not isinstance(files, list):
            raise AssertionError("fixture search inventory must be a list")
        files = sorted(files, key=lambda item: str(item["artifact_id"]).encode("utf-8"))
        return PortableCollectionInventoryPage(
            authority=PortableCollectionInventoryAuthority(
                header=PortableCollectionHeader(
                    collection="11",
                    artifact_set_identity=_sha("2"),
                    encryption_format="age-v1-scrypt",
                    passphrase_id="fixture-archive-key-v1",
                    provenance_identity=_sha("d"),
                ),
                inventory_identity=_sha("a"),
                artifact_count=str(len(files)),
                artifact_bytes=str(sum(int(item["bytes"]) for item in files)),
            ),
            artifacts=[
                ArtifactMemberIdentityDocument.model_validate({**item, "bytes": str(item["bytes"])})
                for item in files
            ],
            complete=True,
        )


class LargeCatalogApi(CatalogApi):
    def search(self, **_kwargs: object) -> dict[str, object]:
        return {
            "artifacts": [
                {
                    "artifact_id": f"{index + 100:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                }
                for index in range(257)
            ]
        }


class ConformanceCatalogApi(CatalogApi):
    def search(self, **_kwargs: object) -> dict[str, object]:
        return {
            "artifacts": [
                {
                    "artifact_id": f"{3:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
                {
                    "artifact_id": f"{4:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
                {
                    "artifact_id": f"{5:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
                {
                    "artifact_id": f"{6:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
                {
                    "artifact_id": f"{7:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
                {
                    "artifact_id": f"{8:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
            ]
        }


class BatchMediaObservers:
    """Accepted fixture measurements; real contracts and relation interpretation."""

    def __init__(self, batch_size: int, *, capture_time: str | None = None) -> None:
        from a_stove0_exiftool_observer import ExiftoolObserver
        from a_stove0_ffprobe_observer import FfprobeObserver
        from a_stove0_filename_prefix_sidecar_observer import FilenamePrefixSidecarObserver
        from a_stove0_riverhog_provenance_observer import RiverhogProvenanceObserver

        self.batch_size = batch_size
        self.capture_time = capture_time
        image = "sha256:" + _sha("9")
        self.filename = FilenamePrefixSidecarObserver(image_id=image)
        provenance = RiverhogProvenanceObserver(image_id=image).descriptor()
        self.descriptors = {
            "exiftool": ExiftoolObserver(image_id=image).descriptor(),
            "ffprobe-streams": FfprobeObserver(image_id=image).descriptor(),
            "canonical-provenance": provenance,
            "canonical-hint": provenance,
            "filename-prefix-sidecars": self.filename.descriptor(),
        }
        self.value = self.descriptors["exiftool"]
        self.canonical = {}
        for registration, descriptor in self.descriptors.items():
            if registration == "filename-prefix-sidecars":
                continue
            payload = descriptor.model_dump(mode="json", exclude={"descriptor_sha256"})
            for support in payload["contracts"]:
                support["preferred_subject_batch_size"] = self.batch_size
            self.descriptors[registration] = ObserverDescriptor.seal(
                ObserverDescriptorPayload.model_validate(payload)
            )

    def registration_ids(self):
        return tuple(self.descriptors)

    def semantic_validators(self, _registration):
        return SemanticValidatorRegistry(
            (
                MEDIA_METADATA_SEMANTIC_VALIDATOR,
                FFPROBE_STREAMS_SEMANTIC_VALIDATOR,
                MATERIALIZATION_HINT_SEMANTIC_VALIDATOR,
                CORE_PROVENANCE_SEMANTIC_VALIDATOR,
                FILENAME_SEMANTIC_VALIDATOR,
            )
        )

    def descriptor(self, registration_id: str) -> ObserverDescriptor:
        return self.descriptors[registration_id]

    def canonical_facts(self, subject):
        from a_stove0_riverhog_provenance_observer import extract_core_facts
        from riverhog_protocol import CollectionArtifactProvenanceBindingDocument
        from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
        from riverhog_provenance import (
            BoundedSourceObserver,
            BytesSource,
            assertion,
            create_journal,
            reference,
            validate_journal,
        )
        from riverhog_provenance_contracts import SOURCE_NAMING_VIEW_SCHEME

        from tests.support.member_history import member_history_fixture

        snapshot = self.canonical.get(subject.artifact_id)
        if snapshot is None:
            observed = BoundedSourceObserver().observe(BytesSource(b"abc"))
            graph = observed.graph_fragment()
            graph["descriptions"][0]["address_status"] = "known"
            delivery = assertion("context", observed.observer_agent_id, kind="delivery")
            namespace = assertion(
                "context",
                observed.observer_agent_id,
                kind="filesystem_namespace",
                identifiers=[
                    {
                        "scheme": SOURCE_NAMING_VIEW_SCHEME,
                        "scope": "global",
                        "value": {
                            "kind": "text",
                            "text": "urn:uuid:11111111-1111-4111-8111-111111111111",
                        },
                    }
                ],
            )
            graph["contexts"] = [namespace, delivery]
            graph["locator_bindings"] = [
                assertion(
                    "locator_binding",
                    observed.observer_agent_id,
                    target=reference(observed.state_id, "state"),
                    context_id=namespace["id"],
                    locator={
                        "kind": "filesystem_path",
                        "syntax": "posix",
                        "form": "absolute",
                        "name": {
                            "kind": "text",
                            "text": "/fixture/" + _FIXTURE_NAMES[subject.artifact_id],
                        },
                    },
                    temporal_scope={"kind": "unknown", "reason": "fixture source view"},
                    observation_id=observed.observation_id,
                )
            ]
            association = assertion(
                "delivery_association",
                observed.observer_agent_id,
                delivery_context_id=delivery["id"],
                slot={"kind": "text", "text": str(subject.artifact_id)},
                role=COLLECTION_MEMBER_ROLE,
                state=reference(observed.state_id, "state"),
                verification_observation_id=observed.observation_id,
            )
            graph["delivery_associations"] = [association]
            summary = validate_journal(
                create_journal(graph, recorded_by_agent_id=observed.observer_agent_id)
            )
            binding = CollectionArtifactProvenanceBindingDocument.model_validate(
                {
                    "artifact_id": str(subject.artifact_id),
                    "journal": summary.anchor,
                    "delivery_association_id": association["id"],
                }
            )
            snapshot = self.canonical[subject.artifact_id] = (binding, summary)
        binding, summary = snapshot
        return extract_core_facts(
            subject,
            binding,
            summary,
            history=member_history_fixture(subject, binding),
            selected_summaries=(summary,),
        )

    def facts(self, request, accepted):
        if request.observer_registration_id == "exiftool":
            return _metadata_facts(request, capture_time=self.capture_time)
        if request.observer_registration_id == "ffprobe-streams":
            from a_stove0_ffprobe_streams_contract_lib import artifact_facts
            from riverhog_canonical_json import canonical_json_bytes

            rows = []
            for subject in request.subjects:
                name = _FIXTURE_NAMES[subject.artifact_id]
                streams = []
                if name in {"audio/take.wav", "video/clip.mov", "camera/clip.mov"}:
                    streams.append({"index": 0, "codec_type": "audio", "codec_name": "pcm_s16le"})
                if name == "video/clip.mov":
                    streams.append(
                        {
                            "index": 1,
                            "codec_type": "video",
                            "codec_name": "h264",
                            "width": 1280,
                            "height": 720,
                        }
                    )
                report = canonical_json_bytes(
                    {"streams": streams, "format": {"format_name": "fixture"}}
                )
                rows.append(
                    artifact_facts(
                        subject.id,
                        report,
                        ffprobe_version="fixture-ffprobe-1",
                        executable_sha256=_sha("f"),
                    ).model_dump(mode="json")
                )
            return {"artifacts": rows}
        if request.observer_registration_id == "canonical-provenance":
            return {"artifacts": [self.canonical_facts(subject) for subject in request.subjects]}
        if request.observer_registration_id == "canonical-hint":
            return {
                "artifacts": [
                    {
                        "subject_id": subject.id,
                        "primary_binding": (fact := self.canonical_facts(subject))[
                            "primary_binding"
                        ],
                        "occurrence": {"scope": "external", **fact["occurrence"]},
                        "materialization_hint": None,
                    }
                    for subject in request.subjects
                ]
            }
        raise AssertionError(request.observer_registration_id)


def _metadata_facts(request, *, capture_time=None):
    rows = []
    for subject in request.subjects:
        name = _FIXTURE_NAMES.get(subject.artifact_id, "unknown")
        kind = (
            "XMP"
            if name.endswith((".XMP", ".xmp"))
            else "MOV"
            if name.endswith(".mov")
            else "WAV"
            if name.endswith(".wav")
            else "MP4"
            if name.endswith(".mp4")
            else "TXT"
        )
        facts = [
            MediaMetadataFact(
                name="container-format",
                value=kind,
                evidence=MediaFactEvidence(artifact_id=subject.id, field="File:FileType"),
            )
        ]
        if capture_time is not None:
            facts.append(
                MediaMetadataFact(
                    name="capture-time",
                    value=capture_time,
                    evidence=MediaFactEvidence(artifact_id=subject.id, field="XMP-xmp:CreateDate"),
                )
            )
        rows.append(
            MediaArtifactFacts(
                artifact_id=subject.id,
                state="observed",
                facts=tuple(sorted(facts, key=lambda item: item.name)),
            )
        )
    return MediaMetadataFacts(artifacts=tuple(rows)).model_dump(mode="json")


class ConformanceTargets:
    def __init__(self) -> None:
        empty_schema = JsonSchemaValidationProfile.from_schema(
            "fixture.target-options/v1",
            {"type": "object"},
        )
        self.contracts = {
            "opus": TargetDescriptor.seal(
                TargetDescriptorPayload(
                    implementation_id="fixture.target/v1",
                    implementation_version="1.0.0",
                    source_revision="fixture",
                    image_id="sha256:" + _sha("a"),
                    operations=(
                        TargetOperationSupport(
                            operation_id=AUDIO_ARCHIVE_OPERATION.id,
                            operation_contract_sha256=AUDIO_ARCHIVE_OPERATION.contract_sha256,
                            options_schema=empty_schema,
                        ),
                    ),
                )
            ),
            "nvenc-av1-opus": TargetDescriptor.seal(
                TargetDescriptorPayload(
                    implementation_id="fixture.av1-target/v1",
                    implementation_version="1.0.0",
                    source_revision="fixture",
                    image_id="sha256:" + _sha("b"),
                    operations=(
                        TargetOperationSupport(
                            operation_id=AV1_OPUS_ARCHIVE_OPERATION.id,
                            operation_contract_sha256=AV1_OPUS_ARCHIVE_OPERATION.contract_sha256,
                            options_schema=empty_schema,
                        ),
                    ),
                )
            ),
        }

    def registration_ids(self):
        return tuple(self.contracts)

    def descriptor(self, registration_id: str) -> TargetDescriptor:
        return self.contracts[registration_id]


class AssociatedMediaCatalogApi(CatalogApi):
    def search(self, **_kwargs: object) -> dict[str, object]:
        return {
            "artifacts": [
                {
                    "artifact_id": f"{9:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
                {
                    "artifact_id": f"{10:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
                {
                    "artifact_id": f"{11:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
            ]
        }


class MultiArtifactCatalogApi(CatalogApi):
    def search(self, **_kwargs: object) -> dict[str, object]:
        return {
            "artifacts": [
                {
                    "artifact_id": f"{1:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
                {
                    "artifact_id": f"{2:064x}",
                    "bytes": 3,
                    "sha256": _PAYLOAD_SHA,
                },
            ]
        }


CATALOG = Path(__file__).parents[4] / "qualification/fixtures/stove0/recipes.yaml"
ROOT = CollectionRootIdentityRef(
    collection_id="11", archive_root_sha256="1" * 64, artifact_set_identity="2" * 64
)


def planner_for(tmp_path, *, observers=None, catalog=None, api=None, targets=None):
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'control.db'}")
    return RecipePlanner(
        catalog=catalog or load_recipe_catalog(CATALOG),
        state=state,
        riverhog=api or ConformanceCatalogApi(),
        observers=observers or BatchMediaObservers(4),
        targets=targets or ConformanceTargets(),
    )


def plan_until_complete(planner, work, *, restart=False):
    planner = planner.for_invocation("work", work.work_id)
    testimony = []
    for ordinal in range(4000):
        progress = planner.step(work)
        if progress.state not in {"pending", "question"}:
            return planner, progress, tuple(testimony)
        if progress.state == "question":
            prepared = planner.observation_delivery.request(progress.work, progress.question)
            if prepared is None:
                continue
            request, descriptor = prepared
            if request.observer_registration_id == "filename-prefix-sidecars":
                dependencies = {
                    slot.slot: document
                    for slot, document in zip(
                        request.evidence_slots or (),
                        planner.observation_delivery.inputs(request),
                        strict=True,
                    )
                }
                result = planner.observers.filename.observe(
                    request, SimpleNamespace(open_evidence=dependencies.__getitem__)
                )
            else:
                result = ContentObservationResultBuilder(descriptor, request).observed(
                    planner.observers.facts(request, testimony)
                )
            assert result.state == "observed", result.failure
            evidence = ContentObservationEvidence(request=request, result=result)
            planner.observation_delivery.accept(progress.work, evidence)
            testimony.append(evidence)
        if restart and ordinal % 13 == 0:
            state = planner.state
            owner = state.planning_owner
            url = str(state.engine.url)
            state.engine.dispose()
            state = SqlAlchemyStateStore(url).planning_context(*owner)
            planner = RecipePlanner(
                catalog=CompiledRecipeCatalog(),
                state=state,
                riverhog=planner.riverhog,
                observers=planner.observers,
                targets=planner.targets,
            )
    raise AssertionError("current compiled media planning stopped making progress")
