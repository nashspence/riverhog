from __future__ import annotations

import hashlib
from pathlib import Path
from typing import cast

import pytest
from a_stove0_media_archive_contract_lib import (
    AUDIO_ARCHIVE_OPERATION,
    AV1_OPUS_ARCHIVE_OPERATION,
    SOURCE_ROLE,
    XMP_SOURCE_ROLE,
)
from a_stove0_media_metadata_contract_lib import (
    MEDIA_METADATA_OBSERVER_CONTRACT,
    MediaArtifactFacts,
    MediaFactEvidence,
    MediaMetadataFact,
    MediaMetadataFacts,
)
from a_stove0_rclone_target.contracts import RCLONE_DELIVER_OPERATION
from review0_contracts import (
    REVIEW_MATERIALIZE_OPERATION,
    REVIEW_SOURCE_ROLE,
    ReviewSamplePlan,
    ReviewSamplePlanPayload,
    ReviewSampleWindow,
)
from review0_planner import ReviewVariant, review_evaluation_definition
from riverhog_client import ApiClient
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    PortableCollectionHeader,
    PortableCollectionInventoryAuthority,
    PortableCollectionInventoryPage,
)
from riverhog_protocol.collection_workflows import canonical_json_sha256
from riverhog_protocol.errors import NotFound
from stove0_core import (
    ObserverPort,
    RecipeCatalog,
    RecipeDefinition,
    RecipePlanner,
    TargetPort,
    WorkInapplicable,
    WorkNoAction,
)
from stove0_core.recipes import (
    ArtifactFactBinding,
    ArtifactRule,
    FactCondition,
    FactPredicate,
    ObserverUse,
    OperationProjection,
    RecipeCoordinationRoute,
    RecipeJoin,
    RecipeJoinMember,
    RecipeRoute,
)
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
)
from stove0_observer_support import ContentObservationResultBuilder
from stove0_protocol import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    ArtifactSelection,
    BranchNoOutputSettlement,
    BranchSetDecision,
    BranchSettlement,
    CollectionRootIdentityRef,
    CoordinationBranchPlan,
    JsonSchemaValidationProfile,
    NoOutputBranchPlan,
    WorkArtifactSubject,
    WorkIdentity,
    evaluate_branch_set,
    resolve_join_plan,
)
from stove0_target_protocol import TargetInputRoleCount
from stove0_target_support import (
    InputArtifactContract,
    OperationContract,
    OperationContractPayload,
    OutputArtifactContract,
    TargetDescriptor,
    TargetDescriptorPayload,
    TargetOperationSupport,
)

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


def _sha(character: str) -> str:
    return character * 64


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


class Targets:
    def __init__(self, contract: TargetDescriptor) -> None:
        self._contract = contract

    def descriptor(self, registration_id: str) -> TargetDescriptor:
        assert registration_id == "review-ffmpeg"
        return self._contract


class MediaObservers:
    def __init__(self) -> None:
        self.value = ObserverDescriptor.seal(
            ObserverDescriptorPayload(
                implementation_id="fixture.observer/v1",
                implementation_version="1.0.0",
                source_revision="fixture",
                image_id="sha256:" + _sha("9"),
                contracts=(
                    ObserverContractSupport.from_contract(
                        MEDIA_METADATA_OBSERVER_CONTRACT,
                        preferred_subject_batch_size=100,
                    ),
                ),
            )
        )

    def descriptor(self, registration_id: str) -> ObserverDescriptor:
        assert registration_id == "exiftool"
        return self.value


class ArchiveTargets:
    def __init__(self) -> None:
        options = JsonSchemaValidationProfile.from_schema(
            "fixture.archive-options/v1",
            {"type": "object", "additionalProperties": False},
        )
        self.value = TargetDescriptor.seal(
            TargetDescriptorPayload(
                implementation_id="fixture.archive-target/v1",
                implementation_version="1.0.0",
                source_revision="fixture",
                image_id="sha256:" + _sha("8"),
                operations=(
                    TargetOperationSupport(
                        operation_id=AUDIO_ARCHIVE_OPERATION.id,
                        operation_contract_sha256=AUDIO_ARCHIVE_OPERATION.contract_sha256,
                        options_schema=options,
                    ),
                ),
            )
        )

    def descriptor(self, registration_id: str) -> TargetDescriptor:
        assert registration_id in {"opus", "fixture-target"}
        return self.value


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


def _container_fact(role: str, formats: tuple[str, ...]) -> FactPredicate:
    return FactPredicate(
        observation_contract_id=MEDIA_METADATA_OBSERVER_CONTRACT.id,
        artifact_roles=(role,),
        artifact_facts=ArtifactFactBinding(records_pointer="/artifacts"),
        pointer="/value",
        operator="one-of",
        value=list(formats),
        array_pointer="/facts",
        same_item=(FactCondition(pointer="/name", value="container-format"),),
    )


def _metadata_use() -> ObserverUse:
    return ObserverUse(
        registration_id="exiftool",
        contract_id=MEDIA_METADATA_OBSERVER_CONTRACT.id,
        contract_sha256=MEDIA_METADATA_OBSERVER_CONTRACT.contract_sha256,
    )


def _metadata_evidence(planner, work):
    observer = MediaObservers()
    return tuple(
        ContentObservationEvidence(
            request=request,
            result=ContentObservationResultBuilder(observer.value, request).observed(
                _metadata_facts(request)
            ),
        )
        for request in planner.observation_requests(work)
    )


def _observe_stages(planner, work, observers):
    from types import SimpleNamespace

    evidence = []
    for _ in range(8):
        known = {item.request.request_id for item in evidence}
        pending = [
            request
            for request in planner.observation_requests(work, tuple(evidence))
            if request.request_id not in known
        ]
        if not pending:
            return tuple(sorted(evidence, key=lambda item: item.request.request_id))
        for request in pending:
            if request.observer_registration_id == "filename-prefix-sidecars":
                dependencies = {
                    slot.slot: next(
                        item for item in evidence if item.request.request_id == slot.request_id
                    )
                    for slot in request.evidence_slots or ()
                }
                result = observers.filename.observe(
                    request, SimpleNamespace(open_evidence=dependencies.__getitem__)
                )
            else:
                result = ContentObservationResultBuilder(
                    observers.descriptor(request.observer_registration_id), request
                ).observed(observers.facts(request, evidence))
            assert result.state == "observed", result.failure
            evidence.append(ContentObservationEvidence(request=request, result=result))
    raise AssertionError("fixture observation graph did not complete")


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

    def descriptor(self, registration_id: str) -> TargetDescriptor:
        return self.contracts[registration_id]


def _conformance_plan(
    preferred_batch_size: int,
) -> tuple[BranchSetDecision, BranchSetDecision]:
    path = Path(__file__).parents[4] / "qualification/fixtures/stove0/recipes.yaml"
    original = RecipeCatalog.load(path)
    recipe = original.recipe("stove0.conformance-media/v1")
    recipe = recipe.model_copy(
        update={
            "observers": tuple(
                use.model_copy(update={"subject_batch_size": preferred_batch_size})
                if use.registration_id != "filename-prefix-sidecars"
                else use
                for use in recipe.observers
            )
        }
    )
    catalog = RecipeCatalog(operations=original.operations, recipes=(recipe,))
    observers = BatchMediaObservers(preferred_batch_size)
    planner = RecipePlanner(
        catalog=catalog,
        riverhog=cast(ApiClient, ConformanceCatalogApi()),
        observers=cast(ObserverPort, observers),
        targets=cast(TargetPort, ConformanceTargets()),
    )
    root = CollectionRootIdentityRef(
        collection_id="11", archive_root_sha256=_sha("1"), artifact_set_identity=_sha("2")
    )
    work = planner.create_work(recipe.id, (root,))
    evidence = _observe_stages(planner, work, observers)
    plan = planner.workflow_plan(work, evidence)
    reversed_plan = planner.workflow_plan(work, tuple(reversed(evidence)))
    assert isinstance(plan, BranchSetDecision) and isinstance(reversed_plan, BranchSetDecision)
    from a_stove0_media_archive_contract_lib import MediaProjectionPolicy
    from a_stove0_media_archive_lib.projection import resolve_media_archive_preflight_projection
    from a_stove0_media_archive_lib.publication import accepted_source_hints

    for branch in plan.plan.branches:
        request = planner.target_preflight_request(branch.workflow_plan, plan.selection_documents)
        projection = resolve_media_archive_preflight_projection(
            request,
            policy=MediaProjectionPolicy.model_validate(request.intent["metadata_projection"]),
        )
        hints, identities = accepted_source_hints(request)
        selected = plan.selection_documents[branch.artifact_selection.selection_sha256]
        assert set(hints) == {item.id for item in selected.artifacts}
        assert len(identities) > 0
        assert {item.input_artifact_id for item in projection.items} == {
            group.primary_id for group in request.input_groups
        }
        assert {item.input_artifact_id for item in projection.retained_xmp_sidecars} == {
            subject.id for subject in selected.artifacts if subject.role == XMP_SOURCE_ROLE
        }
    return plan, reversed_plan


def _selection_ids(decision: BranchSetDecision) -> dict[str, tuple[str, ...]]:
    return {
        branch.branch_id: tuple(
            sorted(
                artifact.artifact_id
                for artifact in decision.selection_documents[
                    branch.artifact_selection.selection_sha256
                ].artifacts
            )
        )
        for branch in decision.plan.branches
    }


def test_explicit_observation_rule_resolves_no_action_before_target_planning() -> None:
    fixture = Path(__file__).parents[4] / "qualification/fixtures/stove0/recipes.yaml"
    catalog = RecipeCatalog.load(fixture)
    original = catalog.recipe("stove0.conformance-media/v1")
    recipe_document = original.model_dump(mode="json", exclude_none=True)
    recipe_document["no_action"] = {
        "code": "fixture.already-complete/v1",
        "message": "The observed collection requires no transformation.",
        "when": [
            {
                "observation_contract_id": MEDIA_METADATA_OBSERVER_CONTRACT.id,
                "pointer": "/artifacts",
                "operator": "exists",
                "value": True,
            }
        ],
    }
    recipe = RecipeDefinition.model_validate(recipe_document)
    planner = RecipePlanner(
        catalog=RecipeCatalog(operations=catalog.operations, recipes=(recipe,)),
        riverhog=cast(ApiClient, ConformanceCatalogApi()),
        observers=cast(ObserverPort, BatchMediaObservers(100)),
        targets=cast(TargetPort, object()),
    )
    work = planner.create_work(
        recipe.id,
        (
            CollectionRootIdentityRef(
                collection_id=str(11),
                archive_root_sha256=_sha("1"),
                artifact_set_identity=_sha("2"),
            ),
        ),
    )
    observer = BatchMediaObservers(100)
    evidence = _observe_stages(planner, work, observer)
    decision = planner.workflow_plan(work, evidence)
    assert decision == WorkNoAction(
        code="fixture.already-complete/v1",
        message="The observed collection requires no transformation.",
    )
    assert planner.workflow_plan(work, ()) == WorkInapplicable(
        code="no-matching-route",
        message="No configured recipe branch accepted the immutable inputs.",
    )


def test_deployment_owned_conformance_catalog_routes_exact_artifacts_independent_of_batching() -> (
    None
):
    single, single_reversed = _conformance_plan(1)
    batched, batched_reversed = _conformance_plan(4)

    expected = {
        "archive-audio": (
            f"{3:064x}",
            f"{4:064x}",
            f"{5:064x}",
        ),
        "archive-audio-overlap": (
            f"{3:064x}",
            f"{4:064x}",
            f"{5:064x}",
        ),
        "archive-video": (
            f"{7:064x}",
            f"{8:064x}",
        ),
    }
    assert _selection_ids(single) == expected
    assert _selection_ids(single_reversed) == expected
    assert _selection_ids(batched) == expected
    assert _selection_ids(batched_reversed) == expected
    assert single.plan.branch_set_sha256 == single_reversed.plan.branch_set_sha256
    assert batched.plan.branch_set_sha256 == batched_reversed.plan.branch_set_sha256
    assert single.plan.source_collection_retirement_policy == "retain"
    intents = {
        branch.branch_id: branch.workflow_plan.work.effective_intent
        for branch in single.plan.branches
    }
    assert intents["archive-audio"]["metadata_projection"] == {
        "format": "stove0-media-projection-policy/v1",
        "device_make": "Example Maker",
        "device_model": "Model One",
        "gps": {"latitude": 45.0, "longitude": -122.0},
        "creators": ["Example Operator"],
        "tags": ["conformance"],
        "field_preferences": [{"name": "capture-time", "fields": ["XMP-xmp:CreateDate"]}],
    }


def test_installed_catalog_rejects_stale_observer_contract_before_observation() -> None:
    path = Path(__file__).parents[4] / "qualification/fixtures/stove0/recipes.yaml"
    catalog = RecipeCatalog.load(path)
    recipe = catalog.recipe("stove0.conformance-media/v1")
    stale_observer = recipe.observers[0].model_copy(update={"contract_sha256": _sha("f")})
    stale_recipe = recipe.model_copy(update={"observers": (stale_observer, *recipe.observers[1:])})
    stale_catalog = RecipeCatalog(
        operations=catalog.operations,
        recipes=tuple(
            stale_recipe if item.id == stale_recipe.id else item for item in catalog.recipes
        ),
    )
    planner = RecipePlanner(
        catalog=stale_catalog,
        riverhog=cast(ApiClient, ConformanceCatalogApi()),
        observers=cast(ObserverPort, BatchMediaObservers(4)),
        targets=cast(TargetPort, ConformanceTargets()),
    )
    work = planner.create_work(
        stale_recipe.id,
        (
            CollectionRootIdentityRef(
                collection_id=str(11),
                archive_root_sha256=_sha("1"),
                artifact_set_identity=_sha("2"),
            ),
        ),
    )

    with pytest.raises(RuntimeError, match="another revision of the recipe contract"):
        planner.observation_requests(work)


def test_planning_rejects_stale_target_operation_contract_before_preflight() -> None:
    changed_operation = OperationContract.seal(
        OperationContractPayload(
            id=AUDIO_ARCHIVE_OPERATION.id,
            intent_semantics=AUDIO_ARCHIVE_OPERATION.intent_semantics,
            intent_schema=AUDIO_ARCHIVE_OPERATION.intent_schema,
            inputs=AUDIO_ARCHIVE_OPERATION.inputs,
            outputs=AUDIO_ARCHIVE_OPERATION.outputs,
            source_collection_retirement_permitted=True,
        )
    )
    recipe = RecipeDefinition(
        artifact_rules=(ArtifactRule(role="stove0.media.source/v1"),),
        id="fixture.stale-target/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        routes=(
            RecipeRoute(
                id="archive",
                operation_id=changed_operation.id,
                target_registration_id="opus",
            ),
        ),
    )
    planner = RecipePlanner(
        catalog=RecipeCatalog(operations=(changed_operation,), recipes=(recipe,)),
        riverhog=cast(ApiClient, CatalogApi()),
        observers=cast(ObserverPort, object()),
        targets=cast(TargetPort, ArchiveTargets()),
    )
    work = planner.create_work(
        recipe.id,
        (
            CollectionRootIdentityRef(
                collection_id=str(11),
                archive_root_sha256=_sha("1"),
                artifact_set_identity=_sha("2"),
            ),
        ),
    )

    with pytest.raises(RuntimeError, match="another revision of the recipe operation"):
        planner.workflow_plan(work, ())


def test_planner_seals_exact_nested_subrecipe_tree_without_target_smearing() -> None:
    child = RecipeDefinition(
        artifact_rules=(ArtifactRule(role="stove0.media.source/v1"),),
        id="fixture.child/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        routes=(
            RecipeRoute(
                id="archive",
                operation_id=AUDIO_ARCHIVE_OPERATION.id,
                target_registration_id="opus",
            ),
        ),
    )
    parent = RecipeDefinition(
        artifact_rules=(ArtifactRule(role="stove0.media.source/v1"),),
        id="fixture.parent/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        routes=(
            RecipeCoordinationRoute(
                id="nested",
                recipe=child.ref,
                intent={"scope": "child"},
            ),
        ),
    )
    planner = RecipePlanner(
        catalog=RecipeCatalog(
            operations=(AUDIO_ARCHIVE_OPERATION,),
            recipes=(child, parent),
        ),
        riverhog=cast(ApiClient, CatalogApi()),
        observers=cast(ObserverPort, object()),
        targets=cast(TargetPort, ArchiveTargets()),
    )
    work = planner.create_work(
        parent.id,
        (
            CollectionRootIdentityRef(
                collection_id=str(11),
                archive_root_sha256=_sha("1"),
                artifact_set_identity=_sha("2"),
            ),
        ),
    )

    first = planner.workflow_plan(work, (), nested_observer=lambda _child: ())
    second = planner.workflow_plan(work, (), nested_observer=lambda _child: ())

    assert isinstance(first, BranchSetDecision)
    assert first == second
    assert len(first.plan.branches) == 1
    declaration = first.plan.branches[0]
    assert isinstance(declaration, CoordinationBranchPlan)
    assert declaration.work.recipe == child.ref
    assert declaration.work.effective_intent == {"scope": "child"}
    child_plan = first.branch_set_documents[declaration.branch_set_sha256]
    assert child_plan.parent_work == declaration.work
    assert child_plan.source_collection_retirement_policy == "retain"
    assert [item.branch_id for item in child_plan.branches] == ["archive"]
    assert [item.workflow_plan.work.work_id for item in first.leaf_branches()] == [
        child_plan.branches[0].workflow_plan.work.work_id
    ]
    with pytest.raises(RuntimeError, match="observation authority"):
        planner.workflow_plan(work, ())


def test_nested_no_output_is_a_required_success_without_material_output() -> None:
    fixture = Path(__file__).parents[4] / "qualification/fixtures/stove0/recipes.yaml"
    catalog = RecipeCatalog.load(fixture)
    child_document = catalog.recipe("stove0.conformance-media/v1").model_dump(
        mode="json", exclude_none=True
    )
    child_document["id"] = "fixture.a-no-output/v1"
    child_document["no_action"] = {
        "code": "fixture.no-output/v1",
        "message": "No material output is needed.",
        "when": [
            {
                "observation_contract_id": MEDIA_METADATA_OBSERVER_CONTRACT.id,
                "pointer": "/artifacts",
                "operator": "exists",
                "value": True,
            }
        ],
    }
    child = RecipeDefinition.model_validate(child_document)
    parent = RecipeDefinition(
        artifact_rules=(ArtifactRule(role="stove0.media.source/v1"),),
        id="fixture.b-parent/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        routes=(
            RecipeCoordinationRoute(
                id="no-output",
                recipe=child.ref,
            ),
        ),
    )
    observer = BatchMediaObservers(100)
    planner = RecipePlanner(
        catalog=RecipeCatalog(operations=catalog.operations, recipes=(child, parent)),
        riverhog=cast(ApiClient, ConformanceCatalogApi()),
        observers=cast(ObserverPort, observer),
        targets=cast(TargetPort, object()),
    )
    work = planner.create_work(
        parent.id,
        (
            CollectionRootIdentityRef(
                collection_id=str(11),
                archive_root_sha256=_sha("1"),
                artifact_set_identity=_sha("2"),
            ),
        ),
    )

    def observe(child_work: WorkIdentity):
        return _observe_stages(planner, child_work, observer)

    decision = planner.workflow_plan(work, (), nested_observer=observe)
    assert isinstance(decision, BranchSetDecision)
    branch = decision.plan.branches[0]
    assert isinstance(branch, NoOutputBranchPlan)
    assert branch.work.recipe == child.ref
    assert decision.leaf_branches() == ()
    settlement = BranchNoOutputSettlement.seal(branch=branch, no_output_settlement_sha256=_sha("f"))
    evaluation = evaluate_branch_set(
        decision.plan,
        decision.selection_documents,
        branch_sets=decision.branch_set_documents,
        branch_no_output_settlements=(settlement,),
    )
    assert evaluation.branch_set_succeeded
    assert evaluation.succeeded_no_outputs == (settlement,)
    assert evaluation.coordination_settlement is not None
    assert evaluation.coordination_settlement.children[0].kind == "no-output"
    assert evaluation.coordination_settlement.collection_result is None


def test_recipe_explicitly_rejects_unmatched_primary_and_sidecar_artifacts() -> None:
    recipe = RecipeDefinition(
        artifact_rules=(
            ArtifactRule(role=SOURCE_ROLE, when=(_container_fact(SOURCE_ROLE, ("MOV",)),)),
        ),
        id="fixture.reject-unmatched/v1",
        revision="1",
        unmatched_artifact_disposition="reject-work",
        observers=(_metadata_use(),),
        routes=(
            RecipeRoute(
                id="archive",
                primary_role=SOURCE_ROLE,
                operation_id=AUDIO_ARCHIVE_OPERATION.id,
                target_registration_id="opus",
            ),
        ),
    )
    planner = RecipePlanner(
        catalog=RecipeCatalog(operations=(AUDIO_ARCHIVE_OPERATION,), recipes=(recipe,)),
        riverhog=cast(ApiClient, AssociatedMediaCatalogApi()),
        observers=cast(ObserverPort, MediaObservers()),
        targets=cast(TargetPort, ArchiveTargets()),
    )
    work = planner.create_work(
        recipe.id,
        (
            CollectionRootIdentityRef(
                collection_id=str(11),
                archive_root_sha256=_sha("1"),
                artifact_set_identity=_sha("2"),
            ),
        ),
    )

    decision = planner.workflow_plan(work, _metadata_evidence(planner, work))

    assert isinstance(decision, WorkInapplicable)
    assert decision.code == "unmatched-artifacts"
    assert f"{10:064x}" in decision.message
    assert f"{11:064x}" in decision.message


def test_recipe_projection_count_is_defined_by_the_recipe() -> None:
    projections = tuple(
        OperationProjection(
            source="work-effective-intent",
            source_pointer=f"/source-{index:03}",
            destination="intent",
            destination_pointer=f"/destination-{index:03}",
        )
        for index in range(65)
    )

    route = RecipeRoute(
        id="large-explicit-projection",
        operation_id="fixture.operation/v1",
        target_registration_id="fixture-target",
        projections=projections,
    )

    assert route.projections == projections


def test_observer_preference_batches_unbounded_collection_work_without_omission() -> None:
    recipe = RecipeDefinition(
        artifact_rules=(ArtifactRule(role="stove0.media.source/v1"),),
        id="fixture.large-observation/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        observers=(
            ObserverUse(
                registration_id="exiftool",
                contract_id=MEDIA_METADATA_OBSERVER_CONTRACT.id,
                contract_sha256=MEDIA_METADATA_OBSERVER_CONTRACT.contract_sha256,
            ),
        ),
        routes=(
            RecipeRoute(
                id="archive",
                forward_observation_contract_ids=(MEDIA_METADATA_OBSERVER_CONTRACT.id,),
                operation_id=AUDIO_ARCHIVE_OPERATION.id,
                target_registration_id="opus",
            ),
            RecipeRoute(
                id="incorrect-absence",
                when=(
                    FactPredicate(
                        observation_contract_id=MEDIA_METADATA_OBSERVER_CONTRACT.id,
                        pointer="/artifacts/63/state",
                        operator="exists",
                        value=False,
                    ),
                ),
                forward_observation_contract_ids=(MEDIA_METADATA_OBSERVER_CONTRACT.id,),
                operation_id=AUDIO_ARCHIVE_OPERATION.id,
                target_registration_id="opus",
            ),
            RecipeRoute(
                id="observed-unsupported",
                when=(
                    FactPredicate(
                        observation_contract_id=MEDIA_METADATA_OBSERVER_CONTRACT.id,
                        pointer="/artifacts/0/state",
                        operator="equals",
                        value="unsupported",
                    ),
                ),
                forward_observation_contract_ids=(MEDIA_METADATA_OBSERVER_CONTRACT.id,),
                operation_id=AUDIO_ARCHIVE_OPERATION.id,
                target_registration_id="opus",
            ),
        ),
    )
    observers = MediaObservers()
    planner = RecipePlanner(
        catalog=RecipeCatalog(
            operations=(AUDIO_ARCHIVE_OPERATION,),
            recipes=(recipe,),
        ),
        riverhog=cast(ApiClient, LargeCatalogApi()),
        observers=cast(ObserverPort, observers),
        targets=cast(TargetPort, ArchiveTargets()),
    )
    root = CollectionRootIdentityRef(
        collection_id=str(11),
        archive_root_sha256=_sha("1"),
        artifact_set_identity=_sha("2"),
    )
    work = planner.create_work(recipe.id, (root,))

    requests = planner.observation_requests(work)

    assert sorted(len(request.subjects) for request in requests) == [57, 100, 100]
    subjects = [subject for request in requests for subject in request.subjects]
    assert len(subjects) == 257
    assert len({subject.id for subject in subjects}) == 257
    evidence = tuple(
        ContentObservationEvidence(
            request=request,
            result=ContentObservationResultBuilder(observers.value, request).observed(
                MediaMetadataFacts(
                    artifacts=tuple(
                        MediaArtifactFacts(artifact_id=subject.id, state="unsupported")
                        for subject in request.subjects
                    )
                ).model_dump(mode="json")
            ),
        )
        for request in requests
    )

    decision = planner.workflow_plan(work, evidence)

    assert isinstance(decision, BranchSetDecision)
    assert decision.plan.evidence_sha256s == tuple(
        sorted(item.result.result_sha256 for item in evidence)
    )
    assert [branch.branch_id for branch in decision.plan.branches] == [
        "archive",
        "observed-unsupported",
    ]
    assert all(branch.workflow_plan.observations == evidence for branch in decision.plan.branches)


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


def test_media_observation_evidence_binds_exact_primary_sidecar_selection() -> None:
    fixture = Path(__file__).parents[4] / "qualification/fixtures/stove0/recipes.yaml"
    supplied = RecipeCatalog.load(fixture).recipe("stove0.conformance-media/v1")
    recipe = RecipeDefinition(
        id="fixture.observed-media/v1",
        revision="1",
        artifact_rules=(
            ArtifactRule(role=XMP_SOURCE_ROLE, when=(_container_fact(XMP_SOURCE_ROLE, ("XMP",)),)),
            ArtifactRule(role=SOURCE_ROLE, when=(_container_fact(SOURCE_ROLE, ("MOV",)),)),
        ),
        artifact_associations=supplied.artifact_associations,
        observers=supplied.observers,
        unmatched_artifact_disposition="retain-in-source",
        routes=(
            RecipeRoute(
                id="archive",
                primary_role=SOURCE_ROLE,
                associated_roles=(XMP_SOURCE_ROLE,),
                operation_id=AUDIO_ARCHIVE_OPERATION.id,
                target_registration_id="opus",
                forward_observation_contract_ids=(MEDIA_METADATA_OBSERVER_CONTRACT.id,),
            ),
        ),
    )
    observers = BatchMediaObservers(1, capture_time="2025:02:03 04:05:06-08:00")
    planner = RecipePlanner(
        catalog=RecipeCatalog(operations=(AUDIO_ARCHIVE_OPERATION,), recipes=(recipe,)),
        riverhog=cast(ApiClient, AssociatedMediaCatalogApi()),
        observers=cast(ObserverPort, observers),
        targets=cast(TargetPort, ArchiveTargets()),
    )
    root = CollectionRootIdentityRef(
        collection_id="11", archive_root_sha256=_sha("1"), artifact_set_identity=_sha("2")
    )
    work = planner.create_work(recipe.id, (root,))
    evidence = _observe_stages(planner, work, observers)
    decision = planner.workflow_plan(work, evidence)
    assert isinstance(decision, BranchSetDecision)
    selection = decision.selection_documents[
        decision.plan.branches[0].artifact_selection.selection_sha256
    ]
    assert {artifact.artifact_id: artifact.role for artifact in selection.artifacts} == {
        f"{9:064x}": SOURCE_ROLE,
        f"{10:064x}": XMP_SOURCE_ROLE,
    }
    assert decision.plan.source_collection_retirement_policy == "retain"
    assert decision.plan.evidence_sha256s == tuple(
        sorted(item.result.result_sha256 for item in evidence)
    )
    forwarded = tuple(
        item
        for item in evidence
        if item.request.observer_contract_id == MEDIA_METADATA_OBSERVER_CONTRACT.id
        and any(
            subject.id in {artifact.id for artifact in selection.artifacts}
            for subject in item.request.subjects
        )
    )
    assert decision.plan.branches[0].workflow_plan.observations == forwarded
    preflight = planner.target_preflight_request(
        decision.plan.branches[0].workflow_plan, decision.selection_documents
    )
    assert preflight.observations == forwarded
    # Changing an accepted measurement changes the seal, while role and relation facts persist.
    request = forwarded[0].request
    changed_result = ContentObservationResultBuilder(observers.value, request).observed(
        _metadata_facts(request, capture_time="2025:02:03 04:05:07-08:00")
    )
    changed_evidence = tuple(
        ContentObservationEvidence(request=request, result=changed_result)
        if item.request.request_id == request.request_id
        else item
        for item in evidence
    )
    changed = planner.workflow_plan(work, changed_evidence)
    assert isinstance(changed, BranchSetDecision)
    assert changed.plan.branch_set_sha256 != decision.plan.branch_set_sha256
    assert _selection_ids(changed) == _selection_ids(decision)


def test_review_recipe_projects_semantic_intent_and_options_before_preflight() -> None:
    target = TargetDescriptor.seal(
        TargetDescriptorPayload(
            implementation_id="riverhog.review-ffmpeg/v1",
            implementation_version="1.0.0",
            source_revision="fixture",
            image_id="sha256:" + _sha("9"),
            operations=(
                TargetOperationSupport(
                    operation_id=REVIEW_MATERIALIZE_OPERATION.id,
                    operation_contract_sha256=(REVIEW_MATERIALIZE_OPERATION.contract_sha256),
                    options_schema=JsonSchemaValidationProfile.from_schema(
                        "riverhog.review-ffmpeg-options/v1",
                        {
                            "type": "object",
                            "properties": {
                                "threads": {"type": "integer"},
                                "crf": {"type": "integer"},
                            },
                            "additionalProperties": False,
                        },
                    ),
                ),
            ),
        )
    )
    recipe = RecipeDefinition(
        artifact_rules=(ArtifactRule(role=REVIEW_SOURCE_ROLE),),
        id="review-evaluation/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        routes=(
            RecipeRoute(
                id="review",
                operation_id=REVIEW_MATERIALIZE_OPERATION.id,
                target_registration_id="review-ffmpeg",
                target_options={"threads": 2},
                projections=(
                    OperationProjection(
                        source="work-effective-intent",
                        source_pointer="/review_sample_plan",
                        destination="intent",
                        destination_pointer="/sample_plan",
                    ),
                    OperationProjection(
                        source="work-evaluation",
                        source_pointer="/variant_id",
                        destination="intent",
                        destination_pointer="/variant/id",
                    ),
                    OperationProjection(
                        source="work-evaluation",
                        source_pointer="/parameters/review_variant/portable_intent",
                        destination="intent",
                        destination_pointer="/variant/portable_intent",
                    ),
                    OperationProjection(
                        source="work-evaluation",
                        source_pointer="/parameters/review_variant/target_options",
                        destination="target-options",
                        destination_pointer="",
                    ),
                ),
            ),
        ),
    )
    assert recipe.event_input_closure == "single-finalized-collection"
    catalog = RecipeCatalog(
        operations=(REVIEW_MATERIALIZE_OPERATION,),
        recipes=(recipe,),
    )
    root = CollectionRootIdentityRef(
        collection_id=str(11),
        archive_root_sha256=_sha("1"),
        artifact_set_identity=_sha("2"),
    )
    artifact_id = (
        "a-"
        + canonical_json_sha256(
            {
                "collection_id": 11,
                "artifact_id": f"{1:064x}",
            }
        )[:32]
    )
    sample_plan = ReviewSamplePlan.seal(
        ReviewSamplePlanPayload(
            samples_per_artifact=1,
            window_duration_ms=500,
            windows=(
                ReviewSampleWindow(
                    artifact_id=artifact_id,
                    start_ms=100,
                    duration_ms=500,
                ),
            ),
        )
    )
    definition = review_evaluation_definition(
        recipe=recipe.ref,
        inputs=(root,),
        sample_plan=sample_plan,
        variants=(
            ReviewVariant(
                id="crf-30",
                portable_intent={"label": "candidate"},
                target_options={"crf": 30},
            ),
        ),
    )
    planner = RecipePlanner(
        catalog=catalog,
        riverhog=cast(ApiClient, CatalogApi()),
        observers=cast(ObserverPort, object()),
        targets=cast(TargetPort, Targets(target)),
    )

    work = definition.child_work("crf-30")
    decision = planner.workflow_plan(work, ())
    assert isinstance(decision, BranchSetDecision)
    assert len(decision.plan.branches) == 1
    plan = decision.plan.branches[0].workflow_plan
    request = planner.target_preflight_request(plan, decision.selection_documents)

    assert plan.requested_target_options == {"threads": 2, "crf": 30}
    assert plan.work.evaluation == work.evaluation
    assert plan.input_retrieval_policy == "available-only"
    assert request.target_options == plan.requested_target_options
    assert request.intent == {
        "sample_plan": sample_plan.model_dump(mode="json"),
        "variant": {
            "id": "crf-30",
            "portable_intent": {"label": "candidate"},
        },
    }
    assert "review_variant" not in request.intent


def test_supplied_recipes_embed_exact_maintained_contracts_and_explicit_cost_policy() -> None:
    path = Path(__file__).parents[4] / "qualification/fixtures/stove0/recipes.yaml"
    catalog = RecipeCatalog.load(path)

    assert catalog.operations == tuple(
        sorted(
            (
                AUDIO_ARCHIVE_OPERATION,
                AV1_OPUS_ARCHIVE_OPERATION,
                REVIEW_MATERIALIZE_OPERATION,
                RCLONE_DELIVER_OPERATION,
            ),
            key=lambda operation: operation.id,
        )
    )
    assert {recipe.event_input_closure for recipe in catalog.recipes} == {
        "single-finalized-collection"
    }
    policies = {
        route.input_retrieval_policy for recipe in catalog.recipes for route in recipe.routes
    }
    assert policies == {"allow", "available-only"}


def test_manual_planning_does_not_consult_derivation() -> None:
    class DerivedCatalogApi(CatalogApi):
        def get_collection_derivation(self, collection_id: int) -> dict[str, object]:
            raise AssertionError("derivation is evidence, not planning authority")

    recipe = RecipeDefinition(
        artifact_rules=(ArtifactRule(role=SOURCE_ROLE),),
        id="fixture.derived-admission/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        routes=(
            RecipeRoute(
                id="archive",
                operation_id=AUDIO_ARCHIVE_OPERATION.id,
                target_registration_id="opus",
            ),
        ),
    )
    root = CollectionRootIdentityRef(
        collection_id="11", archive_root_sha256=_sha("1"), artifact_set_identity=_sha("2")
    )

    planner = RecipePlanner(
        catalog=RecipeCatalog(operations=(AUDIO_ARCHIVE_OPERATION,), recipes=(recipe,)),
        riverhog=cast(ApiClient, DerivedCatalogApi()),
        observers=cast(ObserverPort, object()),
        targets=cast(TargetPort, object()),
    )
    assert planner.create_work(recipe.id, (root,)).inputs == (root,)


def test_production_planner_resolves_overlapping_branches_into_one_exact_join() -> None:
    empty_schema = JsonSchemaValidationProfile.from_schema(
        "fixture.empty/v1",
        {"type": "object", "additionalProperties": False},
    )
    branch_operation = OperationContract.seal(
        OperationContractPayload(
            id="fixture.branch/v1",
            intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            intent_schema=empty_schema,
            inputs=(
                InputArtifactContract(
                    role="fixture.source/v1",
                    allowed_dispositions=("transformed",),
                ),
            ),
            outputs=(
                OutputArtifactContract(
                    role="fixture.branch-output/v1",
                    derived_from_roles=("fixture.source/v1",),
                ),
            ),
        )
    )
    join_operation = OperationContract.seal(
        OperationContractPayload(
            id="fixture.join/v1",
            intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            intent_schema=empty_schema,
            inputs=(
                InputArtifactContract(
                    role="fixture.branch-output/v1",
                    minimum=2,
                    allowed_dispositions=("transformed",),
                ),
            ),
            outputs=(
                OutputArtifactContract(
                    role="fixture.join-output/v1",
                    derived_from_roles=("fixture.branch-output/v1",),
                ),
            ),
        )
    )
    target = TargetDescriptor.seal(
        TargetDescriptorPayload(
            implementation_id="fixture.target/v1",
            implementation_version="1.0.0",
            source_revision="fixture",
            image_id="sha256:" + _sha("9"),
            operations=tuple(
                TargetOperationSupport(
                    operation_id=operation.id,
                    operation_contract_sha256=operation.contract_sha256,
                    options_schema=empty_schema,
                )
                for operation in (branch_operation, join_operation)
            ),
        )
    )

    class ForkJoinTargets:
        def descriptor(self, registration_id: str) -> TargetDescriptor:
            assert registration_id == "fixture-target"
            return target

    recipe = RecipeDefinition(
        artifact_rules=(ArtifactRule(role="fixture.source/v1"),),
        id="fixture.fork-join/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        routes=tuple(
            RecipeRoute(
                id=branch_id,
                operation_id=branch_operation.id,
                target_registration_id="fixture-target",
            )
            for branch_id in ("audio", "video")
        ),
        join=RecipeJoin(
            id="combine",
            members=tuple(
                RecipeJoinMember(
                    branch_id=branch_id,
                    output_roles=("fixture.branch-output/v1",),
                )
                for branch_id in ("audio", "video")
            ),
            operation_id=join_operation.id,
            target_registration_id="fixture-target",
        ),
    )
    planner = RecipePlanner(
        catalog=RecipeCatalog(
            operations=(branch_operation, join_operation),
            recipes=(recipe,),
        ),
        riverhog=cast(ApiClient, CatalogApi()),
        observers=cast(ObserverPort, object()),
        targets=cast(TargetPort, ForkJoinTargets()),
    )
    root = CollectionRootIdentityRef(
        collection_id=str(11),
        archive_root_sha256=_sha("1"),
        artifact_set_identity=_sha("2"),
    )
    work = planner.create_work(recipe.id, (root,))
    decision = planner.workflow_plan(work, ())
    assert isinstance(decision, BranchSetDecision)
    assert decision.plan.join is not None
    branch_selections = [
        decision.selection_documents[item.artifact_selection.selection_sha256]
        for item in decision.plan.branches
    ]
    assert branch_selections[0] == branch_selections[1]

    selections = dict(decision.selection_documents)
    settlements: list[BranchSettlement] = []
    for collection_id, branch in enumerate(decision.plan.branches, start=21):
        output_root = CollectionRootIdentityRef(
            collection_id=str(collection_id),
            archive_root_sha256=f"{collection_id % 16:x}" * 64,
            artifact_set_identity=f"{(collection_id + 1) % 16:x}" * 64,
        )
        output = ArtifactSelection.seal(
            (
                WorkArtifactSubject(
                    id=f"{branch.branch_id}-output",
                    role="fixture.branch-output/v1",
                    collection=output_root,
                    artifact_id=f"{collection_id:064x}",
                    bytes=str(12),
                    sha256=f"{(collection_id + 2) % 16:x}" * 64,
                ),
            )
        )
        selections[output.selection_sha256] = output
        settlements.append(
            BranchSettlement.seal(
                branch=branch,
                producer_settlement_sha256=f"{(collection_id + 4) % 16:x}" * 64,
                derivation_sha256=f"{(collection_id + 3) % 16:x}" * 64,
                output_collection=output_root,
                output_selection=output,
            )
        )
    resolved = resolve_join_plan(decision.plan, selections, settlements)
    assert resolved is not None
    join_plan, join_selections = resolved
    request = planner.target_preflight_request(
        join_plan.workflow_plan,
        {item.selection_sha256: item for item in join_selections},
    )
    assert request.inputs.selection.artifact_count == 2
    assert request.inputs.roles == (TargetInputRoleCount(role="fixture.branch-output/v1", count=2),)
    assert {
        artifact.collection.collection_id
        for selection in join_selections
        for artifact in selection.artifacts
    } == {21, 22}


def _retirement_operation() -> OperationContract:
    return OperationContract.seal(
        OperationContractPayload(
            id="fixture.retirement-copy/v1",
            intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            intent_schema=JsonSchemaValidationProfile.from_schema(
                "fixture.retirement-copy-options/v1",
                {"type": "object", "additionalProperties": False},
            ),
            inputs=(
                InputArtifactContract(
                    role="fixture.source/v1",
                    allowed_dispositions=("transformed",),
                ),
            ),
            outputs=(
                OutputArtifactContract(
                    role="fixture.output/v1",
                    derived_from_roles=("fixture.source/v1",),
                ),
            ),
            source_collection_retirement_permitted=True,
        )
    )


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


def _retirement_planner(recipe: RecipeDefinition) -> RecipePlanner:
    operation = _retirement_operation()
    target = TargetDescriptor.seal(
        TargetDescriptorPayload(
            implementation_id="fixture.retirement-target/v1",
            implementation_version="1.0.0",
            source_revision="fixture",
            image_id="sha256:" + _sha("9"),
            operations=(
                TargetOperationSupport(
                    operation_id=operation.id,
                    operation_contract_sha256=operation.contract_sha256,
                    options_schema=JsonSchemaValidationProfile.from_schema(
                        "fixture.retirement-target-options/v1",
                        {"type": "object", "additionalProperties": False},
                    ),
                ),
            ),
        )
    )
    return RecipePlanner(
        catalog=RecipeCatalog(operations=(operation,), recipes=(recipe,)),
        riverhog=cast(ApiClient, MultiArtifactCatalogApi()),
        observers=cast(ObserverPort, MediaObservers()),
        targets=cast(TargetPort, Targets(target)),
    )


def test_retirement_plan_accepts_overlapping_selections_covering_complete_inventory() -> None:
    recipe = RecipeDefinition(
        artifact_rules=(ArtifactRule(role="fixture.source/v1"),),
        id="fixture.retirement/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        source_collection_retirement_policy="retire-after-settlement",
        observers=(_metadata_use(),),
        routes=(
            RecipeRoute(
                id="all",
                operation_id="fixture.retirement-copy/v1",
                target_registration_id="review-ffmpeg",
            ),
            RecipeRoute(
                id="video",
                primary_role="fixture.source/v1",
                when=(_container_fact("fixture.source/v1", ("MP4",)),),
                operation_id="fixture.retirement-copy/v1",
                target_registration_id="review-ffmpeg",
            ),
        ),
    )
    planner = _retirement_planner(recipe)
    root = CollectionRootIdentityRef(
        collection_id=str(11),
        archive_root_sha256=_sha("1"),
        artifact_set_identity=_sha("2"),
    )

    work = planner.create_work(recipe.id, (root,))
    decision = planner.workflow_plan(work, _metadata_evidence(planner, work))

    assert isinstance(decision, BranchSetDecision)
    assert decision.plan.source_collection_retirement_policy == "retire-after-settlement"
    selections = {
        branch.branch_id: decision.selection_documents[branch.artifact_selection.selection_sha256]
        for branch in decision.plan.branches
    }
    assert len(selections["all"].artifacts) == 2
    assert selections["video"].artifacts[0] in selections["all"].artifacts


def test_retirement_plan_rejects_incomplete_inventory_before_target_preflight() -> None:
    recipe = RecipeDefinition(
        artifact_rules=(ArtifactRule(role="fixture.source/v1"),),
        id="fixture.retirement/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        source_collection_retirement_policy="retire-after-settlement",
        observers=(_metadata_use(),),
        routes=(
            RecipeRoute(
                id="video-only",
                primary_role="fixture.source/v1",
                when=(_container_fact("fixture.source/v1", ("MP4",)),),
                operation_id="fixture.retirement-copy/v1",
                target_registration_id="review-ffmpeg",
            ),
        ),
    )
    planner = _retirement_planner(recipe)
    root = CollectionRootIdentityRef(
        collection_id=str(11),
        archive_root_sha256=_sha("1"),
        artifact_set_identity=_sha("2"),
    )

    work = planner.create_work(recipe.id, (root,))
    decision = planner.workflow_plan(work, _metadata_evidence(planner, work))

    assert isinstance(decision, WorkInapplicable)
    assert decision.code == "unsafe-retirement-coverage"
    assert f"{2:064x}" in decision.message


def test_catalog_rejects_retirement_recipe_using_audio_only_operation() -> None:
    recipe = RecipeDefinition(
        artifact_rules=(ArtifactRule(role="stove0.media.source/v1"),),
        id="fixture.unsafe-audio-retirement/v1",
        revision="1",
        unmatched_artifact_disposition="retain-in-source",
        source_collection_retirement_policy="retire-after-settlement",
        routes=(
            RecipeRoute(
                id="audio",
                operation_id=AUDIO_ARCHIVE_OPERATION.id,
                target_registration_id="opus",
            ),
        ),
    )

    with pytest.raises(ValueError, match="do not authorize retirement: audio"):
        RecipeCatalog(operations=(AUDIO_ARCHIVE_OPERATION,), recipes=(recipe,))
