"""Physical batching reduces deliveries without changing the complete logical question."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from a_stove0_ffprobe_observer import FfprobeObserver
from a_stove0_ffprobe_streams_contract_lib import FFPROBE_STREAMS_SEMANTIC_VALIDATOR, artifact_facts
from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_CONFORMANCE_VECTORS,
    MATERIALIZATION_HINT_SEMANTIC_VALIDATOR,
)
from a_stove0_riverhog_provenance_observer import RiverhogProvenanceObserver
from stove0_core.compiled_observation_delivery import CompiledObservationDelivery
from stove0_core.observation_questions import logical_question
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_observer_protocol import ContentObservationEvidence, SemanticValidatorRegistry
from stove0_observer_support import ContentObservationResultBuilder
from stove0_protocol import (
    ArtifactSelection,
    CollectionRootIdentityRef,
    WorkArtifactSubject,
    WorkIdentity,
    WorkPayload,
    canonical_json_bytes,
)
from stove0_recipe_config import load_recipe_catalog

CATALOG = Path(__file__).parents[4] / "qualification/fixtures/stove0/recipes.yaml"


def _delivery(tmp_path, task_id, count):
    catalog = load_recipe_catalog(CATALOG)
    recipe = catalog.recipe("stove0.audio-archive/v1")
    root = CollectionRootIdentityRef(
        collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
    )
    work = WorkIdentity.seal(WorkPayload(recipe=recipe.ref, inputs=(root,), effective_intent={}))
    subjects = tuple(
        WorkArtifactSubject(
            id=f"{index:064x}",
            artifact_id=f"{index:064x}",
            collection=root,
            role="stove0.media.source/v1",
            bytes="4044",
            sha256="c" * 64,
        )
        for index in range(1, count + 1)
    )
    selection = ArtifactSelection.seal(subjects)
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'control.db'}")
    state.retain_selection(selection)
    task = recipe.observations[task_id]
    resource = catalog.closure.interface(id=task.interface.id, sha256=task.interface.sha256)
    descriptor = (
        FfprobeObserver(image_id="sha256:" + "d" * 64).descriptor()
        if task_id == "streams"
        else RiverhogProvenanceObserver(image_id="sha256:" + "d" * 64).descriptor()
    )
    validators = SemanticValidatorRegistry(
        (FFPROBE_STREAMS_SEMANTIC_VALIDATOR, MATERIALIZATION_HINT_SEMANTIC_VALIDATOR)
    )
    planner = SimpleNamespace(
        state=state,
        _definition=lambda _work: (recipe, catalog.closure),
        observer_binding=lambda _work, _question: (task_id, descriptor),
        observation_execution_timeout_seconds=300,
        observers=SimpleNamespace(semantic_validators=lambda _registration: validators),
    )
    question = state.accepted_observations.ensure_question(
        logical_question(
            work=work,
            task_id=task_id,
            task=task,
            resource=resource,
            scope=selection.ref(),
            subject_ports={"subjects": selection.ref()},
            predecessors={},
        )
    )
    return planner, work, question, descriptor, resource, subjects


def _facts(request):
    if request.task_id == "streams":
        report = canonical_json_bytes(
            {
                "format": {"format_name": "wav", "duration": "0.25"},
                "streams": [{"index": 0, "codec_type": "audio", "codec_name": "pcm_s16le"}],
            }
        )
        return {
            "artifacts": [
                artifact_facts(
                    subject.id,
                    report,
                    ffprobe_version="fixture-ffprobe",
                    executable_sha256="f" * 64,
                ).model_dump(mode="json")
                for subject in request.subjects
            ]
        }
    sample = next(
        vector.facts["artifacts"][0]
        for vector in MATERIALIZATION_HINT_CONFORMANCE_VECTORS.vectors
        if vector.accepted
    )
    facts = []
    for subject in request.subjects:
        fact = deepcopy(sample)
        fact["subject_id"] = subject.id
        fact["primary_binding"]["artifact_id"] = str(subject.artifact_id)
        facts.append(fact)
    return {"artifacts": facts}


@pytest.mark.parametrize("task_id,count,expected_jobs", (("streams", 128, 8), ("hint", 129, 9)))
def test_scale_subjects_are_all_accepted_in_bounded_batches(
    tmp_path, task_id, count, expected_jobs
):
    planner, work, question, descriptor, resource, subjects = _delivery(tmp_path, task_id, count)
    seen, requests = [], []
    try:
        delivery = CompiledObservationDelivery(planner)
        while (prepared := delivery.request(work, question)) is not None:
            request, _ = prepared
            assert 1 <= len(request.subjects) <= 16
            assert request.question_sha256 == question.question_sha256
            assert request.timeout_seconds == 300
            # A new delivery object must reuse the in-flight request, not drop
            # it or allocate another batch before acceptance.
            assert CompiledObservationDelivery(planner).request(work, question)[0] == request
            result = ContentObservationResultBuilder(descriptor, request).observed(_facts(request))
            CompiledObservationDelivery(planner).accept(
                work, ContentObservationEvidence(request=request, result=result)
            )
            requests.append(request)
            seen.extend(request.subjects)
        assert seen == list(subjects)
        assert len(requests) == expected_jobs
        accepted = None
        for _ in range(100):
            accepted = planner.state.accepted_observations.seal_step(
                question, interface=resource.interface
            )
            if accepted is not None:
                break
        assert accepted is not None
        assert accepted.question == question
        assert accepted.question.scope.artifact_count == count
        assert accepted.results.result_count == expected_jobs
    finally:
        planner.state.engine.dispose()


def test_whole_scope_provenance_is_never_split_to_match_the_batch_preference(tmp_path):
    planner, work, question, descriptor, resource, subjects = _delivery(tmp_path, "provenance", 129)
    try:
        assert resource.interface.partitioning == "whole-scope"
        support = descriptor.support_for(question.observer_contract.id)
        assert support.preferred_subject_batch_size == 1
        request, _ = CompiledObservationDelivery(planner).request(work, question)
        assert request.subjects == subjects
        assert request.question_sha256 == question.question_sha256
    finally:
        planner.state.engine.dispose()
