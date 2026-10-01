from __future__ import annotations

import hashlib
import json
import subprocess
import threading
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
)
from a_stove0_rclone_target import target as target_support
from a_stove0_rclone_target.contracts import RCLONE_DELIVER_OPERATION
from a_stove0_rclone_target.target import (
    RcloneDestination,
    RcloneEffectTargetService,
    _planned_destinations,
    _verify_selected_inputs,
    _write_delivery_manifest,
)
from riverhog_canonical_json import require_canonical_json
from riverhog_client.processing import ProcessingWorkspace
from riverhog_materialization import DestinationRules
from riverhog_protocol import canonical_json_bytes
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationRequest,
    ContentObservationRequestPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
)
from stove0_observer_support import ContentObservationResultBuilder
from stove0_protocol import ArtifactSelection, CollectionRootIdentityRef, WorkArtifactSubject
from stove0_target_support import InputArtifact, TargetEffectCommitUncertain, TargetJobRequest


def _destination() -> RcloneDestination:
    return RcloneDestination(
        identity="a" * 64,
        remote="remote:archive",
        naming_rules=DestinationRules(
            windows_names=False,
            case_sensitive=True,
            unicode_equivalence="exact",
            component_bytes=255,
            relative_path_bytes=4096,
        ),
    )


def _subject(name: str, artifact_id: str) -> WorkArtifactSubject:
    return WorkArtifactSubject(
        id=name,
        role="stove0.source/v1",
        collection=CollectionRootIdentityRef(
            collection_id="1",
            archive_root_sha256="b" * 64,
            artifact_set_identity="c" * 64,
        ),
        artifact_id=artifact_id,
        bytes="1",
        sha256="d" * 64,
    )


def _hint_evidence(
    subjects: tuple[WorkArtifactSubject, ...],
    hints: tuple[dict[str, object] | None, ...],
) -> ContentObservationEvidence:
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="fixture.hint-observer/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "e" * 64,
            contracts=(
                ObserverContractSupport.from_contract(MATERIALIZATION_HINT_OBSERVER_CONTRACT),
            ),
        )
    )
    request = ContentObservationRequest.seal(
        ContentObservationRequestPayload(
            work_id="f" * 64,
            observer_registration_id="hint-observer",
            observer_descriptor_sha256=descriptor.descriptor_sha256,
            observer_contract_id=MATERIALIZATION_HINT_OBSERVER_CONTRACT.id,
            observer_contract_sha256=MATERIALIZATION_HINT_OBSERVER_CONTRACT.contract_sha256,
            read_actions=("read-provenance",),
            subjects=subjects,
        )
    )
    occurrence = {
        "scope": "external",
        "journal_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
        "entry": {
            "entry_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
            "sequence": "0",
            "json_sha256": "e" * 64,
        },
        "assertion_id": "urn:uuid:33333333-3333-4333-8333-333333333333",
        "object_id": "urn:uuid:44444444-4444-4444-8444-444444444444",
        "object_type": "occurrence",
    }
    result = ContentObservationResultBuilder(descriptor, request).observed(
        {
            "artifacts": [
                {
                    "subject_id": subject.id,
                    "primary_binding": {
                        "artifact_id": str(subject.artifact_id),
                        "journal": {
                            "journal_id": occurrence["journal_id"],
                            "through": occurrence["entry"],
                            "prefix_sha256": "f" * 64,
                            "prefix_bytes": "100",
                        },
                        "delivery_association_id": (
                            "urn:uuid:55555555-5555-4555-8555-555555555555"
                        ),
                    },
                    "occurrence": occurrence,
                    "materialization_hint": hint,
                }
                for subject, hint in zip(subjects, hints, strict=True)
            ]
        }
    )
    return ContentObservationEvidence(request=request, result=result)


def test_rclone_uses_forwarded_exact_hint_and_id_fallback() -> None:
    first, second = _subject("one", "1" * 64), _subject("two", "2" * 64)
    evidence = _hint_evidence((first, second), ({"components": ["Album", "clip.mp4"]}, None))
    planned = _planned_destinations(
        (evidence,),
        ArtifactSelection.seal(
            tuple(
                subject.model_copy(update={"role": "stove0.rclone.source/v1"})
                for subject in (first, second)
            )
        ).ref(),
        _destination().naming_rules,
    )
    assert planned[first.id].relative_path == "1/files/Album/clip.mp4"
    assert planned[first.id].reason == "hint"
    assert planned[first.id].occurrence["object_type"] == "occurrence"
    assert planned[first.id].primary_binding["artifact_id"] == first.artifact_id
    assert planned[second.id].relative_path == f"1/artifacts/22/{second.artifact_id}"
    assert planned[second.id].reason == "no-hint"


def test_rclone_cannot_infer_missing_advice_without_accepted_evidence() -> None:
    subject = _subject("one", "1" * 64)
    with pytest.raises(ValueError, match="requires accepted canonical hint evidence"):
        _planned_destinations(
            (), ArtifactSelection.seal((subject,)).ref(), _destination().naming_rules
        )


def test_rclone_rejects_hint_evidence_for_another_selection_and_resolves_collisions() -> None:
    first, second = _subject("one", "1" * 64), _subject("two", "2" * 64)
    evidence = _hint_evidence(
        (first, second),
        ({"components": ["same.bin"]}, {"components": ["same.bin"]}),
    )
    selection = ArtifactSelection.seal((first, second)).ref()
    planned = _planned_destinations((evidence,), selection, _destination().naming_rules)
    assert {item.reason for item in planned.values()} == {"destination-collision"}
    assert all("/artifacts/" in item.relative_path for item in planned.values())
    with pytest.raises(ValueError, match="input selection"):
        _planned_destinations(
            (evidence,),
            ArtifactSelection.seal((first,)).ref(),
            _destination().naming_rules,
        )
    limited = replace(_destination().naming_rules, relative_path_bytes=1)
    with pytest.raises(ValueError, match="collection-qualified path"):
        _planned_destinations((evidence,), selection, limited)


def test_rclone_checks_exact_target_selection_before_delivery() -> None:
    first, second = _subject("one", "1" * 64), _subject("two", "2" * 64)
    evidence = _hint_evidence((first, second), (None, None))
    selected = tuple(
        subject.model_copy(update={"role": "stove0.rclone.source/v1"})
        for subject in (first, second)
    )
    selection = ArtifactSelection.seal(selected).ref()
    planned = _planned_destinations((evidence,), selection, _destination().naming_rules)
    inputs = tuple(
        InputArtifact.model_validate(subject.model_dump(mode="json")) for subject in selected
    )
    _verify_selected_inputs(inputs, planned, selection)

    wrong_role = inputs[1].model_copy(update={"role": "stove0.other/v1"})
    with pytest.raises(ValueError, match="exact input selection"):
        _verify_selected_inputs((inputs[0], wrong_role), planned, selection)
    other_member = inputs[1].model_copy(update={"artifact_id": "3" * 64})
    with pytest.raises(ValueError, match="exact input selection"):
        _verify_selected_inputs((inputs[0], other_member), planned, selection)


def test_rclone_streamed_manifest_is_canonical_and_covers_every_input(tmp_path: Path) -> None:
    first, second = _subject("one", "1" * 64), _subject("two", "2" * 64)
    selection = ArtifactSelection.seal((first, second)).ref()
    rows = ((first.id, 1, {"z": 1}), (second.id, 1, {"a": 2}))
    path = tmp_path / "manifest.json"
    count, total, sha256 = _write_delivery_manifest(
        path, delivery_id="e" * 64, selection=selection, entries=iter(rows)
    )
    expected = canonical_json_bytes(
        {
            "format": "stove0-rclone-delivery-manifest/v1",
            "delivery_id": "e" * 64,
            "source_selection_sha256": selection.selection_sha256,
            "artifacts": [{"z": 1}, {"a": 2}],
        }
    )
    assert (count, total, sha256) == (2, 2, hashlib.sha256(expected).hexdigest())
    assert path.read_bytes() == expected
    with pytest.raises(ValueError, match="ordered by subject"):
        _write_delivery_manifest(
            tmp_path / "unordered.json",
            delivery_id="e" * 64,
            selection=selection,
            entries=iter(reversed(rows)),
        )
    with pytest.raises(ValueError, match="sealed selection"):
        _write_delivery_manifest(
            tmp_path / "incomplete.json",
            delivery_id="e" * 64,
            selection=selection,
            entries=iter(rows[:1]),
        )


def test_rclone_execution_delivers_canonical_manifest_for_exact_opaque_members(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = b"x"
    subjects = tuple(
        _subject(name, character * 64).model_copy(
            update={"sha256": hashlib.sha256(payload).hexdigest()}
        )
        for name, character in (("one", "1"), ("two", "2"))
    )
    evidence = _hint_evidence(subjects, ({"components": ["Album", "clip.bin"]}, None))
    inputs = tuple(InputArtifact.model_validate(item.model_dump(mode="json")) for item in subjects)
    selection = ArtifactSelection.seal(subjects).ref()
    destination = _destination()
    service = RcloneEffectTargetService(
        state_root=tmp_path / "state",
        workspace_root=tmp_path / "workspace",
        destination=destination,
        image_id="sha256:" + "b" * 64,
        implementation_version="0.1.0",
    )
    plan = SimpleNamespace(
        target_options={
            "destination_identity": destination.identity,
            "destination_rules_sha256": destination.rules_sha256,
        },
        inputs=SimpleNamespace(selection=selection),
        plan_sha256="a" * 64,
    )
    request = cast(
        TargetJobRequest,
        SimpleNamespace(
            declaration=SimpleNamespace(
                plan=plan,
                job_id="c" * 64,
                controller_evidence=SimpleNamespace(
                    execution_envelope=SimpleNamespace(
                        workflow_plan=SimpleNamespace(observations=(evidence,))
                    )
                ),
            )
        ),
    )
    workspace = ProcessingWorkspace.open(
        service.workspace_root, execution_id="c" * 64, declared_protection="memory-backed"
    )
    execution = MagicMock()
    execution.__enter__.return_value = execution
    execution.open_workspace.return_value = workspace
    execution.iter_inputs.side_effect = lambda: iter((item, item) for item in inputs)
    execution.prepare_inputs.return_value.__enter__.return_value.download.side_effect = (
        lambda _claimed, path: path.write_bytes(payload)
    )
    monkeypatch.setattr(
        target_support.TargetExecutionRuntime, "from_request", lambda *_a, **_k: execution
    )
    committed = []

    def commit(_destination, *, objects_root, manifest_path, **_kwargs):
        encoded = manifest_path.read_bytes()
        require_canonical_json(encoded)
        manifest = json.loads(encoded)
        for item in manifest["artifacts"]:
            assert (objects_root / item["delivered_path"]).read_bytes() == payload
        committed.append(manifest)

    monkeypatch.setattr(RcloneDestination, "commit", commit)
    try:
        service._execute(request, 1, threading.Event(), MagicMock())
        assert len(committed) == 1
        rows = committed[0]["artifacts"]
        assert [item["artifact_id"] for item in rows] == [
            str(item.artifact_id) for item in subjects
        ]
        assert rows[0]["delivered_path"] == "1/files/Album/clip.bin"
        assert rows[0]["materialization_reason"] == "hint"
        assert rows[1]["delivered_path"] == f"1/artifacts/22/{subjects[1].artifact_id}"
        assert rows[1]["materialization_reason"] == "no-hint"
        receipt = execution.effect_success.call_args.args[0]
        assert receipt["artifact_count"] == 2 and receipt["total_bytes"] == 2
        execution.effect_success.assert_called_once()
    finally:
        service.close()


def test_generic_rclone_descriptor_has_one_effect_operation(tmp_path: Path) -> None:
    target = RcloneEffectTargetService(
        state_root=tmp_path / "state",
        workspace_root=tmp_path / "workspace",
        destination=_destination(),
        image_id="sha256:" + "b" * 64,
        implementation_version="0.1.0",
    )
    try:
        descriptor = target.descriptor()
        assert descriptor.protocol == "stove0-effect-target/v1"
        assert descriptor.implementation_id == "a-stove0-rclone-target/v1"
        assert tuple(item.operation_id for item in descriptor.operations) == (
            RCLONE_DELIVER_OPERATION.id,
        )
        assert RCLONE_DELIVER_OPERATION.inputs[0].role == "*"
        assert RCLONE_DELIVER_OPERATION.source_collection_retirement_permitted
    finally:
        target.close()


def test_delivery_verifies_bytes_before_marker_and_reads_marker_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    objects = tmp_path / "objects"
    objects.mkdir()
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(b'{"delivery":"exact"}')
    calls: list[tuple[str, ...]] = []

    def run(command: list[str], **_kwargs: Any) -> SimpleNamespace:
        calls.append(tuple(command))
        if command[1] == "cat":
            _kwargs["stdout"].write(manifest.read_bytes())
        return SimpleNamespace(stdout=b"")

    monkeypatch.setattr(subprocess, "run", run)
    _destination().commit(delivery_id="c" * 64, objects_root=objects, manifest_path=manifest)
    assert [command[1] for command in calls] == ["copy", "check", "copyto", "cat"]
    assert calls[1][2] == "--download"
    assert calls[2][-1] == calls[3][-1]


def test_delivery_readback_mismatch_is_commit_uncertain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    objects = tmp_path / "objects"
    objects.mkdir()
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(b"sealed")

    def run(command: list[str], **_kwargs: Any) -> SimpleNamespace:
        if command[1] == "cat":
            _kwargs["stdout"].write(b"different")
        return SimpleNamespace(stdout=b"")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(TargetEffectCommitUncertain):
        _destination().commit(delivery_id="c" * 64, objects_root=objects, manifest_path=manifest)
