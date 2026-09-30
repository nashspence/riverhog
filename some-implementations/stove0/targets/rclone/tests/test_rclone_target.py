from __future__ import annotations

import hashlib
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
)
from a_stove0_rclone_target.contracts import RCLONE_DELIVER_OPERATION
from a_stove0_rclone_target.target import (
    RcloneDestination,
    RcloneEffectTargetService,
    _planned_destinations,
    _write_delivery_manifest,
)
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
from stove0_target_support import TargetEffectCommitUncertain


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
