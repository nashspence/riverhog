from __future__ import annotations

import hashlib
from dataclasses import fields, replace

import pytest
from native_units import unit_budget
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalValidationObservation, JournalValidationSession
from riverhog_provenance import journal_session as owner
from riverhog_provenance._journal_prefix import PrefixState
from riverhog_provenance._session_checkpoint import ValidationCheckpoint
from sha256_continuation import CheckpointSHA256
from test_journal_session import finish
from test_journal_session_growth import SessionConnection, progress_dimensions, session_observer
from test_retained_journal_session import OwnedSource, directory, open_session
from test_semantic_edge import observed_work

from tests.support.sqlite_observation import observe_admitted_scope


def test_received_empty_pulls_have_live_sequence_without_parent_publication(journal, catalog):
    with JournalValidationSession(
        (b"", b"", b"", journal), attempt_id="a" * 32, catalog=catalog
    ) as session:
        assert session.progress().observation_sequence == 0
        for count in range(1, 4):
            observation = session.advance(unit_budget())
            assert isinstance(observation, JournalValidationObservation)
            assert observation.observation_sequence == count
            assert observation.attempt_id == "a" * 32 and observation.worker_epoch == 0
            assert observation.phase == "source" and not observation.complete
            assert observation.accepted_frames == observation.accepted_octets == 0
            assert observation.source_octets == 0
            assert session._ledger.load().checkpoint.version == 0
            assert session.progress() == observation
        completed = finish(session)
        assert completed.complete and completed.accepted_frames == 1
        assert session.advance() == session.progress() == completed
        assert [field.name for field in fields(completed)] == [
            "attempt_id",
            "worker_epoch",
            "phase",
            "observation_sequence",
            "accepted_frames",
            "accepted_octets",
            "accepted_sha256",
            "source_octets",
            "complete",
        ]


def test_replacement_observation_resets_independently_of_surviving_private_work(
    tmp_path, journal, catalog, journal_constructions
):
    path = directory(tmp_path)
    source = OwnedSource(journal, catalog)
    with open_session(
        path, source, catalog, create=True, journal_constructions=journal_constructions
    ) as first:
        before = first.advance(unit_budget(source_octets=7))
        version = first._ledger.load().checkpoint.version
        assert before.observation_sequence == 1 and before.source_octets == 7
    with open_session(
        path, source, catalog, epoch=1, journal_constructions=journal_constructions
    ) as replacement:
        current = replacement.progress()
        assert current.attempt_id == before.attempt_id and current.worker_epoch == 1
        assert current.observation_sequence == 0
        assert current.source_octets == before.source_octets
        assert current.accepted_sha256 == before.accepted_sha256
        assert replacement._ledger.load().checkpoint.version == version + 1
        assert replacement.advance(unit_budget(source_octets=7)).observation_sequence == 1


@pytest.mark.growth_witness(
    "riverhog-native-volatile-observation-rendering/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_session._processed_progress",
)
def test_actual_observation_rendering_has_zero_checkpoint_representation_work(
    tmp_path, journal, catalog, monkeypatch, record_property, numeric_policy, journal_constructions
):
    operation = owner._processed_progress
    binding = growth_binding(operation)
    projection = project_bindings((binding,), numeric_policy=numeric_policy)
    samples = []
    record_property("growth_samples", samples)

    for width in (32, 256, 2048):
        source = OwnedSource(journal, catalog)
        source.binding = replace(source.binding, directory_id="d" * width)
        root = tmp_path / str(width)
        root.mkdir()
        path = directory(root)
        construction = journal_constructions.retain()
        with (
            observe_admitted_scope(
                construction.scope,
                SessionConnection,
                initialize=lambda _db: None,
                recording=lambda db: db.recording,
            ),
            JournalValidationSession.open_retained(
                path,
                source,
                attempt_id="a" * 32,
                worker_epoch=7,
                create=True,
                catalog=catalog,
                construction=construction,
            ) as session,
        ):
            for completed in (False, True):
                if completed:
                    finish(session)
                before = session._ledger.load()
                dimensions = progress_dimensions(session, before)
                db = session._store.db
                changes = db.total_changes

                def forbidden(*_args, **_kwargs):
                    pytest.fail("observation rendered a recovery representation")

                with monkeypatch.context() as patch:
                    patch.setattr(ValidationCheckpoint, "to_json", forbidden)
                    patch.setattr(CheckpointSHA256, "from_state", forbidden)
                    patch.setattr(owner, "canonical_json_bytes", forbidden)
                    with session_observer(session, dimensions, operation) as (observed, receive):
                        with db.observe():
                            observation = operation(
                                session.attempt_id,
                                session._ledger,
                                before,
                                session._input.cursor.byte_offset,
                                session._observation_sequence,
                            )
                    metrics = observed_work(db, {key: 0 for key in db.metrics}, changes, 0)
                    sample = receive(observed, metrics)
                validate_execution_samples(
                    projection, binding.declaration.id, [sample], numeric_policy=numeric_policy
                )
                samples.append(sample)
                assert observation.complete is completed
                assert (
                    observation.accepted_sha256
                    == hashlib.sha256(journal if completed else b"").hexdigest()
                )
                assert all(
                    sample["metrics"][name] == 0
                    for name in (
                        "sql_statements",
                        "returned_rows",
                        "changed_rows",
                        "native_hashed_octets",
                        "decoded_json_octets",
                        "encoded_json_octets",
                        "source_payload_octets",
                    )
                )


def test_immutable_prefix_retains_its_own_original_sha_continuation():
    original = CheckpointSHA256()
    state = PrefixState(0, 0, None, None, original.export_state(), False, original)
    original.update_bytes(b"later caller work")
    assert state.hexdigest == hashlib.sha256(b"").hexdigest()
    detached = state._continuation.copy()
    detached.update_bytes(b"later prepared work")
    assert state.hexdigest == hashlib.sha256(b"").hexdigest()
    assert state.to_json() == PrefixState.empty().to_json()
    assert PrefixState.from_json(state.to_json()) == state
