from __future__ import annotations

import copy
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "provider_qualification_checkpoint", ROOT / "scripts/provider_qualification_checkpoint.py"
)
assert SPEC is not None and SPEC.loader is not None
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)
NOW = datetime(2026, 9, 26, 12, tzinfo=UTC)
KEY = "a" * 16
QUOTA = 2 * 1024**3
TIMEOUT = 72 * 3600


def timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


@pytest.fixture
def checkpoint():  # type: ignore[no-untyped-def]
    return {
        "phase": "restore-pending",
        "qualification_key_id": KEY,
        "retrieval_job_id": "job-42",
        "collection_id": 42,
        "restore_deadline_at": timestamp(NOW + timedelta(days=3)),
        "run_id": "test-run",
        "source_sha": "b" * 40,
        "checkpoint_sha256": "c" * 64,
    }


@pytest.fixture
def snapshot():  # type: ignore[no-untyped-def]
    return {
        "key": {
            "id": KEY,
            "app": module.APP,
            "revoked_at": None,
            "expires_at": None,
            "has_access": True,
            "monthly_download_quota_bytes": QUOTA,
        },
        "collection": {"id": 42, "created_by_principal_id": module.APP, "created_by_key_id": KEY},
        "job": {
            "id": "job-42",
            "principal_id": module.APP,
            "initiated_by_key_id": KEY,
            "plan_id": "plan-42",
            "plan_etag": "d" * 64,
            "state": "requested",
            "created_at": timestamp(NOW - timedelta(hours=1)),
            "expires_at": None,
            "completed_at": None,
            "canceled_at": None,
        },
        "plan": {
            "id": "plan-42",
            "principal_id": module.APP,
            "initiated_by_key_id": KEY,
            "state": "consumed",
            "etag": "d" * 64,
            "collection_ids": [42],
        },
    }


def validate(checkpoint, snapshot):  # type: ignore[no-untyped-def]
    module.validate_snapshot(
        checkpoint,
        snapshot,
        now=NOW,
        pending_timeout_seconds=TIMEOUT,
        monthly_download_quota_bytes=QUOTA,
    )


@pytest.mark.parametrize("phase", ["restore-requested", "restore-pending"])
@pytest.mark.parametrize("state", ["requested", "ready"])
def test_live_job_remains_owned_by_same_key_across_continuations(
    checkpoint, snapshot, phase, state
):  # type: ignore[no-untyped-def]
    checkpoint["phase"] = phase
    snapshot["job"]["state"] = state
    snapshot["job"]["expires_at"] = timestamp(NOW + timedelta(days=2)) if state == "ready" else None
    validate(checkpoint, snapshot)
    # A new credential token/hash is intentionally not part of the checkpoint or projection.
    validate(copy.deepcopy(checkpoint), copy.deepcopy(snapshot))


@pytest.mark.parametrize("section", ["key", "collection", "job", "plan"])
def test_missing_snapshot_rows_fail_closed(checkpoint, snapshot, section):  # type: ignore[no-untyped-def]
    snapshot[section] = None
    with pytest.raises(module.CheckpointStateError, match="snapshot is missing"):
        validate(checkpoint, snapshot)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("key", "id", "e" * 16),
        ("key", "app", "another-app"),
        ("key", "revoked_at", timestamp(NOW)),
        ("key", "has_access", False),
        ("key", "monthly_download_quota_bytes", None),
        ("collection", "id", 43),
        ("collection", "created_by_principal_id", "another-app"),
        ("collection", "created_by_key_id", "e" * 16),
        ("job", "id", "another-job"),
        ("job", "principal_id", "another-app"),
        ("job", "initiated_by_key_id", "e" * 16),
        ("job", "initiated_by_key_id", None),
        ("plan", "principal_id", "another-app"),
        ("plan", "initiated_by_key_id", "e" * 16),
        ("job", "plan_id", "another-plan"),
        ("job", "plan_etag", "f" * 64),
        ("plan", "state", "ready"),
        ("plan", "etag", None),
        ("plan", "collection_ids", [43]),
        ("plan", "collection_ids", [42, 43]),
        ("plan", "collection_ids", []),
        ("job", "completed_at", timestamp(NOW)),
        ("job", "canceled_at", timestamp(NOW)),
    ],
)
def test_owner_plan_and_lifecycle_mismatches_fail_closed(
    checkpoint, snapshot, section, field, value
):  # type: ignore[no-untyped-def]
    snapshot[section][field] = value
    with pytest.raises(module.CheckpointStateError):
        validate(checkpoint, snapshot)


@pytest.mark.parametrize("state", ["canceled", "expired", "failed", "completed", "unknown", None])
def test_nonlive_retrieval_cannot_be_uploaded(checkpoint, snapshot, state):  # type: ignore[no-untyped-def]
    snapshot["job"]["state"] = state
    with pytest.raises(module.CheckpointStateError, match="not live"):
        validate(checkpoint, snapshot)


@pytest.mark.parametrize(
    "field", ["id", "principal_id", "initiated_by_key_id", "completed_at", "canceled_at"]
)
def test_incomplete_job_projection_fails_closed(checkpoint, snapshot, field):  # type: ignore[no-untyped-def]
    del snapshot["job"][field]
    with pytest.raises(module.CheckpointStateError):
        validate(checkpoint, snapshot)


@pytest.mark.parametrize(
    "value",
    ["not-a-time", "2026-09-26T12:00:00", timestamp(NOW), timestamp(NOW - timedelta(seconds=1))],
)
def test_invalid_or_expired_ready_lease_rejected(checkpoint, snapshot, value):  # type: ignore[no-untyped-def]
    snapshot["job"].update(state="ready", expires_at=value)
    with pytest.raises(module.CheckpointStateError):
        validate(checkpoint, snapshot)


@pytest.mark.parametrize("age", [timedelta(hours=72), timedelta(hours=73), timedelta(seconds=-1)])
def test_pending_expiry_boundary_and_future_creation_rejected(checkpoint, snapshot, age):  # type: ignore[no-untyped-def]
    snapshot["job"]["created_at"] = timestamp(NOW - age)
    with pytest.raises(module.CheckpointStateError, match="pending lifetime"):
        validate(checkpoint, snapshot)


@pytest.mark.parametrize("delta", [timedelta(0), timedelta(seconds=1)])
def test_owner_expiry_covers_deadline_plus_margin(checkpoint, snapshot, delta):  # type: ignore[no-untyped-def]
    snapshot["key"]["expires_at"] = timestamp(NOW + timedelta(days=4) + delta)
    validate(checkpoint, snapshot)


@pytest.mark.parametrize(
    "expiry", [NOW - timedelta(seconds=1), NOW, NOW + timedelta(days=4, seconds=-1)]
)
def test_owner_expiry_cannot_fall_short_of_required_lifetime(checkpoint, snapshot, expiry):  # type: ignore[no-untyped-def]
    snapshot["key"]["expires_at"] = timestamp(expiry)
    with pytest.raises(module.CheckpointStateError, match="deadline plus margin"):
        validate(checkpoint, snapshot)


def test_restored_checkpoint_cannot_regress_to_requested(checkpoint, snapshot):  # type: ignore[no-untyped-def]
    checkpoint["phase"] = "restored"
    with pytest.raises(module.CheckpointStateError, match="not live"):
        validate(checkpoint, snapshot)
    snapshot["job"].update(state="ready", expires_at=timestamp(NOW + timedelta(days=1)))
    validate(checkpoint, snapshot)


def test_verified_checkpoint_is_completed_cleanup_not_active_restore(checkpoint, snapshot):  # type: ignore[no-untyped-def]
    checkpoint["phase"] = "verified"
    with pytest.raises(module.CheckpointStateError, match="acknowledged"):
        validate(checkpoint, snapshot)
    snapshot["job"].update(state="completed", completed_at=timestamp(NOW))
    validate(checkpoint, snapshot)


@pytest.mark.parametrize("phase", ["cleaned", "failed", "unknown"])
def test_terminal_or_unknown_checkpoint_is_never_active_continuation(checkpoint, snapshot, phase):  # type: ignore[no-untyped-def]
    checkpoint["phase"] = phase
    with pytest.raises(module.CheckpointStateError, match="cannot be uploaded"):
        validate(checkpoint, snapshot)


@pytest.mark.parametrize("field", ["qualification_key_id", "retrieval_job_id", "collection_id"])
def test_restore_checkpoint_requires_all_identities(checkpoint, snapshot, field):  # type: ignore[no-untyped-def]
    checkpoint[field] = None
    with pytest.raises(module.CheckpointStateError):
        validate(checkpoint, snapshot)


def test_pre_retrieval_and_initial_checkpoints_do_not_invent_jobs(checkpoint, snapshot):  # type: ignore[no-untyped-def]
    checkpoint.update(phase="deep-archive-cache-observed", retrieval_job_id=None)
    validate(checkpoint, snapshot)
    checkpoint.update(phase="created", qualification_key_id=None, collection_id=None)
    validate(checkpoint, {})
    checkpoint["phase"] = "restore-pending"
    with pytest.raises(module.CheckpointStateError):
        validate(checkpoint, {})


def write_state(path, checkpoint):  # type: ignore[no-untyped-def]
    (path / "checkpoint.json").write_text(json.dumps(checkpoint))
    (path / "database.dump").write_bytes(b"dummy dump, not a real PostgreSQL dump")
    (path / "continuation.json").write_text(json.dumps(module.pair_manifest(checkpoint, path)))


@pytest.mark.parametrize("target", ["checkpoint.json", "database.dump", "continuation.json"])
def test_pair_rejects_changed_or_mixed_artifact_bytes(tmp_path, checkpoint, target):  # type: ignore[no-untyped-def]
    write_state(tmp_path, checkpoint)
    module.verify_pair(checkpoint, tmp_path)
    with (tmp_path / target).open("ab") as handle:
        handle.write(b" changed")
    with pytest.raises(module.CheckpointStateError):
        module.verify_pair(checkpoint, tmp_path)


def test_unsealed_legacy_continuation_requires_explicit_restart(tmp_path, checkpoint):  # type: ignore[no-untyped-def]
    write_state(tmp_path, checkpoint)
    (tmp_path / "continuation.json").unlink()
    with pytest.raises(module.CheckpointStateError, match="restart"):
        module.verify_pair(checkpoint, tmp_path)


class FakeDatabase:
    def __init__(self, snapshot, fail=None, mutate=None):  # type: ignore[no-untyped-def]
        self.snapshot = snapshot
        self.fail = fail
        self.mutate = mutate
        self.commands = []

    def __call__(self, command, **kwargs):  # type: ignore[no-untyped-def]
        self.commands.append(command)
        if self.fail is not None and self.fail in command:
            raise module.CheckpointStateError("injected database failure")
        if "pg_dump" in command:
            kwargs["stdout"].write(b"exact captured dump")
        if "pg_restore" in command:
            assert kwargs["stdin"].read() == b"exact captured dump"
            assert "--exit-on-error" in command
        if "psql" in command:
            assert kwargs["input"] == module.SNAPSHOT_SQL.encode()
            if self.mutate:
                self.mutate()
            return SimpleNamespace(stdout=json.dumps(self.snapshot).encode())
        return SimpleNamespace(stdout=b"")


def capture(tmp_path, checkpoint, database):  # type: ignore[no-untyped-def]
    module.capture_snapshot(
        checkpoint,
        tmp_path,
        ["docker", "compose"],
        run=database,
        clock=lambda: NOW,
        pending_timeout_seconds=TIMEOUT,
        monthly_download_quota_bytes=QUOTA,
    )


def test_capture_verifies_exact_restored_dump_before_sealing(tmp_path, checkpoint, snapshot):  # type: ignore[no-untyped-def]
    write_state(tmp_path, checkpoint)
    database = FakeDatabase(snapshot)
    capture(tmp_path, checkpoint, database)
    module.verify_pair(checkpoint, tmp_path)
    assert database.commands[0] == ["docker", "compose", "stop", "app"]
    restore = next(c for c in database.commands if "pg_restore" in c)
    query = next(c for c in database.commands if "psql" in c)
    restored_db = restore[restore.index("--dbname") + 1]
    assert restored_db.startswith("qualification_verify_")
    assert query[query.index("--dbname") + 1] == restored_db
    assert database.commands[-1][-1] == restored_db
    assert "dropdb" in database.commands[-1]
    assert (tmp_path / "database.dump").read_bytes() == b"exact captured dump"


@pytest.mark.parametrize("failure", ["pg_dump", "createdb", "pg_restore", "psql", "dropdb"])
def test_database_failures_cannot_leave_a_publishable_seal(tmp_path, checkpoint, snapshot, failure):  # type: ignore[no-untyped-def]
    write_state(tmp_path, checkpoint)
    database = FakeDatabase(snapshot, fail=failure)
    with pytest.raises(module.CheckpointStateError):
        capture(tmp_path, checkpoint, database)
    assert not (tmp_path / "continuation.json").exists()
    if failure in {"pg_restore", "psql"}:
        assert "dropdb" in database.commands[-1]


def test_canceled_dump_fails_even_with_preexisting_valid_pair(tmp_path, checkpoint, snapshot):  # type: ignore[no-untyped-def]
    write_state(tmp_path, checkpoint)
    snapshot["job"]["state"] = "canceled"
    database = FakeDatabase(snapshot)
    with pytest.raises(module.CheckpointStateError, match="not live"):
        capture(tmp_path, checkpoint, database)
    assert not (tmp_path / "continuation.json").exists()
    assert "dropdb" in database.commands[-1]


def test_checkpoint_change_during_capture_is_not_sealed(tmp_path, checkpoint, snapshot):  # type: ignore[no-untyped-def]
    write_state(tmp_path, checkpoint)
    database = FakeDatabase(
        snapshot, mutate=lambda: (tmp_path / "checkpoint.json").write_text("{}")
    )
    with pytest.raises(module.CheckpointStateError, match="checkpoint changed"):
        capture(tmp_path, checkpoint, database)
    assert not (tmp_path / "continuation.json").exists()


@pytest.mark.parametrize("phase", ["cleaned", "failed"])
def test_terminal_capture_removes_resumable_state_without_database_commands(
    tmp_path, checkpoint, phase
):  # type: ignore[no-untyped-def]
    write_state(tmp_path, checkpoint)
    checkpoint["phase"] = phase
    database = FakeDatabase({})
    capture(tmp_path, checkpoint, database)
    assert database.commands == []
    assert not (tmp_path / "continuation.json").exists()
    assert not (tmp_path / "database.dump").exists()


class FakeKeys:
    def __init__(self, result=None, fail=False):  # type: ignore[no-untyped-def]
        self.result = result or {"id": KEY, "app": module.APP, "status": "revoked"}
        self.fail = fail
        self.revoked = []
        self.closed = False

    def revoke_app_key(self, app, key_id):  # type: ignore[no-untyped-def]
        self.revoked.append((app, key_id))
        if self.fail:
            raise module.CheckpointStateError("revoke failed")
        return self.result

    def close(self):  # type: ignore[no-untyped-def]
        self.closed = True


@pytest.mark.parametrize("phase", sorted(module.PRE_RETRIEVAL_PHASES | module.RETRIEVAL_PHASES))
def test_pending_or_retryable_phase_never_constructs_revocation_client(checkpoint, phase):  # type: ignore[no-untyped-def]
    checkpoint["phase"] = phase

    def forbidden():  # type: ignore[no-untyped-def]
        pytest.fail("nonterminal continuation must preserve its owner")

    module.revoke_terminal_owner(checkpoint, forbidden)


@pytest.mark.parametrize("phase", ["cleaned", "failed"])
def test_terminal_success_or_failure_revokes_only_checkpointed_owner(checkpoint, phase):  # type: ignore[no-untyped-def]
    checkpoint["phase"] = phase
    client = FakeKeys()
    module.revoke_terminal_owner(checkpoint, lambda: client)
    assert client.revoked == [(module.APP, KEY)]
    assert client.closed


@pytest.mark.parametrize(
    "result",
    [
        {"id": "f" * 16, "app": module.APP, "status": "revoked"},
        {"id": KEY, "app": "other", "status": "revoked"},
        {"id": KEY, "app": module.APP, "status": "active"},
    ],
)
def test_unconfirmed_terminal_revoke_is_a_failure(checkpoint, result):  # type: ignore[no-untyped-def]
    checkpoint["phase"] = "cleaned"
    client = FakeKeys(result=result)
    with pytest.raises(module.CheckpointStateError, match="not confirmed"):
        module.revoke_terminal_owner(checkpoint, lambda: client)
    assert client.closed


def test_revocation_failure_closes_client(checkpoint):  # type: ignore[no-untyped-def]
    checkpoint["phase"] = "failed"
    client = FakeKeys(fail=True)
    with pytest.raises(module.CheckpointStateError):
        module.revoke_terminal_owner(checkpoint, lambda: client)
    assert client.closed


def test_snapshot_query_never_selects_credentials() -> None:
    assert "token" not in module.SNAPSHOT_SQL
    assert "event_context" not in module.SNAPSHOT_SQL
    assert "SELECT *" not in module.SNAPSHOT_SQL
    for variable in ("key_id", "job_id", "collection_id"):
        assert f":'{variable}'" in module.SNAPSHOT_SQL


@pytest.mark.parametrize("phase", ["cleaned", "failed", "unknown"])
def test_terminal_or_unknown_pair_cannot_resume(checkpoint, tmp_path, phase):  # type: ignore[no-untyped-def]
    checkpoint["phase"] = phase
    with pytest.raises(module.CheckpointStateError, match="cannot be resumed"):
        module.verify_pair(checkpoint, tmp_path)
