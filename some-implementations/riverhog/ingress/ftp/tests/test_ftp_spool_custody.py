from __future__ import annotations

import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import Any

import a_riverhog_ftp_spool.landing as landing
import pytest
from a_riverhog_ftp_spool.completion import CompletionHandoff, completion_log_path
from a_riverhog_ftp_spool.config import FtpSpoolConfig, SourceConfig
from a_riverhog_ftp_spool.landing import FtpEventCursorChanged, FtpSpool
from a_riverhog_ftp_spool_client.events import (
    ATTEMPT_FAILED,
    CLAIM_PUBLISHED,
    CLAIM_REGISTERED,
    CUSTODY_READY,
)
from a_riverhog_ftp_spool_client.status import FtpSpoolStatus
from riverhog_client.producer import ProducedCollection
from riverhog_provenance import canonical_sidecar_path, create_observation_journal

from tests.provenance_observer import native_provenance_observer

REPO_ROOT = Path(__file__).resolve().parents[5]


def _config(tmp_path: Path) -> FtpSpoolConfig:
    return FtpSpoolConfig(
        host_id="test-host",
        riverhog_base_url="https://riverhog.invalid",
        riverhog_token="riverhog-token",
        api_token="adapter-token",
        sources=(
            SourceConfig(
                id="camera-a",
                root=tmp_path / "landing",
                ingest_source="ftp:camera-a",
                description="Camera A intake",
                tags=("source:ftp", "camera:a"),
                max_files=10,
                max_bytes=1024 * 1024,
                provenance="omit",
                provenance_omission_reason="Fixture intentionally has no host provenance.",
            ),
        ),
    )


class _Producer:
    calls: list[dict[str, Any]] = []
    fail_once = False

    def __init__(self, _api: object, **kwargs: object) -> None:
        self.kwargs = kwargs

    def publish(self, files: object, **kwargs: object) -> ProducedCollection:
        materialized = tuple(files)  # type: ignore[arg-type]
        self.__class__.calls.append(
            {
                "files": [
                    (item.path, item.source.read_bytes(), item.provenance) for item in materialized
                ],
                "kwargs": kwargs,
                "producer": self.kwargs,
            }
        )
        if self.__class__.fail_once:
            self.__class__.fail_once = False
            raise ConnectionError("finalized response was lost")
        return ProducedCollection(
            collection_id=41,
            archive_root_sha256="a" * 64,
            content_identity="b" * 64,
            receipt={"state": "finalized"},
        )


class _ControlledProducer:
    calls: list[str] = []
    successes: list[str] = []
    available = False
    permanently_failing_paths: set[str] = set()

    def __init__(self, _api: object, **_kwargs: object) -> None:
        pass

    def publish(self, files: object, **_kwargs: object) -> ProducedCollection:
        materialized = tuple(files)  # type: ignore[arg-type]
        path = str(materialized[0].path)
        self.__class__.calls.append(path)
        if not self.__class__.available or path in self.__class__.permanently_failing_paths:
            raise ConnectionError(f"publication unavailable for {path}")
        self.__class__.successes.append(path)
        return ProducedCollection(
            collection_id=len(self.__class__.successes),
            archive_root_sha256="a" * 64,
            content_identity="b" * 64,
            receipt={"state": "finalized"},
        )


class _SimulatedProcessStop(BaseException):
    pass


def _completed_upload(source: SourceConfig, relative: str, content: bytes) -> Path:
    path = source.root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    record = CompletionHandoff(source.root, source.id).complete(path)
    return source.root / record.custody


def _append_invalid_completion(source: SourceConfig, raw: bytes = b"invalid\n") -> None:
    CompletionHandoff(source.root, source.id)
    with completion_log_path(source.root).open("ab") as stream:
        stream.write(raw)


def _completion_event_rows(adapter: FtpSpool, source: SourceConfig) -> list[tuple[str, str]]:
    with closing(adapter._open_state(source)) as connection:
        return [
            (str(event_id), str(claim_id))
            for event_id, claim_id in connection.execute(
                "SELECT event_id, claim_id FROM completion_events ORDER BY event_id"
            )
        ]


def test_v1_claim_fixture_retains_payload_and_portable_provenance_identity() -> None:
    fixture_root = REPO_ROOT / "tests/fixtures/state/v1_0001/a-riverhog-ftp-spool"
    manifest = json.loads((fixture_root / "claim.json").read_text(encoding="utf-8"))
    payload = (fixture_root / "payload.bin").read_bytes()

    assert landing._read_manifest(fixture_root) == manifest
    assert hashlib.sha256(payload).hexdigest() == manifest["files"][0]["sha256"]
    assert landing._producer_provenance(manifest["files"][0]) == {
        "status": "omitted",
        "omission_reason": "Fixture intentionally has no host provenance.",
    }


def test_landing_adapter_reconciles_lost_response_without_releasing_custody(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    source = config.sources[0]
    payload = _completed_upload(source, "camera/clip.mp4", b"immutable camera payload")
    _Producer.calls = []
    _Producer.fail_once = True
    monkeypatch.setattr("a_riverhog_ftp_spool.landing.CollectionProducer", _Producer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]

    first = adapter.run_once()

    assert first["completed"] == 0
    assert len(first["failed"]) == 1  # type: ignore[arg-type]
    assert not payload.exists()
    claims = list((source.root / ".a-riverhog-ftp-spool" / "claims").iterdir())
    assert len(claims) == 1
    assert (claims[0] / "payload" / "camera" / "clip.mp4").read_bytes() == (
        b"immutable camera payload"
    )

    second = adapter.run_once()

    assert second["format"] == "a-riverhog-ftp-spool-pass/v1"
    assert second["completed"] == 1
    assert second["failed"] == []
    assert second["sources"] == ["camera-a"]
    assert second["source_results"] == [
        {
            "id": "camera-a",
            "claim_attempts": 1,
            "completed": 1,
            "failed": 0,
            "discovery_entries_examined": 0,
            "discovery_sweep_complete": True,
            "admission_deferred": False,
            "completion_failure_attempts": 0,
            "completion_failures": 0,
        }
    ]
    assert not claims[0].exists()
    assert [call["kwargs"]["idempotency_key"] for call in _Producer.calls] == [
        _Producer.calls[0]["kwargs"]["idempotency_key"],
        _Producer.calls[0]["kwargs"]["idempotency_key"],
    ]
    assert all(
        call["producer"]["adapter_id"] == "ftp/v1"
        and call["files"][0][:2] == ("camera/clip.mp4", b"immutable camera payload")
        for call in _Producer.calls
    )


def test_lifecycle_feed_is_bounded_source_fenced_and_replays_exact_claim_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    source = config.sources[0]
    _completed_upload(source, "camera/clip.mp4", b"immutable camera payload")
    _Producer.calls = []
    _Producer.fail_once = True
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]

    empty = adapter.event_page(source.id, after="0", limit=1)
    assert empty.events == []
    assert empty.next_cursor.startswith("camera-a~")
    assert not empty.has_more

    assert adapter.run_once()["completed"] == 0
    page = adapter.event_page(source.id, after=empty.next_cursor, limit=1)
    assert [event.type for event in page.events] == [CLAIM_REGISTERED]
    assert page.has_more
    claim_id = page.events[0].subject
    assert page.events[0].payload.source_id == source.id
    assert page.events[0].payload.file_count == 1
    assert page.events[0].payload.bytes == len(b"immutable camera payload")
    page.require_progress_after(empty.next_cursor)

    custody = adapter.event_page(source.id, after=page.next_cursor, limit=1)
    assert [event.type for event in custody.events] == [CUSTODY_READY]
    assert custody.events[0].subject == claim_id
    assert custody.has_more
    failed = adapter.event_page(source.id, after=custody.next_cursor, limit=1)
    assert [event.type for event in failed.events] == [ATTEMPT_FAILED]
    assert failed.events[0].payload.error_type == "ConnectionError"
    assert not failed.has_more

    restarted = FtpSpool(object(), config)  # type: ignore[arg-type]
    assert restarted.event_page(source.id, after=page.next_cursor, limit=100).events == [
        *custody.events,
        *failed.events,
    ]
    assert restarted.run_once()["completed"] == 1
    published = restarted.event_page(source.id, after=failed.next_cursor, limit=100)
    assert [event.type for event in published.events] == [CLAIM_PUBLISHED]
    assert published.events[0].subject == claim_id
    assert published.events[0].payload.collection_id == "41"
    assert published.events[0].payload.archive_root_sha256 == "a" * 64
    assert published.events[0].payload.content_identity == "b" * 64
    assert restarted.run_once()["completed"] == 0
    assert restarted.event_page(source.id, after=published.next_cursor, limit=100).events == []

    with pytest.raises(FtpEventCursorChanged):
        restarted.event_page(
            source.id, after=published.next_cursor.replace("camera-a~", "other~"), limit=1
        )
    with pytest.raises(ValueError, match="ahead"):
        restarted.event_page(
            source.id, after=published.next_cursor.rsplit("~", 1)[0] + "~100", limit=1
        )
    with pytest.raises(ValueError, match="malformed"):
        restarted.event_page(source.id, after="1", limit=1)
    with pytest.raises(ValueError, match="limit"):
        restarted.event_page(source.id, after="0", limit=101)

    reset = FtpSpool(object(), _config(tmp_path / "another-state"))  # type: ignore[arg-type]
    with pytest.raises(FtpEventCursorChanged):
        reset.event_page(source.id, after=published.next_cursor, limit=1)


@pytest.mark.parametrize("claim_root_survives", [False, True])
def test_durable_receipt_finishes_claim_cleanup_after_process_stop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    claim_root_survives: bool,
) -> None:
    config = _config(tmp_path)
    source = config.sources[0].model_copy(update={"max_files": 1})
    config = config.model_copy(update={"sources": (source,)})
    _completed_upload(source, "settled.bin", b"settled once")
    _Producer.calls = []
    _Producer.fail_once = False
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]

    with monkeypatch.context() as crash:
        if claim_root_survives:
            remove_tree = landing.shutil.rmtree

            def stop_before_claim_removal(path: Any, *args: Any, **kwargs: Any) -> None:
                if Path(path).parent.name == "claims":
                    raise _SimulatedProcessStop
                remove_tree(path, *args, **kwargs)

            crash.setattr(landing.shutil, "rmtree", stop_before_claim_removal)
        else:

            def stop_before_claim_retirement(_source: SourceConfig, _claim_id: str) -> None:
                raise _SimulatedProcessStop

            crash.setattr(adapter, "_forget_claim", stop_before_claim_retirement)
        with pytest.raises(_SimulatedProcessStop):
            adapter.run_once()

    receipts = list((source.root / ".a-riverhog-ftp-spool" / "receipts").glob("*.json"))
    claims = list((source.root / ".a-riverhog-ftp-spool" / "claims").glob("*/claim.json"))
    assert len(receipts) == 1
    assert len(claims) == int(claim_root_survives)
    assert len(_Producer.calls) == 1
    assert len(adapter._claim_work(source, limit=10)) == 1

    restarted = FtpSpool(object(), config)  # type: ignore[arg-type]
    result = restarted.run_once()

    assert result["completed"] == 1
    assert result["failed"] == []
    assert len(_Producer.calls) == 1
    assert restarted.status()["sources"][0]["claims"] == 0  # type: ignore[index]
    assert list((source.root / ".a-riverhog-ftp-spool" / "claims").glob("*")) == []
    assert list((source.root / ".a-riverhog-ftp-spool" / "handoffs").glob("*")) == []


def test_durable_receipt_replays_publication_event_after_process_stop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    source = config.sources[0].model_copy(update={"max_files": 1})
    config = config.model_copy(update={"sources": (source,)})
    _completed_upload(source, "settled.bin", b"settled once")
    _Producer.calls = []
    _Producer.fail_once = False
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]

    with monkeypatch.context() as crash:

        def stop_before_event(
            _source: SourceConfig, _claim_id: str, _receipt: ProducedCollection
        ) -> None:
            raise _SimulatedProcessStop

        crash.setattr(adapter, "_record_publication", stop_before_event)
        with pytest.raises(_SimulatedProcessStop):
            adapter.run_once()

    before = adapter.event_page(source.id, after="0", limit=100)
    assert [event.type for event in before.events] == [CLAIM_REGISTERED, CUSTODY_READY]
    assert len(_Producer.calls) == 1
    restarted = FtpSpool(object(), config)  # type: ignore[arg-type]
    assert restarted.run_once()["completed"] == 1
    after = restarted.event_page(source.id, after=before.next_cursor, limit=100)
    assert [event.type for event in after.events] == [CLAIM_PUBLISHED]
    assert len(_Producer.calls) == 1
    assert restarted.run_once()["completed"] == 0
    assert restarted.event_page(source.id, after=before.next_cursor, limit=100) == after


@pytest.mark.parametrize("cleanup_before_replay", [False, True])
def test_completed_file_receipt_replay_records_publication_before_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    cleanup_before_replay: bool,
) -> None:
    config = _config(tmp_path)
    source = config.sources[0]
    content = b"receipt-led terminal event"
    upload = source.root / "replay.bin"
    upload.parent.mkdir(parents=True)
    upload.write_bytes(content)
    record = CompletionHandoff(source.root, source.id).complete(upload)
    custody = source.root / record.custody
    _Producer.calls = []
    _Producer.fail_once = False
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]

    with monkeypatch.context() as crash:

        def stop_before_event(
            _source: SourceConfig, _claim_id: str, _receipt: ProducedCollection
        ) -> None:
            raise _SimulatedProcessStop

        crash.setattr(adapter, "_record_publication", stop_before_event)
        with pytest.raises(_SimulatedProcessStop):
            adapter.accept_completed_file(
                source,
                custody,
                relative_path=record.path,
                source_event_id=record.event_id,
                expected_bytes=record.bytes,
                expected_sha256=hashlib.sha256(content).hexdigest(),
            )

    before = adapter.event_page(source.id, after="0", limit=100)
    assert [event.type for event in before.events] == [CLAIM_REGISTERED, CUSTODY_READY]
    receipt_files = list((source.root / ".a-riverhog-ftp-spool" / "receipts").glob("*.json"))
    assert len(receipt_files) == 1
    if cleanup_before_replay:
        # Model a prior receipt-led cleanup that retired the SQLite claim first.
        adapter._finish_claim_cleanup(source, receipt_files[0].stem)  # noqa: SLF001

    restarted = FtpSpool(object(), config)  # type: ignore[arg-type]
    arguments = dict(
        relative_path=record.path,
        source_event_id=record.event_id,
        expected_bytes=record.bytes,
        expected_sha256=hashlib.sha256(content).hexdigest(),
    )
    first = restarted.accept_completed_file(source, custody, **arguments)
    assert first.collection_id == 41
    after = restarted.event_page(source.id, after=before.next_cursor, limit=100)
    assert [event.type for event in after.events] == [CLAIM_PUBLISHED]
    assert restarted.accept_completed_file(source, custody, **arguments) == first
    assert restarted.event_page(source.id, after=before.next_cursor, limit=100) == after
    assert len(_Producer.calls) == 1


def test_completion_event_replay_guard_survives_until_exact_log_tip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(update={"max_files": 1})
    config = base.model_copy(
        update={
            "sources": (source,),
            "claim_attempt_budget": 1,
            "discovery_entry_budget": 1,
        }
    )
    path = source.root / "replayed.bin"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"one logical publication")
    record = CompletionHandoff(source.root, source.id).complete(path)
    with completion_log_path(source.root).open("ab") as stream:
        stream.write(record.canonical_bytes())
    _Producer.calls = []
    _Producer.fail_once = False
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)

    first = FtpSpool(object(), config)  # type: ignore[arg-type]
    assert first.run_once()["completed"] == 1
    receipts = list((source.root / ".a-riverhog-ftp-spool" / "receipts").glob("*.json"))
    assert len(receipts) == 1
    assert _completion_event_rows(first, source) == [(record.event_id, receipts[0].stem)]
    assert len(_Producer.calls) == 1

    restarted = FtpSpool(object(), config)  # type: ignore[arg-type]
    assert restarted.run_once()["completed"] == 0
    assert len(_Producer.calls) == 1
    assert _completion_event_rows(restarted, source) == []

    with completion_log_path(source.root).open("ab") as stream:
        stream.write(record.canonical_bytes())
    corrupted = restarted.run_once()
    assert corrupted["completed"] == 0
    assert corrupted["source_results"][0]["completion_failures"] == 1  # type: ignore[index]
    assert len(_Producer.calls) == 1


def test_completion_event_replay_state_reclaims_in_bounded_restartable_pages(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(update={"max_files": 1})
    config = base.model_copy(
        update={
            "sources": (source,),
            "claim_attempt_budget": 1,
            "discovery_entry_budget": 2,
        }
    )
    _Producer.calls = []
    _Producer.fail_once = False
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    with monkeypatch.context() as construction:
        construction.setattr(FtpSpool, "_reclaim_completion_events", lambda *_args, **_kwargs: 0)
        adapter = FtpSpool(object(), config)  # type: ignore[arg-type]
        for index in range(5):
            _completed_upload(source, f"settled-{index}.bin", f"settled-{index}".encode())
            assert adapter.run_once()["completed"] == 1

    assert len(_completion_event_rows(adapter, source)) == 5
    remaining: list[int] = []
    for _ in range(3):
        restarted = FtpSpool(object(), config)  # type: ignore[arg-type]
        assert restarted.run_once()["completed"] == 0
        remaining.append(len(_completion_event_rows(restarted, source)))

    assert remaining == [3, 1, 0]


def test_large_unavailable_backlog_is_bounded_then_drains_exactly_after_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(update={"max_files": 1})
    config = base.model_copy(
        update={
            "sources": (source,),
            "pending_claim_capacity": 3,
            "claim_attempt_budget": 2,
            "discovery_entry_budget": 32,
        }
    )
    expected = {f"camera-{index:02d}.bin" for index in range(12)}
    for name in sorted(expected):
        _completed_upload(source, name, name.encode())
    _ControlledProducer.calls = []
    _ControlledProducer.successes = []
    _ControlledProducer.available = False
    _ControlledProducer.permanently_failing_paths = set()
    monkeypatch.setattr(landing, "CollectionProducer", _ControlledProducer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]

    for _ in range(8):
        result = adapter.run_once()
        assert result["source_results"][0]["claim_attempts"] <= 2  # type: ignore[index]
        assert len(result["failed"]) <= 2  # type: ignore[arg-type]
        assert adapter.status()["sources"][0]["claims"] <= 3  # type: ignore[index]

    assert adapter.status()["sources"][0]["claims"] == 3  # type: ignore[index]
    assert len(_ControlledProducer.successes) == 0

    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]
    _ControlledProducer.available = True
    for _ in range(30):
        result = adapter.run_once()
        assert result["source_results"][0]["claim_attempts"] <= 2  # type: ignore[index]
        if (
            len(_ControlledProducer.successes) == len(expected)
            and adapter.status()["sources"][0]["claims"] == 0  # type: ignore[index]
        ):
            break

    assert set(_ControlledProducer.successes) == expected
    assert len(_ControlledProducer.successes) == len(expected)
    assert adapter.status()["sources"][0]["claims"] == 0  # type: ignore[index]


def test_completion_discovery_progresses_beyond_persistent_prefix_across_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(update={"max_files": 1})
    config = base.model_copy(
        update={
            "sources": (source,),
            "claim_attempt_budget": 2,
            "discovery_entry_budget": 2,
        }
    )
    CompletionHandoff(source.root, source.id)
    log = completion_log_path(source.root)
    for index in range(5):
        with log.open("ab") as stream:
            stream.write(f"invalid-{index}\n".encode())
    payload = _completed_upload(source, "one/two/three/four/five/payload.bin", b"payload")
    _ControlledProducer.calls = []
    _ControlledProducer.successes = []
    _ControlledProducer.available = True
    _ControlledProducer.permanently_failing_paths = set()
    monkeypatch.setattr(landing, "CollectionProducer", _ControlledProducer)

    passes: list[dict[str, object]] = []
    for _ in range(4):
        adapter = FtpSpool(object(), config)  # type: ignore[arg-type]
        current = adapter.run_once()
        passes.append(current)
        assert current["source_results"][0]["discovery_entries_examined"] <= 2  # type: ignore[index]
        if not payload.exists():
            break

    assert not payload.exists()
    assert _ControlledProducer.successes == ["one/two/three/four/five/payload.bin"]
    assert sum(int(current["completed"]) for current in passes) == 1


def test_adapter_owned_completion_authority_is_restartable_and_hidden(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    source = config.sources[0]
    FtpSpool(object(), config)  # type: ignore[arg-type]
    log = completion_log_path(source.root)
    assert log.parent == source.root / ".a-riverhog-ftp-spool"

    payload = source.root / "after-restart.bin"
    payload.write_bytes(b"after")
    custody = CompletionHandoff(source.root, source.id).complete(payload)
    _Producer.calls = []
    _Producer.fail_once = False
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    restarted = FtpSpool(object(), config)  # type: ignore[arg-type]

    assert restarted.run_once()["completed"] == 1
    assert not payload.exists()
    assert not (source.root / custody.custody).exists()
    assert log.read_text(encoding="ascii").count("\n") == 2


@pytest.mark.parametrize("failure", ["missing", "size-changed"])
def test_completion_identity_failure_is_durable_and_does_not_block_later_input(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    config = _config(tmp_path)
    source = config.sources[0]
    payload = _completed_upload(source, "identity.bin", b"exact")
    if failure == "missing":
        payload.unlink()
    else:
        payload.chmod(0o600)
        payload.write_bytes(b"different-size")
    good = _completed_upload(source, "good.bin", b"good")
    _Producer.calls = []
    _Producer.fail_once = False
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)

    first = FtpSpool(object(), config)  # type: ignore[arg-type]
    result = first.run_once()

    assert result["completed"] == 1
    assert result["failed"] == []
    assert first.status()["sources"][0]["claims"] == 0  # type: ignore[index]
    assert first.status()["sources"][0]["completion_failures"] == 1  # type: ignore[index]
    failure_status = first.status()["sources"][0]["oldest_completion_failure"]  # type: ignore[index]
    assert isinstance(failure_status, dict)
    assert failure_status["retryable"] is True
    assert failure_status["reason"]
    assert failure_status["attempts"] == 0
    typed_failure = (
        FtpSpoolStatus.model_validate(first.status()).sources[0].oldest_completion_failure
    )
    assert typed_failure is not None and typed_failure.attempts == 0
    assert not good.exists()

    restarted = FtpSpool(object(), config)  # type: ignore[arg-type]

    assert restarted.run_once()["completed"] == 0
    assert restarted.status()["sources"][0]["completion_failures"] == 1  # type: ignore[index]


def test_json_numeric_limit_failure_is_durable_and_does_not_block_later_input(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    source = config.sources[0]
    _append_invalid_completion(source, b'{"bytes":' + (b"9" * 5000) + b"}\n")
    good = _completed_upload(source, "after-malformed.bin", b"good")
    _Producer.calls = []
    _Producer.fail_once = False
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)

    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]
    result = adapter.run_once()

    assert result["completed"] == 1
    assert result["failed"] == []
    assert result["source_results"][0]["completion_failures"] == 1  # type: ignore[index]
    assert not good.exists()


def test_full_batch_returns_before_bad_lookahead_record(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(update={"max_files": 1})
    config = base.model_copy(update={"sources": (source,)})
    payload = _completed_upload(source, "first.bin", b"first")
    _append_invalid_completion(source)
    _Producer.calls = []
    _Producer.fail_once = False
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]

    first = adapter.run_once()

    assert first["completed"] == 1
    assert first["source_results"][0]["discovery_entries_examined"] == 1  # type: ignore[index]
    assert first["source_results"][0]["completion_failures"] == 0  # type: ignore[index]
    assert not payload.exists()

    second = adapter.run_once()

    assert second["completed"] == 0
    assert second["source_results"][0]["completion_failures"] == 1  # type: ignore[index]


def test_completion_failure_capacity_backpressures_at_exact_cursor(tmp_path: Path) -> None:
    base = _config(tmp_path)
    config = base.model_copy(update={"completion_failure_capacity": 1})
    source = config.sources[0]
    _append_invalid_completion(source, b"first-invalid\n")
    _append_invalid_completion(source, b"second-invalid\n")
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]

    result = adapter.run_once()

    assert result["completed"] == 0
    assert result["source_results"][0]["completion_failures"] == 1  # type: ignore[index]
    assert result["source_results"][0]["discovery_entries_examined"] == 0  # type: ignore[index]
    assert result["failed"][0]["claim"] == "new"  # type: ignore[index]
    assert "completion failure capacity is exhausted" in result["failed"][0]["error"]  # type: ignore[index]


def test_lowered_admission_capacity_preserves_bounded_status_and_claim_drain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(update={"max_files": 1})
    original = base.model_copy(
        update={
            "sources": (source,),
            "pending_claim_capacity": 5,
            "claim_attempt_budget": 2,
            "discovery_entry_budget": 2,
        }
    )
    _ControlledProducer.calls = []
    _ControlledProducer.successes = []
    _ControlledProducer.available = False
    _ControlledProducer.permanently_failing_paths = set()
    monkeypatch.setattr(landing, "CollectionProducer", _ControlledProducer)
    expected = {f"claimed-{index}.bin" for index in range(5)}
    for name in sorted(expected):
        _completed_upload(source, name, name.encode())
    adapter = FtpSpool(object(), original)  # type: ignore[arg-type]
    for _ in range(5):
        result = adapter.run_once()
        assert result["source_results"][0]["claim_attempts"] <= 2  # type: ignore[index]
    assert adapter.status()["sources"][0]["claims"] == 5  # type: ignore[index]

    pending = _completed_upload(source, "new-after-reduction.bin", b"new")
    lowered = original.model_copy(update={"pending_claim_capacity": 2})
    adapter = FtpSpool(object(), lowered)  # type: ignore[arg-type]
    status = adapter.status()["sources"][0]  # type: ignore[index]
    assert status["claims"] == 5
    assert status["pending_claim_capacity"] == 2
    _ControlledProducer.available = True

    first = adapter.run_once()
    assert first["source_results"][0]["claim_attempts"] == 2  # type: ignore[index]
    assert first["source_results"][0]["admission_deferred"] is True  # type: ignore[index]
    assert pending.exists()

    for _ in range(8):
        adapter = FtpSpool(object(), lowered)  # type: ignore[arg-type]
        result = adapter.run_once()
        assert result["source_results"][0]["claim_attempts"] <= 2  # type: ignore[index]
        if adapter.status()["sources"][0]["claims"] == 0:  # type: ignore[index]
            break

    assert set(_ControlledProducer.successes) == expected | {"new-after-reduction.bin"}
    assert not pending.exists()
    assert adapter.status()["sources"][0]["claims"] == 0  # type: ignore[index]


def test_persistent_old_failure_does_not_starve_new_input_across_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(update={"max_files": 1})
    config = base.model_copy(
        update={
            "sources": (source,),
            "pending_claim_capacity": 4,
            "claim_attempt_budget": 2,
            "discovery_entry_budget": 16,
        }
    )
    _ControlledProducer.calls = []
    _ControlledProducer.successes = []
    _ControlledProducer.available = True
    _ControlledProducer.permanently_failing_paths = {"bad.bin"}
    monkeypatch.setattr(landing, "CollectionProducer", _ControlledProducer)
    _completed_upload(source, "bad.bin", b"bad")

    first = FtpSpool(object(), config)  # type: ignore[arg-type]
    assert first.run_once()["failed"]
    _completed_upload(source, "good.bin", b"good")

    restarted = FtpSpool(object(), config)  # type: ignore[arg-type]
    second = restarted.run_once()

    assert second["source_results"][0]["claim_attempts"] == 2  # type: ignore[index]
    assert "good.bin" in _ControlledProducer.successes
    assert restarted.status()["sources"][0]["claims"] == 1  # type: ignore[index]
    assert not (source.root / "good.bin").exists()
    assert _ControlledProducer.calls.count("bad.bin") == 2

    # The durable cursor names the now-completed good claim. A later restart
    # must wrap past that missing entry, retry the old failure, and still admit
    # new input in the same bounded pass.
    _completed_upload(source, "later.bin", b"later")
    restarted = FtpSpool(object(), config)  # type: ignore[arg-type]
    third = restarted.run_once()

    assert third["source_results"][0]["claim_attempts"] == 2  # type: ignore[index]
    assert _ControlledProducer.successes == ["good.bin", "later.bin"]
    assert _ControlledProducer.calls.count("bad.bin") == 3
    assert restarted.status()["sources"][0]["claims"] == 1  # type: ignore[index]


def test_changed_claim_remains_in_custody_for_bounded_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(update={"max_files": 1})
    config = base.model_copy(
        update={
            "sources": (source,),
            "pending_claim_capacity": 2,
            "claim_attempt_budget": 2,
            "discovery_entry_budget": 8,
        }
    )
    _ControlledProducer.calls = []
    _ControlledProducer.successes = []
    _ControlledProducer.available = False
    _ControlledProducer.permanently_failing_paths = set()
    monkeypatch.setattr(landing, "CollectionProducer", _ControlledProducer)
    _completed_upload(source, "changed.bin", b"original")

    first = FtpSpool(object(), config)  # type: ignore[arg-type]
    result = first.run_once()
    assert result["source_results"][0]["claim_attempts"] == 1  # type: ignore[index]
    claim_root = first._claim_work(source, limit=1)[0].root
    claimed_payload = claim_root / "payload" / "changed.bin"
    claimed_payload.chmod(0o600)
    claimed_payload.write_bytes(b"tampered")

    _ControlledProducer.available = True
    restarted = FtpSpool(object(), config)  # type: ignore[arg-type]
    result = restarted.run_once()

    assert result["source_results"][0]["claim_attempts"] == 1  # type: ignore[index]
    assert result["source_results"][0]["failed"] == 1  # type: ignore[index]
    assert restarted.status()["sources"][0]["claims"] == 1  # type: ignore[index]
    assert claimed_payload.read_bytes() == b"tampered"
    assert _ControlledProducer.successes == []


def test_pre_manifest_construction_orphan_is_rebuilt_after_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    source = config.sources[0].model_copy(update={"max_files": 1})
    config = config.model_copy(update={"sources": (source,)})
    payload = _completed_upload(source, "orphaned.bin", b"orphaned")
    _Producer.calls = []
    _Producer.fail_once = False
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]
    discovery = adapter._discover_batch(source, flush=False)
    event_id = hashlib.sha256(
        "\n".join(
            f"{row.event_identity}\0{row.relative}\0{row.observed.st_size}"
            for row in discovery.files
        ).encode()
    ).hexdigest()
    claim_id = landing._claim_identity(
        source.id,
        event_id,
        tuple(
            (row.relative, row.observed.st_size, row.observed.st_dev, row.observed.st_ino)
            for row in discovery.files
        ),
    )
    orphan = adapter._claims_root(source) / claim_id
    orphan.mkdir(mode=0o700, parents=True)
    (orphan / ".claim.json.part").write_text("interrupted", encoding="utf-8")

    result = FtpSpool(object(), config).run_once()  # type: ignore[arg-type]

    assert result["completed"] == 1
    assert result["failed"] == []
    assert not payload.exists()
    assert not orphan.exists()
    assert _Producer.calls[0]["files"][0][1] == b"orphaned"


def test_status_pages_sources_without_claiming_an_exact_backlog_snapshot(tmp_path: Path) -> None:
    base = _config(tmp_path)
    first = base.sources[0].model_copy(update={"id": "camera-a", "root": tmp_path / "a"})
    second = base.sources[0].model_copy(update={"id": "camera-b", "root": tmp_path / "b"})
    adapter = FtpSpool(
        object(),  # type: ignore[arg-type]
        base.model_copy(update={"sources": (first, second)}),
    )

    page = adapter.status(page_size=1)
    following = adapter.status(page_size=1, page_token=str(page["next_page_token"]))

    assert [row["id"] for row in page["sources"]] == ["camera-a"]  # type: ignore[index]
    assert page["next_page_token"] == "camera-a"
    assert page["snapshot"] is False
    assert [row["id"] for row in following["sources"]] == ["camera-b"]  # type: ignore[index]
    assert following["next_page_token"] is None


def test_explicit_flush_is_the_same_bounded_claim_and_receipt_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(update={"close_mode": "explicit-flush"})
    config = base.model_copy(update={"sources": (source,)})
    _completed_upload(source, "current.bin", b"current")
    _Producer.calls = []
    monkeypatch.setattr("a_riverhog_ftp_spool.landing.CollectionProducer", _Producer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]

    assert adapter.run_once()["completed"] == 0
    assert adapter.flush(source.id)["completed"] == 1
    assert _Producer.calls[0]["files"] == [
        (
            "current.bin",
            b"current",
            {
                "status": "omitted",
                "omission_reason": "Fixture intentionally has no host provenance.",
            },
        )
    ]


def test_explicit_flush_marker_and_pass_are_one_serialized_operation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(update={"close_mode": "explicit-flush"})
    config = base.model_copy(update={"sources": (source,)})
    _completed_upload(source, "current.bin", b"current")
    _Producer.calls = []
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]

    marker_written = threading.Event()
    release_marker_write = threading.Event()
    polling_pass_entered = threading.Event()
    original_write_atomic = landing._write_atomic
    original_pass = adapter._run_once

    def blocking_marker_write(path: Path, content: bytes) -> None:
        original_write_atomic(path, content)
        if path.name == ".riverhog-ftp-flush":
            marker_written.set()
            assert release_marker_write.wait(timeout=2)

    def observed_pass(source_ids: object = None) -> dict[str, object]:
        if threading.current_thread().name == "polling-pass":
            polling_pass_entered.set()
        return original_pass(source_ids)  # type: ignore[arg-type]

    monkeypatch.setattr(landing, "_write_atomic", blocking_marker_write)
    monkeypatch.setattr(adapter, "_run_once", observed_pass)

    with ThreadPoolExecutor(max_workers=2) as executor:
        flush = executor.submit(adapter.flush, source.id)
        try:
            assert marker_written.wait(timeout=2)
            poll = executor.submit(adapter.run_once)
            polling_stole_marker = polling_pass_entered.wait(timeout=0.25)
        finally:
            release_marker_write.set()
        flush_result = flush.result(timeout=5)
        poll_result = poll.result(timeout=5)

    assert polling_stole_marker is False
    assert flush_result["completed"] == 1
    assert flush_result["failed"] == []
    assert poll_result["completed"] == 0


def test_captured_provenance_is_identity_checked_and_projected_for_the_producer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(
        update={
            "close_mode": "explicit-flush",
            "provenance": "capture",
            "provenance_omission_reason": None,
        }
    )
    config = base.model_copy(
        update={
            "host_id": "urn:uuid:00000000-0000-4000-8000-000000000522",
            "provenance_observer": "fixture-observer",
            "sources": (source,),
        }
    )
    _completed_upload(source, "captured.bin", b"captured")
    _Producer.calls = []
    monkeypatch.setattr("a_riverhog_ftp_spool.landing.CollectionProducer", _Producer)

    result = FtpSpool(
        object(),  # type: ignore[arg-type]
        config,
        provenance_observer_factory=native_provenance_observer,
    ).flush(source.id)

    assert result["completed"] == 1
    provenance = _Producer.calls[0]["files"][0][2]
    assert provenance["status"] == "captured"
    assert set(provenance) == {"status", "journal_id", "current_state_id"}
    assert _Producer.calls[0]["kwargs"]["provenance_journals"]


def test_completed_portable_sidecar_follows_payload_into_claim(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base = _config(tmp_path)
    source = base.sources[0].model_copy(
        update={
            "close_mode": "explicit-flush",
            "provenance": "capture",
            "provenance_omission_reason": None,
        }
    )
    config = base.model_copy(
        update={
            "host_id": "urn:uuid:00000000-0000-4000-8000-000000000522",
            "provenance_observer": "fixture-observer",
            "sources": (source,),
        }
    )
    payload = source.root / "captured.bin"
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b"captured with its original provenance")
    journal = create_observation_journal(
        payload,
        relative_path="captured.bin",
        host_id=config.host_id,
        agent_name="source-client",
        agent_version="1.0.0",
        observer=native_provenance_observer(),
    )
    sidecar = canonical_sidecar_path(payload)
    sidecar.write_bytes(journal)
    handoff = CompletionHandoff(source.root, source.id)
    handoff.complete(sidecar)
    handoff.complete(payload)
    _Producer.calls = []
    monkeypatch.setattr("a_riverhog_ftp_spool.landing.CollectionProducer", _Producer)

    result = FtpSpool(
        object(),  # type: ignore[arg-type]
        config,
        provenance_observer_factory=native_provenance_observer,
    ).flush(source.id)

    assert result["completed"] == 1
    assert [item[:2] for item in _Producer.calls[0]["files"]] == [
        ("captured.bin", b"captured with its original provenance")
    ]
    assert [
        content for _journal_id, content in _Producer.calls[0]["kwargs"]["provenance_journals"]
    ] == [journal]


def test_custody_passes_are_serialized_across_protocol_and_polling_entrypoints(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    source = config.sources[0]
    active = 0
    maximum_active = 0
    guard = threading.Lock()

    class BlockingProducer(_Producer):
        def publish(self, files: object, **kwargs: object) -> ProducedCollection:
            nonlocal active, maximum_active
            with guard:
                active += 1
                maximum_active = max(maximum_active, active)
            try:
                time.sleep(0.05)
                return super().publish(files, **kwargs)
            finally:
                with guard:
                    active -= 1

    BlockingProducer.calls = []
    monkeypatch.setattr("a_riverhog_ftp_spool.landing.CollectionProducer", BlockingProducer)
    adapter = FtpSpool(object(), config)  # type: ignore[arg-type]
    payloads = []
    for index in range(2):
        payload = source.root / f"protocol-{index}.bin"
        payload.parent.mkdir(parents=True, exist_ok=True)
        content = f"protocol-{index}".encode()
        payload.write_bytes(content)
        payloads.append((payload, content))

    def accept(index: int) -> ProducedCollection:
        payload, content = payloads[index]
        digest = hashlib.sha256(content).hexdigest()
        return adapter.accept_completed_file(
            source,
            payload,
            relative_path=payload.name,
            source_event_id=f"event-{index}",
            expected_bytes=len(content),
            expected_sha256=digest,
            provenance={
                "path": payload.name,
                "bytes": len(content),
                "sha256": digest,
                "status": "omitted",
                "omission_reason": "Fixture intentionally has no host provenance.",
            },
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        receipts = tuple(executor.map(accept, range(2)))

    assert [receipt.collection_id for receipt in receipts] == [41, 41]
    assert maximum_active == 1
    assert len(BlockingProducer.calls) == 2
