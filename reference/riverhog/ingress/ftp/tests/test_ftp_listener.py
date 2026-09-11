from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from ftplib import FTP, error_temp
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
import riverhog_ftp_adapter.landing as landing
import riverhog_ftp_adapter.listener as ftp_listener
from riverhog_client.producer import ProducedCollection
from riverhog_ftp_adapter.completion import (
    CONTROL_DIR,
    CompletionHandoff,
    CompletionRecord,
    completion_log_path,
    parse_completion_record,
)
from riverhog_ftp_adapter.config import FtpAdapterConfig, SourceConfig
from riverhog_ftp_adapter.landing import FtpAdapter
from riverhog_ftp_adapter.listener import build_ftp_server


@contextmanager
def _listener(root: Path) -> Iterator[tuple[str, int]]:
    server = build_ftp_server(
        source_root=root,
        source_id="camera-a",
        username="camera",
        password="secret",
        host="127.0.0.1",
        port=0,
        passive_ports=(),
        public_host=None,
        max_connections=16,
        max_connections_per_ip=8,
    )
    address = server.socket.getsockname()[:2]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield str(address[0]), int(address[1])
    finally:
        server.close_all()
        thread.join(timeout=5)


def _login(address: tuple[str, int]) -> FTP:
    ftp = FTP()
    ftp.connect(*address, timeout=5)
    ftp.login("camera", "secret")
    return ftp


def _records(root: Path) -> list[CompletionRecord]:
    lines = completion_log_path(root).read_bytes().splitlines(keepends=True)
    return [parse_completion_record(line) for line in lines[1:]]


def _adapter_config(root: Path) -> FtpAdapterConfig:
    return FtpAdapterConfig(
        host_id="test-host",
        riverhog_base_url="https://riverhog.invalid",
        riverhog_token="riverhog-token",
        api_token="adapter-token",
        sources=(
            SourceConfig(
                id="camera-a",
                root=root,
                ingest_source="ftp:camera-a",
                max_files=10,
                max_bytes=1024 * 1024,
                provenance="omit",
                provenance_omission_reason="Fixture intentionally omits host provenance.",
            ),
        ),
    )


class _Producer:
    calls: list[list[tuple[str, bytes]]] = []
    available = True

    def __init__(self, _api: object, **_kwargs: object) -> None:
        pass

    def publish(self, files: object, **_kwargs: object) -> ProducedCollection:
        materialized = tuple(files)  # type: ignore[arg-type]
        self.__class__.calls.append(
            [(str(item.path), item.source.read_bytes()) for item in materialized]
        )
        if not self.__class__.available:
            raise ConnectionError("publication intentionally unavailable")
        ordinal = len(self.__class__.calls)
        return ProducedCollection(
            collection_id=ordinal,
            archive_root_sha256=f"{ordinal:064x}",
            content_identity=f"{ordinal + 100:064x}",
            receipt={"state": "finalized"},
        )


class _SimulatedProcessStop(BaseException):
    pass


def test_success_ack_follows_exact_durable_handoff_and_path_reuse(tmp_path: Path) -> None:
    root = tmp_path / "intake"
    first = b"first completed bytes"
    second = b"second completed bytes"
    with _listener(root) as address:
        with _login(address) as ftp:
            assert ftp.storbinary("STOR clip.bin", BytesIO(first)).startswith("226 ")
        assert not (root / "clip.bin").exists()
        first_record = _records(root)[0]
        first_custody = root / first_record.custody
        assert first_custody.read_bytes() == first

        with _login(address) as ftp:
            assert ftp.storbinary("STOR clip.bin", BytesIO(second)).startswith("226 ")

    records = _records(root)
    assert len(records) == 2
    assert records[0].event_id != records[1].event_id
    assert records[0].path == records[1].path == "clip.bin"
    assert first_custody.read_bytes() == first
    assert (root / records[1].custody).read_bytes() == second


def test_incomplete_upload_is_not_handed_off_and_resumes_after_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "intake"
    prefix = b"durable prefix-"
    suffix = b"completed after restart"
    incomplete_received = threading.Event()
    on_incomplete = ftp_listener._CompletionHandler.on_incomplete_file_received

    def observe_incomplete(handler: Any, file: str) -> None:
        on_incomplete(handler, file)
        incomplete_received.set()

    monkeypatch.setattr(
        ftp_listener._CompletionHandler,
        "on_incomplete_file_received",
        observe_incomplete,
    )
    with _listener(root) as address:
        ftp = _login(address)
        data = ftp.transfercmd("STOR resumed.bin")
        data.sendall(prefix)
        ftp.close()
        try:
            assert incomplete_received.wait(timeout=5)
        finally:
            data.close()

    partial = root / "resumed.bin"
    assert partial.read_bytes() == prefix
    assert _records(root) == []

    with _listener(root) as address:
        with _login(address) as ftp:
            response = ftp.storbinary(
                "STOR resumed.bin",
                BytesIO(suffix),
                rest=len(prefix),
            )
            assert response.startswith("226 ")

    records = _records(root)
    assert len(records) == 1
    assert (root / records[0].custody).read_bytes() == prefix + suffix
    assert not partial.exists()


def test_concurrent_overwrite_of_active_path_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "intake"
    with _listener(root) as address:
        first = _login(address)
        data = first.transfercmd("STOR active.bin")
        data.sendall(b"partial")
        with _login(address) as second:
            with pytest.raises(error_temp, match="450"):
                second.storbinary("STOR active.bin", BytesIO(b"overwrite"))
        first.close()
        data.close()

    assert (root / "active.bin").read_bytes() == b"partial"
    assert _records(root) == []


def test_listener_restart_finishes_durable_pre_record_handoff(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "intake"
    source = root / "restart.bin"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"completed before crash")
    handoff = CompletionHandoff(root, "camera-a")
    append = handoff._append_record

    def interrupted(_record: object) -> None:
        raise OSError("simulated process loss before record append")

    monkeypatch.setattr(handoff, "_append_record", interrupted)
    with pytest.raises(OSError, match="simulated process loss"):
        handoff.complete(source)

    assert not source.exists()
    assert _records(root) == []

    monkeypatch.setattr(handoff, "_append_record", append)
    CompletionHandoff(root, "camera-a")

    records = _records(root)
    assert len(records) == 1
    assert (root / records[0].custody).read_bytes() == b"completed before crash"


def test_completed_provenance_sidecar_enters_exact_payload_custody(tmp_path: Path) -> None:
    root = tmp_path / "intake"
    sidecar = b"exact portable provenance"
    payload = b"payload with provenance"
    with _listener(root) as address:
        with _login(address) as ftp:
            assert ftp.storbinary(
                "STOR clip.bin.riverhog-provenance.json-seq", BytesIO(sidecar)
            ).startswith("226 ")
            with pytest.raises(error_temp, match="450"):
                ftp.storbinary(
                    "STOR clip.bin.riverhog-provenance.json-seq", BytesIO(b"replacement")
                )
            assert ftp.storbinary("STOR clip.bin", BytesIO(payload)).startswith("226 ")

    records = _records(root)
    assert [record.path for record in records] == [
        "clip.bin.riverhog-provenance.json-seq",
        "clip.bin",
    ]
    payload_custody = root / records[1].custody
    assert payload_custody.read_bytes() == payload
    assert (
        payload_custody.with_name(
            payload_custody.name + ".riverhog-provenance.json-seq"
        ).read_bytes()
        == sidecar
    )


def test_same_path_replay_and_new_event_partition_into_restartable_claims(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "intake"
    first = b"first completed bytes"
    second = b"second completed bytes"
    with _listener(root) as address:
        with _login(address) as ftp:
            assert ftp.storbinary("STOR clip.bin", BytesIO(first)).startswith("226 ")
        first_record = completion_log_path(root).read_bytes().splitlines(keepends=True)[1]
        with completion_log_path(root).open("ab") as stream:
            stream.write(first_record)
        with _login(address) as ftp:
            assert ftp.storbinary("STOR clip.bin", BytesIO(second)).startswith("226 ")

    _Producer.calls = []
    _Producer.available = True
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    config = _adapter_config(root)

    first_pass = FtpAdapter(object(), config).run_once()  # type: ignore[arg-type]
    second_pass = FtpAdapter(object(), config).run_once()  # type: ignore[arg-type]

    assert first_pass["completed"] == 1
    assert second_pass["completed"] == 1
    assert _Producer.calls == [[("clip.bin", first)], [("clip.bin", second)]]
    receipts = sorted((root / CONTROL_DIR / "receipts").glob("*.json"))
    assert len(receipts) == 2
    assert list((root / CONTROL_DIR / "handoffs").glob("*/payload")) == []


@pytest.mark.parametrize("finalize_before_listener_restart", [False, True])
def test_listener_recovers_published_intent_after_exact_adapter_acquisition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    finalize_before_listener_restart: bool,
) -> None:
    root = tmp_path / "intake"
    uploaded = root / "interleaved.bin"
    uploaded.parent.mkdir(parents=True)
    uploaded.write_bytes(b"exact interleaved custody")
    handoff = CompletionHandoff(root, "camera-a")

    def stop_after_publication(_record: Any, _intent: Path) -> None:
        raise _SimulatedProcessStop

    monkeypatch.setattr(handoff, "_retire_intent", stop_after_publication)
    with pytest.raises(_SimulatedProcessStop):
        handoff.complete(uploaded)

    config = _adapter_config(root)
    _Producer.calls = []
    _Producer.available = finalize_before_listener_restart
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    adapter_result = FtpAdapter(object(), config).run_once()  # type: ignore[arg-type]

    if finalize_before_listener_restart:
        assert adapter_result["completed"] == 1
    else:
        assert len(adapter_result["failed"]) == 1  # type: ignore[arg-type]
    assert list((root / CONTROL_DIR / "handoff-intents").glob("*.json")) == []
    acquisitions = list((root / CONTROL_DIR / "handoffs").glob("*/acquired-*.json"))
    assert len(acquisitions) == 1

    CompletionHandoff(root, "camera-a")

    assert list((root / CONTROL_DIR / "handoff-intents").glob("*.json")) == []
    if not finalize_before_listener_restart:
        _Producer.available = True
        settled = FtpAdapter(object(), config).run_once()  # type: ignore[arg-type]
        assert settled["completed"] == 1
    assert list((root / CONTROL_DIR / "handoffs").glob("*/acquired-*.json")) == []
    FtpAdapter(object(), config).run_once()  # type: ignore[arg-type]
    matching_calls = [
        call
        for call in _Producer.calls
        if call == [("interleaved.bin", b"exact interleaved custody")]
    ]
    assert len(matching_calls) == (1 if finalize_before_listener_restart else 2)
    assert len(list((root / CONTROL_DIR / "receipts").glob("*.json"))) == 1


def test_intent_retirement_and_adapter_acquisition_race_converges_repeatedly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "intake"
    config = _adapter_config(root)
    _Producer.calls = []
    _Producer.available = True
    monkeypatch.setattr(landing, "CollectionProducer", _Producer)
    adapter = FtpAdapter(object(), config)  # type: ignore[arg-type]

    for ordinal in range(8):
        uploaded = root / f"race-{ordinal}.bin"
        uploaded.write_bytes(f"exact-{ordinal}".encode("ascii"))
        handoff = CompletionHandoff(root, "camera-a")
        retire = handoff._retire_intent
        publication_complete = threading.Event()
        acquisition_complete = threading.Event()
        errors: list[BaseException] = []

        def retire_after_acquisition(
            record: CompletionRecord,
            intent: Path,
            ready: threading.Event = publication_complete,
            acquired: threading.Event = acquisition_complete,
            retire_intent: Any = retire,
        ) -> None:
            ready.set()
            if not acquired.wait(timeout=5):
                raise TimeoutError("adapter did not acquire the published handoff")
            retire_intent(record, intent)

        handoff._retire_intent = retire_after_acquisition  # type: ignore[method-assign]

        def complete(
            current_handoff: CompletionHandoff = handoff,
            current_uploaded: Path = uploaded,
            current_errors: list[BaseException] = errors,
        ) -> None:
            try:
                current_handoff.complete(current_uploaded)
            except BaseException as exc:
                current_errors.append(exc)

        thread = threading.Thread(target=complete)
        thread.start()
        assert publication_complete.wait(timeout=5)

        result = adapter.run_once()
        acquisition_complete.set()
        thread.join(timeout=5)

        assert not thread.is_alive()
        assert errors == []
        assert result["completed"] == 1

    assert len(_Producer.calls) == 8
    assert list((root / CONTROL_DIR / "handoff-intents").glob("*.json")) == []
    assert list((root / CONTROL_DIR / "handoffs").glob("*/acquired-*.json")) == []
    assert adapter.run_once()["completed"] == 0
