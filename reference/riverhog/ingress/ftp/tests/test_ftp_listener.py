from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from ftplib import FTP, error_temp
from io import BytesIO
from pathlib import Path

import pytest
from riverhog_ftp_adapter.completion import (
    CompletionHandoff,
    CompletionRecord,
    completion_log_path,
    parse_completion_record,
)
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


def test_incomplete_upload_is_not_handed_off_and_resumes_after_restart(tmp_path: Path) -> None:
    root = tmp_path / "intake"
    prefix = b"durable prefix-"
    suffix = b"completed after restart"
    with _listener(root) as address:
        ftp = _login(address)
        data = ftp.transfercmd("STOR resumed.bin")
        data.sendall(prefix)
        ftp.close()
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
