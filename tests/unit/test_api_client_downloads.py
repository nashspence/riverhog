from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable
from pathlib import Path

from riverhog_client.downloads import RetrievalDownload, download_retrieval_files


class DownloadApi:
    def __init__(
        self,
        contents: dict[str, bytes],
        transfer: Callable[[str], None],
    ) -> None:
        self.contents = contents
        self.transfer = transfer

    def spawn(self) -> DownloadApi:
        return DownloadApi(self.contents, self.transfer)

    def download_retrieval_artifact(
        self,
        job_id: str,
        *,
        collection_id: int,
        artifact_id: str,
        output: Path,
        expected_bytes: int,
        expected_sha256: str,
    ) -> int:
        assert job_id == "job-1"
        assert collection_id == 1
        content = self.contents[artifact_id]
        assert expected_bytes == len(content)
        assert expected_sha256 == hashlib.sha256(content).hexdigest()
        self.transfer(artifact_id)
        output.write_bytes(content)
        return len(content)


def _artifact(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _downloads(tmp_path: Path, contents: dict[str, bytes]) -> list[RetrievalDownload]:
    return [
        RetrievalDownload(
            collection_id=1,
            artifact_id=artifact_id,
            output=tmp_path / artifact_id,
            expected_bytes=len(content),
            expected_sha256=hashlib.sha256(content).hexdigest(),
        )
        for artifact_id, content in contents.items()
    ]


def test_retrieval_files_download_concurrently_and_preserve_identity(tmp_path: Path) -> None:
    contents = {_artifact("one"): b"one", _artifact("two"): b"two"}
    rendezvous = threading.Barrier(2)
    completed: list[str] = []

    def transfer(path: str) -> None:
        rendezvous.wait(timeout=2)
        completed.append(path)

    api = DownloadApi(contents, transfer)
    downloads = _downloads(tmp_path, contents)

    assert (
        download_retrieval_files(
            api,
            "job-1",
            downloads,
            concurrency=2,
            window=2,
        )
        == 6
    )
    assert sorted(completed) == sorted(contents)
    assert {item.artifact_id: item.output.read_bytes() for item in downloads} == contents


def test_download_progress_does_not_block_transfer_workers(tmp_path: Path) -> None:
    contents = {_artifact(str(index)): bytes([index]) for index in range(4)}
    condition = threading.Condition()
    callback_started = threading.Event()
    callback_lock = threading.Lock()
    completed = 0
    callbacks: list[str] = []

    def transfer(path: str) -> None:
        nonlocal completed
        if path != _artifact("0"):
            assert callback_started.wait(timeout=2)
        with condition:
            completed += 1
            condition.notify_all()

    def downloaded(item: RetrievalDownload, _accepted: int) -> None:
        with callback_lock:
            callbacks.append(item.artifact_id)
            if len(callbacks) == 1:
                callback_started.set()
                with condition:
                    assert condition.wait_for(lambda: completed == 4, timeout=2)

    api = DownloadApi(contents, transfer)

    assert (
        download_retrieval_files(
            api,
            "job-1",
            _downloads(tmp_path, contents),
            concurrency=2,
            window=4,
            on_downloaded=downloaded,
        )
        == 4
    )
    assert sorted(callbacks) == sorted(contents)


def test_download_heartbeat_runs_while_one_unbounded_file_is_active(tmp_path: Path) -> None:
    heartbeat = threading.Event()

    def transfer(_path: str) -> None:
        assert heartbeat.wait(timeout=2)

    api = DownloadApi({_artifact("large"): b"content"}, transfer)

    assert download_retrieval_files(
        api,
        "job-1",
        _downloads(tmp_path, {_artifact("large"): b"content"}),
        concurrency=1,
        window=1,
        heartbeat=heartbeat.set,
        heartbeat_interval_seconds=0.01,
    ) == len(b"content")
    assert heartbeat.is_set()
