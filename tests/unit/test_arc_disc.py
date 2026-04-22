from __future__ import annotations

import hashlib

from typer.testing import CliRunner

import arc_disc.main as arc_disc_main

runner = CliRunner()


def _manifest_for(plaintext: bytes) -> dict[str, object]:
    sha256 = hashlib.sha256(plaintext).hexdigest()
    return {
        "id": "fx-1",
        "target": "docs/tax/2022/invoice-123.pdf",
        "entries": [
            {
                "id": "e1",
                "path": "tax/2022/invoice-123.pdf",
                "bytes": len(plaintext),
                "sha256": sha256,
                "parts": [
                    {
                        "index": 0,
                        "bytes": len(plaintext),
                        "sha256": sha256,
                        "copies": [
                            {
                                "copy": "copy-docs-1",
                                "location": "vault-a/shelf-01",
                                "disc_path": "disc/000001.bin",
                                "enc": {"fixture_key": "fixture-1"},
                            }
                        ],
                    }
                ],
            }
        ],
    }


def test_arc_disc_fetch_recovers_in_memory_and_reports_progress(monkeypatch) -> None:
    recovered = b"invoice fixture bytes\n"
    uploaded: list[tuple[str, int, str, bytes]] = []

    class FakeClient:
        def get_fetch_manifest(self, fetch_id: str) -> dict[str, object]:
            assert fetch_id == "fx-1"
            return _manifest_for(recovered)

        def create_or_resume_fetch_entry_upload(self, fetch_id: str, entry_id: str) -> dict[str, object]:
            assert fetch_id == "fx-1"
            assert entry_id == "e1"
            return {
                "entry": entry_id,
                "protocol": "tus",
                "upload_url": "https://uploads.test/fx-1/e1",
                "offset": 0,
                "length": len(recovered),
                "checksum_algorithm": "sha256",
                "expires_at": "2026-04-23T00:00:00Z",
            }

        def append_upload_chunk(
            self,
            upload_url: str,
            *,
            offset: int,
            checksum_algorithm: str,
            content: bytes,
        ) -> dict[str, object]:
            uploaded.append((upload_url, offset, checksum_algorithm, content))
            return {"offset": offset + len(content), "expires_at": None}

        def complete_fetch(self, fetch_id: str) -> dict[str, object]:
            assert fetch_id == "fx-1"
            return {"id": fetch_id, "state": "done"}

    class FakeReader:
        def read_iter(self, disc_path: str, *, device: str):
            assert disc_path == "disc/000001.bin"
            assert device == "/dev/fake-sr0"
            yield recovered[:8]
            yield recovered[8:]

    monkeypatch.setattr(arc_disc_main, "ApiClient", FakeClient)
    monkeypatch.setattr(arc_disc_main, "build_optical_reader", lambda: FakeReader())

    result = runner.invoke(
        arc_disc_main.app,
        ["fetch", "fx-1", "--device", "/dev/fake-sr0", "--json"],
        input="\n",
    )

    assert result.exit_code == 0
    assert '"state": "done"' in result.stdout
    assert "copy-docs-1" in result.stderr
    assert "current file" in result.stderr
    assert "manifest" in result.stderr
    assert "/s" in result.stderr
    assert uploaded == [
        ("https://uploads.test/fx-1/e1", 0, "sha256", recovered[:8]),
        ("https://uploads.test/fx-1/e1", 8, "sha256", recovered[8:]),
    ]


def test_arc_disc_fetch_reports_clean_error_when_optical_read_fails(monkeypatch) -> None:
    class FakeClient:
        def get_fetch_manifest(self, fetch_id: str) -> dict[str, object]:
            return _manifest_for(b"invoice fixture bytes\n")

        def create_or_resume_fetch_entry_upload(self, fetch_id: str, entry_id: str) -> dict[str, object]:
            return {
                "entry": entry_id,
                "protocol": "tus",
                "upload_url": "https://uploads.test/fx-1/e1",
                "offset": 0,
                "length": len(b"invoice fixture bytes\n"),
                "checksum_algorithm": "sha256",
                "expires_at": "2026-04-23T00:00:00Z",
            }

    class FailingReader:
        def read_iter(self, disc_path: str, *, device: str):
            raise RuntimeError(f"fixture optical read failed for {disc_path} on {device}")

    monkeypatch.setattr(arc_disc_main, "ApiClient", FakeClient)
    monkeypatch.setattr(arc_disc_main, "build_optical_reader", lambda: FailingReader())

    result = runner.invoke(
        arc_disc_main.app,
        ["fetch", "fx-1", "--device", "/dev/fake-sr0"],
        input="\n",
    )

    assert result.exit_code == 1
    assert "error: fixture optical read failed for disc/000001.bin on /dev/fake-sr0" in result.stderr
    assert "Traceback" not in result.stderr


def test_arc_disc_fetch_resumes_split_entry_from_session_offset(monkeypatch) -> None:
    part_one = b"invoice fixture "
    part_two = b"bytes\n"
    uploaded: list[tuple[int, bytes]] = []

    class FakeClient:
        def get_fetch_manifest(self, fetch_id: str) -> dict[str, object]:
            assert fetch_id == "fx-1"
            return {
                "id": "fx-1",
                "target": "docs/tax/2022/invoice-123.pdf",
                "entries": [
                    {
                        "id": "e1",
                        "path": "tax/2022/invoice-123.pdf",
                        "bytes": len(part_one) + len(part_two),
                        "sha256": hashlib.sha256(part_one + part_two).hexdigest(),
                        "parts": [
                            {
                                "index": 0,
                                "bytes": len(part_one),
                                "sha256": hashlib.sha256(part_one).hexdigest(),
                                "copies": [
                                    {
                                        "copy": "copy-docs-split-1",
                                        "location": "vault-a/shelf-01",
                                        "disc_path": "disc/000001.bin",
                                        "enc": {"fixture_key": "fixture-1"},
                                    }
                                ],
                            },
                            {
                                "index": 1,
                                "bytes": len(part_two),
                                "sha256": hashlib.sha256(part_two).hexdigest(),
                                "copies": [
                                    {
                                        "copy": "copy-docs-split-2",
                                        "location": "vault-a/shelf-02",
                                        "disc_path": "disc/000002.bin",
                                        "enc": {"fixture_key": "fixture-2"},
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }

        def create_or_resume_fetch_entry_upload(self, fetch_id: str, entry_id: str) -> dict[str, object]:
            return {
                "entry": entry_id,
                "protocol": "tus",
                "upload_url": "https://uploads.test/fx-1/e1",
                "offset": len(part_one),
                "length": len(part_one) + len(part_two),
                "checksum_algorithm": "sha256",
                "expires_at": "2026-04-23T00:00:00Z",
            }

        def append_upload_chunk(
            self,
            upload_url: str,
            *,
            offset: int,
            checksum_algorithm: str,
            content: bytes,
        ) -> dict[str, object]:
            assert upload_url == "https://uploads.test/fx-1/e1"
            assert checksum_algorithm == "sha256"
            uploaded.append((offset, content))
            return {"offset": offset + len(content), "expires_at": None}

        def complete_fetch(self, fetch_id: str) -> dict[str, object]:
            return {"id": fetch_id, "state": "done"}

    class FakeReader:
        def read_iter(self, disc_path: str, *, device: str):
            assert disc_path == "disc/000002.bin"
            assert device == "/dev/fake-sr0"
            yield part_two[:2]
            yield part_two[2:]

    monkeypatch.setattr(arc_disc_main, "ApiClient", FakeClient)
    monkeypatch.setattr(arc_disc_main, "build_optical_reader", lambda: FakeReader())

    result = runner.invoke(
        arc_disc_main.app,
        ["fetch", "fx-1", "--device", "/dev/fake-sr0", "--json"],
        input="\n",
    )

    assert result.exit_code == 0
    assert '"state": "done"' in result.stdout
    assert "copy-docs-split-1" not in result.stderr
    assert "copy-docs-split-2" in result.stderr
    assert uploaded == [
        (len(part_one), part_two[:2]),
        (len(part_one) + 2, part_two[2:]),
    ]
