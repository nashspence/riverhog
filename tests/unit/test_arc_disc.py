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
    plaintext = b"invoice fixture bytes\n"
    uploaded: list[tuple[str, str, str, bytes]] = []

    class FakeClient:
        def get_fetch_manifest(self, fetch_id: str) -> dict[str, object]:
            assert fetch_id == "fx-1"
            return _manifest_for(plaintext)

        def upload_fetch_entry(self, fetch_id: str, entry_id: str, sha256: str, content: bytes) -> dict[str, object]:
            uploaded.append((fetch_id, entry_id, sha256, content))
            return {"entry": entry_id, "accepted": True, "bytes": len(content)}

        def complete_fetch(self, fetch_id: str) -> dict[str, object]:
            assert fetch_id == "fx-1"
            return {"id": fetch_id, "state": "done"}

    class FakeReader:
        def read(self, disc_path: str, *, device: str) -> bytes:
            assert disc_path == "disc/000001.bin"
            assert device == "/dev/fake-sr0"
            return b"ciphertext"

    class FakeCrypto:
        def decrypt_entry(self, encrypted: bytes, enc: dict[str, object]) -> bytes:
            assert encrypted == b"ciphertext"
            assert enc["fixture_key"] == "fixture-1"
            return plaintext

    monkeypatch.setattr(arc_disc_main, "ApiClient", FakeClient)
    monkeypatch.setattr(arc_disc_main, "build_optical_reader", lambda: FakeReader())
    monkeypatch.setattr(arc_disc_main, "build_crypto", lambda: FakeCrypto())

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
    assert uploaded == [("fx-1", "e1", hashlib.sha256(plaintext).hexdigest(), plaintext)]


def test_arc_disc_fetch_reports_clean_error_when_optical_read_fails(monkeypatch) -> None:
    class FakeClient:
        def get_fetch_manifest(self, fetch_id: str) -> dict[str, object]:
            return _manifest_for(b"invoice fixture bytes\n")

    class FailingReader:
        def read(self, disc_path: str, *, device: str) -> bytes:
            raise RuntimeError(f"fixture optical read failed for {disc_path} on {device}")

    class FakeCrypto:
        def decrypt_entry(self, encrypted: bytes, enc: dict[str, object]) -> bytes:
            raise AssertionError("decrypt should not be called after optical read failure")

    monkeypatch.setattr(arc_disc_main, "ApiClient", FakeClient)
    monkeypatch.setattr(arc_disc_main, "build_optical_reader", lambda: FailingReader())
    monkeypatch.setattr(arc_disc_main, "build_crypto", lambda: FakeCrypto())

    result = runner.invoke(
        arc_disc_main.app,
        ["fetch", "fx-1", "--device", "/dev/fake-sr0"],
        input="\n",
    )

    assert result.exit_code == 1
    assert "error: fixture optical read failed for disc/000001.bin on /dev/fake-sr0" in result.stderr
    assert "Traceback" not in result.stderr
