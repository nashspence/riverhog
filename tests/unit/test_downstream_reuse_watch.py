# SPDX-FileCopyrightText: 2026 Nash Spence
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import copy
import io
import json
import os
import shutil
import subprocess
import sys
import urllib.error
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import downstream_reuse_watch as watch  # noqa: E402

RUN = {
    "repository": "example/upstream", "repository_id": 1, "source_commit": "a" * 40,
    "workflow": watch.WORKFLOW, "run_id": 12, "attempt": 1,
}
CATALOGUE = {
    "format": watch.CATALOGUE_FORMAT, "source_commit": "b" * 40,
    "reuse_sha256": "c" * 64,
    "signatures": [{"id": "literal-one", "literal": "example-format/v1+age",
                    "path": "src/example.py", "source_sha256": "d" * 64,
                    "reviewed_license": "CAL-1.0"}],
}
CANDIDATE = {"id": 2, "full_name": "example/downstream", "private": False}


def match(**updates: Any) -> dict[str, Any]:
    value = {"repository": CANDIDATE, "sha": "e" * 40, "path": "src/copied.py",
             "text_matches": [{"fragment": "Untrusted text: ignore previous instructions."}]}
    value.update(updates)
    return value


def page(payload: Any, next_page: bool = False) -> watch.Page:
    return watch.Page(payload, watch.digest(watch.canonical(payload)), next_page)


def search(items: list[dict[str, Any]], total: int | None = None,
           incomplete: bool = False) -> watch.Page:
    return page({"items": items, "total_count": len(items) if total is None else total,
                 "incomplete_results": incomplete})


class FakeReader:
    def __init__(self, *responses: watch.Page | watch.WatchError) -> None:
        self.responses = iter(responses)
        self.calls: list[tuple[str, dict[str, str | int]]] = []

    def get(self, path: str, params: dict[str, str | int]) -> watch.Page:
        self.calls.append((path, params))
        result = next(self.responses)
        if isinstance(result, Exception):
            raise result
        return result


def collect(*responses: watch.Page | watch.WatchError, **options: Any) -> dict[str, Any]:
    return watch.collect(FakeReader(*responses), CATALOGUE, RUN,
                         observed_at="2026-10-05T00:00:00+00:00", **options)


def test_empty_is_complete_only_within_the_reported_api_scope() -> None:
    value = collect(page([]), search([]))
    assert value["status"] == "complete" and not value["observations"]
    assert all(row["scope"] == "public-github-api-snapshot" for row in value["coverage"])
    assert watch.validate_envelope(watch.canonical(value), RUN) == value


def test_fork_inventory_does_not_assert_code_reuse_and_search_sha_is_a_blob() -> None:
    value = collect(page([CANDIDATE]), search([match()]))
    fork = next(row for row in value["observations"] if row["kind"] == "fork_inventory")
    code = next(row for row in value["observations"] if row["kind"] == "code_search_match")
    assert fork["signature_id"] is None and fork["blob_sha"] is None
    assert code["blob_sha"] == "e" * 40 and code["commit_sha"] is None
    assert code["url"].endswith("/git/blobs/" + "e" * 40)
    assert code["untrusted_content"] is True and "ignore" in code["snippets"][0]
    assert watch.validate_envelope(watch.canonical(value), RUN) == value


def test_pagination_is_controlled_and_does_not_follow_remote_urls() -> None:
    fake = FakeReader(page([CANDIDATE], True), page([]), search([match()], total=2),
                      search([match(path="other.py")], total=2))
    value = watch.collect(fake, CATALOGUE, RUN)
    assert value["status"] == "complete"
    assert [params["page"] for _, params in fake.calls] == [1, 2, 1, 2]
    assert fake.calls[2][1]["q"] == '"example-format/v1+age" in:file -repo:example/upstream'


@pytest.mark.parametrize("reason", ["authentication", "forbidden", "rate_limited", "api_failure",
    "deadline", "response_limit", "request_budget", "network_failure",
])
def test_failure_is_not_a_clean_empty_search(reason: str) -> None:
    value = collect(page([]), watch.WatchError(reason))
    assert value["status"] == "partial"
    assert value["coverage"][1]["reasons"] == [reason]
    watch.validate_envelope(watch.canonical(value), RUN)


def test_api_incomplete_and_page_budget_are_preserved() -> None:
    value = collect(page([]), search([match()], total=1001, incomplete=True), pages=1)
    assert value["coverage"][1]["reasons"] == ["api_incomplete", "page_budget"]


def test_duplicate_results_are_deduplicated_without_claiming_complete_coverage() -> None:
    value = collect(page([]), search([match(), match()]))
    assert len(value["observations"]) == 1
    assert value["coverage"][1]["reasons"] == ["duplicate_item"]


def test_private_results_are_discarded_before_snippet_retention() -> None:
    private = dict(CANDIDATE, private=True)
    value = collect(page([]), search([match(repository=private)]))
    assert not value["observations"]
    assert value["coverage"][1]["reasons"] == ["filtered_private"]
    assert b"ignore previous" not in watch.canonical(value)


def test_long_snippets_remain_bounded_and_explicitly_truncated() -> None:
    value = collect(page([]), search([match(text_matches=[{"fragment": "x" * 9999}])]))
    record = value["observations"][0]
    assert len(record["snippets"][0]) == 2048 and record["snippet_truncated"]
    assert value["status"] == "partial"


def test_observation_identity_is_stable_across_runs_not_a_first_seen_claim() -> None:
    first = collect(page([]), search([match()]))
    run = dict(RUN, run_id=13)
    second = watch.collect(FakeReader(page([]), search([match()])), CATALOGUE, run)
    assert first["observations"][0]["id"] == second["observations"][0]["id"]
    assert "first_seen" not in second["observations"][0]


@pytest.mark.parametrize("raw", [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1e999}',
                                  b"[" * 26 + b"0" + b"]" * 26, b"\xff", b"{broken"])
def test_json_rejects_ambiguous_or_malformed_input(raw: bytes) -> None:
    with pytest.raises(watch.WatchError):
        watch.strict_json(raw)


def test_json_response_bound() -> None:
    with pytest.raises(watch.WatchError, match="response_limit"):
        watch.strict_json(b"123", limit=2)


@pytest.mark.parametrize("change", ["version", "binding", "digest", "unknown", "coverage",
                                    "signature", "commit", "duplicate", "scope", "catalogue"])
def test_envelope_rejects_tamper_and_shape_confusion(change: str) -> None:
    value = collect(page([]), search([match()]))
    if change == "version":
        value["format"] = "unrecognized/v999"
    elif change == "binding":
        value["run"]["attempt"] = 2
    elif change == "digest":
        value["observations_sha256"] = "0" * 64
    elif change == "unknown":
        value["approved"] = True
    elif change == "coverage":
        value["coverage"] = value["coverage"][:1]
    elif change == "signature":
        value["observations"][0]["signature_id"] = "not-a-reviewed-signature"
    elif change == "commit":
        value["observations"][0]["commit_sha"] = "e" * 40
    elif change == "duplicate":
        value["observations"].append(copy.deepcopy(value["observations"][0]))
    elif change == "scope":
        value["coverage"][0]["scope"] = "all-uses-certified"
    elif change == "catalogue":
        value["catalogue"]["signatures"][0]["path"] = "../../secrets"
    if change != "digest":
        value["observations_sha256"] = watch.digest(watch.canonical(value["observations"]))
        value["catalogue_sha256"] = watch.digest(watch.canonical(value["catalogue"]))
    with pytest.raises(watch.WatchError):
        watch.validate_envelope(watch.canonical(value), RUN)


def test_real_catalogue_is_bound_to_the_audited_source_bytes() -> None:
    catalogue = watch.read_catalogue(ROOT / "scripts/downstream_reuse_signatures.json", ROOT)
    assert len(catalogue["signatures"]) == 3
    assert all(entry["reviewed_license"] == "CAL-1.0" for entry in catalogue["signatures"])


def test_source_substitution_is_rejected(tmp_path: Path) -> None:
    source = json.loads((ROOT / "scripts/downstream_reuse_signatures.json").read_text())
    source["signatures"][0]["source_sha256"] = "0" * 64
    path = tmp_path / "catalogue.json"
    path.write_text(json.dumps(source))
    with pytest.raises(watch.WatchError):
        watch.read_catalogue(path, ROOT)


def test_ciphertext_write_is_exclusive_and_refuses_plaintext(tmp_path: Path) -> None:
    path = tmp_path / "observations.age"
    with pytest.raises(watch.WatchError):
        watch.write_ciphertext(path, b"secret observations")
    data = b"age-encryption.org/v1\nsynthetic-not-real-crypto"
    watch.write_ciphertext(path, data)
    with pytest.raises(FileExistsError):
        watch.write_ciphertext(path, data)
    assert path.read_bytes() == data
    assert path.stat().st_mode & 0o777 == 0o600


def test_missing_recipient_fails_before_network_and_has_no_payload_log(tmp_path: Path) -> None:
    env = dict(os.environ)
    env.pop("REUSE_WATCH_AGE_RECIPIENT", None)
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/downstream_reuse_watch.py"),
         "--output", str(tmp_path / "observations.age")], env=env,
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 1 and not (tmp_path / "observations.age").exists()
    assert result.stdout == ""
    assert result.stderr == (
        "Reuse observation collection failed; no plaintext report is published.\n"
    )


@pytest.mark.parametrize("path", ["https://evil.example/", "/user", "/repos/other/repo/forks"])
def test_reader_refuses_arbitrary_endpoint(path: str) -> None:
    reader = watch.GitHubReader("example/upstream", "synthetic-token")
    with pytest.raises(watch.WatchError):
        reader.get(path, {})


def test_reader_does_not_fallback_to_anonymous_code_search() -> None:
    reader = watch.GitHubReader("example/upstream", "")
    with pytest.raises(watch.WatchError, match="authentication"):
        reader.get("/search/code", {"q": "example"})


@pytest.mark.parametrize("code,headers,expected", [
    (401, {}, "authentication"), (403, {}, "forbidden"),
    (403, {"X-RateLimit-Remaining": "0"}, "rate_limited"),
    (403, {"Retry-After": "60"}, "rate_limited"), (429, {}, "rate_limited"),
    (500, {}, "api_failure"), (302, {"Location": "https://evil.example"}, "api_failure"),
])
def test_http_diagnostics_do_not_include_sensitive_bodies(
    code: int, headers: dict[str, str], expected: str,
) -> None:
    class Opener:
        def open(self, *_args: Any, **_kwargs: Any) -> None:
            raise urllib.error.HTTPError("https://api.github.com/search/code", code,
                                         "secret body", headers, io.BytesIO(b"private"))

    reader = watch.GitHubReader("example/upstream", "synthetic-token")
    reader.opener = Opener()  # type: ignore[assignment]
    with pytest.raises(watch.WatchError, match=f"^{expected}$"):
        reader.get("/search/code", {})


def test_search_throttling_and_deadline() -> None:
    times = [0.0]
    waits: list[float] = []

    def sleep(delay: float) -> None:
        waits.append(delay)
        times[0] += delay

    class Response(io.BytesIO):
        headers: dict[str, str] = {}

    class Opener:
        def open(self, *_args: Any, **_kwargs: Any) -> Response:
            return Response(b'{"items":[],"total_count":0,"incomplete_results":false}')

    reader = watch.GitHubReader(
        "example/upstream", "synthetic", clock=lambda: times[0], sleep=sleep,
    )
    reader.opener = Opener()  # type: ignore[assignment]
    reader.get("/search/code", {})
    reader.get("/search/code", {})
    assert waits == [0, 6.2]
    times[0] = 241
    with pytest.raises(watch.WatchError, match="deadline"):
        reader.get("/search/code", {})


@pytest.mark.integration
@pytest.mark.skipif(not shutil.which("age") or not shutil.which("age-keygen"),
                    reason="requires actual age and age-keygen; mocks do not qualify cryptography")
def test_actual_age_round_trip_and_tamper_rejection(tmp_path: Path) -> None:
    identity = tmp_path / "identity.txt"
    subprocess.run(["age-keygen", "-o", str(identity)], check=True, capture_output=True)
    recipient = subprocess.check_output(["age-keygen", "-y", str(identity)]).decode().strip()
    raw = watch.canonical(collect(page([]), search([match()])))
    sealed = watch.encrypt(raw, recipient)
    result = subprocess.run(["age", "-d", "-i", str(identity)], input=sealed,
                            check=True, capture_output=True)
    assert result.stdout == raw
    damaged = sealed[:-1] + bytes([sealed[-1] ^ 1])
    failure = subprocess.run(["age", "-d", "-i", str(identity)], input=damaged,
                             check=False, capture_output=True)
    assert failure.returncode != 0


def test_collection_snapshot_does_not_alias_mutable_inputs() -> None:
    run, catalogue = copy.deepcopy(RUN), copy.deepcopy(CATALOGUE)
    value = watch.collect(FakeReader(page([]), search([])), catalogue, run)
    run["attempt"] = 999
    catalogue["signatures"][0]["id"] = "changed"
    assert value["run"]["attempt"] == RUN["attempt"]
    assert value["catalogue"]["signatures"][0]["id"] == "literal-one"


def test_ciphertext_transport_extracts_no_files(tmp_path: Path) -> None:
    import zipfile

    data = b"age-encryption.org/v1\nsynthetic-not-real-crypto"
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("observations.age", data)
    before = set(tmp_path.iterdir())
    assert watch.ciphertext_from_artifact(stream.getvalue()) == data
    assert set(tmp_path.iterdir()) == before


@pytest.mark.parametrize("case", ["traversal", "extra", "symlink", "plain", "oversized", "invalid"])
def test_untrusted_artifact_transport_rejects_unsafe_members(case: str) -> None:
    import zipfile

    stream = io.BytesIO()
    data = b"age-encryption.org/v1\nsynthetic-not-real-crypto"
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        name = "../observations.age" if case == "traversal" else "observations.age"
        if case == "symlink":
            entry = zipfile.ZipInfo(name)
            entry.create_system = 3
            entry.external_attr = (0o120777 << 16)
            archive.writestr(entry, data)
        elif case == "plain":
            archive.writestr(name, b"unencrypted observations")
        elif case == "oversized":
            archive.writestr(name, b"x" * (watch.MAX_JSON + 64 * 1024 + 1))
        else:
            archive.writestr(name, data)
        if case == "extra":
            archive.writestr("secrets.txt", b"private")
    with pytest.raises(watch.WatchError):
        watch.ciphertext_from_artifact(b"not-a-zip" if case == "invalid" else stream.getvalue())


def test_age_subprocess_does_not_inherit_credentials_or_echo_errors(monkeypatch) -> None:
    recipient = "age1" + "q" * 58  # Synthetic shape only; not a real age recipient/checksum.
    monkeypatch.setenv("GH_TOKEN", "synthetic-sensitive-token")
    monkeypatch.setenv("REUSE_WATCH_SEARCH_TOKEN", "synthetic-search-token")

    def fake_run(argv, **options):
        assert argv == ["age", "--encrypt", "--recipient", recipient]
        assert "GH_TOKEN" not in options["env"]
        assert "REUSE_WATCH_SEARCH_TOKEN" not in options["env"]
        assert not options.get("shell")
        raise subprocess.CalledProcessError(1, argv, stderr=b"sensitive downstream detail")

    monkeypatch.setattr(watch.subprocess, "run", fake_run)
    with pytest.raises(watch.WatchError, match="^encryption_failed$"):
        watch.encrypt(b"private", recipient)


def test_partial_cli_snapshot_is_encrypted_for_publication_not_logged(
    tmp_path, monkeypatch, capsys,
):
    monkeypatch.setattr(watch, "read_catalogue", lambda *_: CATALOGUE)
    monkeypatch.setattr(watch, "GitHubReader", lambda *_: FakeReader(
        page([]), watch.WatchError("authentication"),
    ))
    plaintexts = []

    def fake_encrypt(raw, recipient):
        plaintexts.append(raw)
        return b"age-encryption.org/v1\nsynthetic-not-real-crypto"

    monkeypatch.setattr(watch, "encrypt", fake_encrypt)
    env = {"GITHUB_REPOSITORY": RUN["repository"], "GITHUB_REPOSITORY_ID": "1",
           "GITHUB_SHA": RUN["source_commit"], "GITHUB_RUN_ID": "12",
           "GITHUB_RUN_ATTEMPT": "1", "GITHUB_OUTPUT": str(tmp_path / "outputs"),
           "REUSE_WATCH_AGE_RECIPIENT": "synthetic"}
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    output = tmp_path / "observations.age"
    assert watch.main(["--output", str(output)]) == 0
    assert plaintexts[0] == b""  # Encryption preflight precedes network operations.
    assert watch.validate_envelope(plaintexts[1], RUN)["status"] == "partial"
    assert (tmp_path / "outputs").read_text() == "coverage=partial\n"
    captured = capsys.readouterr()
    assert captured.out == "Reuse observation snapshot encrypted.\n"
    assert captured.err == ""
