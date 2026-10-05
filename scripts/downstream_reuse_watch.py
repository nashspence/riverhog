# SPDX-FileCopyrightText: 2026 Nash Spence
# SPDX-License-Identifier: Apache-2.0
"""Bounded, observational GitHub reuse collection; never a compliance verdict.

The private consumer must authenticate ciphertext provenance before decryption.
``validate_envelope`` checks structure/bindings only, not authenticity or legal truth.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

FORMAT = "riverhog-downstream-reuse/v1"
CATALOGUE_FORMAT = "riverhog-reuse-signatures/v1"
WORKFLOW = ".github/workflows/downstream-reuse-watch.yml"
MAX_JSON = 8 * 1024 * 1024
MAX_RESPONSE = 2 * 1024 * 1024
MAX_OBSERVATIONS = 1200
SHA = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"[0-9a-f]{64}")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
NATIVE_RECIPIENT = re.compile(r"age1[023456789acdefghjklmnpqrstuvwxyz]{58}")
RUN_KEYS = {"repository", "repository_id", "source_commit", "workflow", "run_id", "attempt"}
REASONS = {
    "authentication", "forbidden", "rate_limited", "api_failure", "network_failure",
    "response_limit", "invalid_response", "request_budget", "deadline", "page_budget",
    "api_incomplete", "filtered_private", "invalid_item", "snippet_truncated",
    "duplicate_item", "result_limit", "changed_result_count",
}


class WatchError(Exception):
    """Fixed diagnostic category; never carry remote bodies, tokens or snippets."""


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode("ascii")


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def require(condition: bool) -> None:
    if not condition:
        raise WatchError("invalid_response")


def strict_json(raw: bytes, limit: int = MAX_JSON) -> Any:
    if len(raw) > limit:
        raise WatchError("response_limit")

    def pairs(entries: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in entries:
            require(key not in result)
            result[key] = value
        return result

    def constant(_: str) -> None:
        raise WatchError("invalid_response")

    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
        pending = [(value, 0)]
        while pending:
            item, depth = pending.pop()
            require(depth <= 24)
            if isinstance(item, float):
                require(math.isfinite(item))
            if isinstance(item, dict):
                pending.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list):
                pending.extend((child, depth + 1) for child in item)
        return value
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise WatchError("invalid_response") from exc


def validate_run(run: Any) -> None:
    require(isinstance(run, dict) and set(run) == RUN_KEYS)
    require(isinstance(run["repository"], str)
            and REPOSITORY.fullmatch(run["repository"]) is not None)
    require(isinstance(run["source_commit"], str)
            and SHA.fullmatch(run["source_commit"]) is not None)
    require(run["workflow"] == WORKFLOW)
    for key in ("repository_id", "run_id", "attempt"):
        require(type(run[key]) is int and 0 < run[key] < 2**63)


def validate_catalogue(value: Any) -> None:
    require(isinstance(value, dict) and set(value) == {
        "format", "source_commit", "reuse_sha256", "signatures",
    })
    require(value["format"] == CATALOGUE_FORMAT)
    require(isinstance(value["source_commit"], str)
            and SHA.fullmatch(value["source_commit"]) is not None)
    require(isinstance(value["reuse_sha256"], str)
            and DIGEST.fullmatch(value["reuse_sha256"]) is not None)
    entries = value["signatures"]
    require(isinstance(entries, list) and 1 <= len(entries) <= 8)
    seen: set[str] = set()
    for entry in entries:
        require(isinstance(entry, dict) and set(entry) == {
            "id", "path", "source_sha256", "literal", "reviewed_license",
        })
        require(all(isinstance(item, str) for item in entry.values()))
        require(re.fullmatch(r"[a-z0-9-]{1,64}", entry["id"]) is not None)
        require(entry["id"] not in seen)
        seen.add(entry["id"])
        require(re.fullmatch(r"[a-z0-9/+._-]{16,120}", entry["literal"]) is not None)
        require(DIGEST.fullmatch(entry["source_sha256"]) is not None)
        pathname = entry["path"]
        require(bool(pathname) and not pathname.startswith(("/", "-"))
                and ".." not in pathname.split("/") and "\x00" not in pathname)
        require(entry["reviewed_license"] in {"CAL-1.0", "Apache-2.0"})


def read_catalogue(path: Path, root: Path) -> dict[str, Any]:
    """Check exact reviewed source bytes, not a replacement REUSE license resolver."""
    value = strict_json(path.read_bytes())
    validate_catalogue(value)

    def source(pathname: str) -> bytes:
        try:
            result = subprocess.run(
                ["git", "show", f"{value['source_commit']}:{pathname}"], cwd=root,
                check=True, capture_output=True, timeout=15,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise WatchError("invalid_response") from exc
        require(len(result.stdout) <= MAX_JSON)
        return result.stdout

    require(digest(source("REUSE.toml")) == value["reuse_sha256"])
    for entry in value["signatures"]:
        raw = source(entry["path"])
        require(digest(raw) == entry["source_sha256"] and entry["literal"].encode() in raw)
    return cast(dict[str, Any], value)


@dataclass(frozen=True)
class Page:
    payload: Any
    response_sha256: str
    has_next: bool = False


class Reader(Protocol):
    def get(self, path: str, params: dict[str, str | int]) -> Page: ...


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: Any, msg: Any,
                         headers: Any, newurl: Any) -> None:
        return None


class GitHubReader:
    """GET only, fixed origin, no credential-bearing redirects or proxy inheritance."""

    def __init__(self, repository: str, token: str, search_token: str = "", *,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        require(REPOSITORY.fullmatch(repository) is not None)
        self.repository, self.token, self.search_token = repository, token, search_token or token
        self.clock, self.sleep = clock, sleep
        self.deadline = clock() + 240
        self.next_search = clock()
        self.calls = 0
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def get(self, path: str, params: dict[str, str | int]) -> Page:
        require(path in {f"/repos/{self.repository}/forks", "/search/code"})
        search = path == "/search/code"
        token = self.search_token if search else self.token
        if search and not token:
            raise WatchError("authentication")
        if self.calls >= 32:
            raise WatchError("request_budget")
        if search:
            delay = max(0.0, self.next_search - self.clock())
            if self.clock() + delay >= self.deadline:
                raise WatchError("deadline")
            self.sleep(delay)
            self.next_search = self.clock() + 6.2
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise WatchError("deadline")
        self.calls += 1
        url = "https://api.github.com" + path + "?" + urllib.parse.urlencode(params)
        headers = {"Accept": "application/vnd.github.text-match+json",
                   "X-GitHub-Api-Version": "2026-03-10",
                   "User-Agent": "riverhog-downstream-reuse-watch/1"}
        if token:
            headers["Authorization"] = "Bearer " + token
        try:
            request = urllib.request.Request(url, headers=headers, method="GET")
            with self.opener.open(request, timeout=min(20, remaining)) as response:
                raw = response.read(MAX_RESPONSE + 1)
                has_next = 'rel="next"' in response.headers.get("Link", "")
            return Page(strict_json(raw, MAX_RESPONSE), digest(raw), has_next)
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                reason = "authentication"
            elif exc.code == 429 or (exc.code == 403 and (
                exc.headers.get("X-RateLimit-Remaining") == "0"
                or exc.headers.get("Retry-After") is not None
            )):
                reason = "rate_limited"
            elif exc.code == 403:
                reason = "forbidden"
            else:
                reason = "api_failure"
            exc.close()
            raise WatchError(reason) from None
        except (urllib.error.URLError, OSError) as exc:
            raise WatchError("network_failure") from exc


def observation(item: Any, signature: str | None) -> dict[str, Any]:
    require(isinstance(item, dict))
    repository = item if signature is None else item.get("repository")
    require(isinstance(repository, dict))
    if repository.get("private") is not False:
        raise WatchError("filtered_private")
    name, repo_id = repository.get("full_name"), repository.get("id")
    require(isinstance(name, str) and REPOSITORY.fullmatch(name) is not None)
    require(type(repo_id) is int and 0 < repo_id < 2**63)
    record: dict[str, Any] = {
        "kind": "fork_inventory" if signature is None else "code_search_match",
        "repository": name, "repository_id": repo_id, "signature_id": signature,
        "path": None, "blob_sha": None, "commit_sha": None,
        "url": f"https://github.com/{name}", "snippets": [],
        "snippet_truncated": False, "untrusted_content": True,
    }
    if signature is not None:
        path, sha = item.get("path"), item.get("sha")
        require(isinstance(path, str) and 0 < len(path) <= 2048 and "\x00" not in path)
        require(isinstance(sha, str) and SHA.fullmatch(sha) is not None)
        record.update(
            path=path, blob_sha=sha, url=f"https://api.github.com/repos/{name}/git/blobs/{sha}",
        )
        matches = item.get("text_matches", [])
        require(isinstance(matches, list))
        fragments = [match.get("fragment") for match in matches if isinstance(match, dict)]
        require(all(isinstance(fragment, str) for fragment in fragments))
        record["snippets"] = [fragment[:2048] for fragment in fragments[:2]]
        record["snippet_truncated"] = len(fragments) > 2 or any(
            len(fragment) > 2048 for fragment in fragments
        )
    # Snapshot identity: renames change the locator, not the repository's numeric identity.
    identity = [
        record[key] for key in ("kind", "repository_id", "signature_id", "path", "blob_sha")
    ]
    record["id"] = digest(canonical(identity))
    return record


def collect(reader: Reader, catalogue: dict[str, Any], run: dict[str, Any], *,
            pages: int = 3, observed_at: str | None = None) -> dict[str, Any]:
    validate_run(run)
    validate_catalogue(catalogue)
    run = strict_json(canonical(run))
    catalogue = strict_json(canonical(catalogue))
    require(type(pages) is int and 1 <= pages <= 10)
    observations: dict[str, dict[str, Any]] = {}
    coverage: list[dict[str, Any]] = []
    tasks = [(None, None), *((item["id"], item["literal"]) for item in catalogue["signatures"])]
    for signature, literal in tasks:
        reasons: set[str] = set()
        hashes: list[str] = []
        received = 0
        seen: set[str] = set()
        last_total: int | None = None
        finished = False
        for page_number in range(1, pages + 1):
            params: dict[str, str | int] = {"per_page": 100, "page": page_number}
            path = f"/repos/{run['repository']}/forks"
            if signature is not None:
                path = "/search/code"
                params["q"] = f'"{literal}" in:file -repo:{run["repository"]}'
            else:
                params["sort"] = "oldest"
            try:
                page = reader.get(path, params)
                hashes.append(page.response_sha256)
                if signature is None:
                    items, total = page.payload, None
                else:
                    data = page.payload
                    require(isinstance(data, dict))
                    items, total = data.get("items"), data.get("total_count")
                    require(type(total) is int and total >= 0)
                    require(type(data.get("incomplete_results")) is bool)
                    if data["incomplete_results"]:
                        reasons.add("api_incomplete")
                    if last_total is not None and last_total != total:
                        reasons.add("changed_result_count")
                    last_total = total
                require(isinstance(items, list) and len(items) <= 100)
                received += len(items)
                more = page.has_next or (total is not None and received < total)
                for item in items:
                    try:
                        record = observation(item, signature)
                    except WatchError as exc:
                        reasons.add("filtered_private" if str(exc) == "filtered_private"
                                    else "invalid_item")
                        continue
                    if record["repository_id"] == run["repository_id"]:
                        continue
                    if record["id"] in seen:
                        reasons.add("duplicate_item")
                    seen.add(record["id"])
                    if record["snippet_truncated"]:
                        reasons.add("snippet_truncated")
                    if len(observations) >= MAX_OBSERVATIONS and record["id"] not in observations:
                        reasons.add("result_limit")
                        continue
                    observations[record["id"]] = record
                if not more:
                    finished = True
                    break
            except WatchError as exc:
                reasons.add(str(exc) if str(exc) in REASONS else "invalid_response")
                break
        if not finished and not reasons:
            reasons.add("page_budget")
        elif not finished and len(hashes) == pages:
            reasons.add("page_budget")
        coverage.append({
            "query_id": signature or "fork-inventory", "scope": "public-github-api-snapshot",
            "status": "partial" if reasons else "complete", "reasons": sorted(reasons),
            "response_sha256": hashes, "received_items": received,
        })
    records = sorted(observations.values(), key=lambda item: item["id"])
    return {
        "format": FORMAT, "run": run,
        "observed_at": observed_at or datetime.now(UTC).isoformat(),
        "catalogue": catalogue, "catalogue_sha256": digest(canonical(catalogue)),
        "coverage": coverage, "observations": records,
        "observations_sha256": digest(canonical(records)),
        "status": "complete" if all(item["status"] == "complete" for item in coverage)
                  else "partial",
    }


def validate_envelope(raw: bytes, expected_run: dict[str, Any]) -> dict[str, Any]:
    """Offline shape/binding check AFTER external provenance verification, not instead."""
    value = strict_json(raw)
    require(isinstance(value, dict) and set(value) == {
        "format", "run", "observed_at", "catalogue", "catalogue_sha256", "coverage",
        "observations", "observations_sha256", "status",
    })
    require(value["format"] == FORMAT)
    validate_run(value["run"])
    validate_run(expected_run)
    require(value["run"] == expected_run)
    require(isinstance(value["observed_at"], str))
    try:
        require(datetime.fromisoformat(value["observed_at"]).utcoffset() is not None)
    except ValueError as exc:
        raise WatchError("invalid_response") from exc
    catalogue = value["catalogue"]
    validate_catalogue(catalogue)
    signature_ids = {entry["id"] for entry in catalogue["signatures"]}
    require(value["catalogue_sha256"] == digest(canonical(catalogue)))
    records = value["observations"]
    require(isinstance(records, list) and len(records) <= MAX_OBSERVATIONS)
    require(value["observations_sha256"] == digest(canonical(records)))
    seen: set[str] = set()
    for record in records:
        require(isinstance(record, dict) and set(record) == {
            "kind", "repository", "repository_id", "signature_id", "path", "blob_sha",
            "commit_sha", "url", "snippets", "snippet_truncated", "untrusted_content", "id",
        })
        signature = record["signature_id"]
        require(signature is None or (isinstance(signature, str) and signature in signature_ids))
        reconstructed = observation({
            "repository": {"full_name": record["repository"], "id": record["repository_id"],
                           "private": False},
            "full_name": record["repository"], "id": record["repository_id"], "private": False,
            "path": record["path"], "sha": record["blob_sha"],
        }, signature)
        for key in (
            "id", "kind", "repository", "repository_id", "path", "blob_sha", "commit_sha", "url",
        ):
            require(record[key] == reconstructed[key])
        require(record["id"] not in seen and record["untrusted_content"] is True)
        seen.add(record["id"])
        require(type(record["snippet_truncated"]) is bool and isinstance(record["snippets"], list))
        require(len(record["snippets"]) <= 2 and all(
            isinstance(text, str) and len(text) <= 2048 for text in record["snippets"]
        ))
    coverage = value["coverage"]
    require(isinstance(coverage, list) and 1 <= len(coverage) <= 9)
    require([item.get("query_id") for item in coverage if isinstance(item, dict)]
            == ["fork-inventory", *(entry["id"] for entry in catalogue["signatures"])])
    for item in coverage:
        require(isinstance(item, dict) and set(item) == {
            "query_id", "scope", "status", "reasons", "response_sha256", "received_items",
        })
        require(isinstance(item["query_id"], str) and item["scope"] == "public-github-api-snapshot")
        require(isinstance(item["reasons"], list) and all(
            isinstance(reason, str) and reason in REASONS for reason in item["reasons"]
        ))
        require(item["status"] == ("partial" if item["reasons"] else "complete"))
        require(type(item["received_items"]) is int and 0 <= item["received_items"] <= 1000)
        require(isinstance(item["response_sha256"], list) and len(item["response_sha256"]) <= 10)
        require(all(isinstance(sha, str) and DIGEST.fullmatch(sha)
                    for sha in item["response_sha256"]))
    require(value["status"] == ("complete" if all(
        item["status"] == "complete" for item in coverage
    ) else "partial"))
    return cast(dict[str, Any], value)


def ciphertext_from_artifact(raw_zip: bytes) -> bytes:
    """Bounded ZIP transport unpacking, without filesystem writes or trust claims.

    Next verify the returned ciphertext's attestation, THEN decrypt. The ZIP's
    transport digest and its member's attested digest identify different bytes.
    """
    require(len(raw_zip) <= MAX_JSON + 128 * 1024)
    try:
        with zipfile.ZipFile(io.BytesIO(raw_zip)) as archive:
            entries = archive.infolist()
            require(len(entries) == 1)
            entry = entries[0]
            require(entry.filename == "observations.age" and not entry.is_dir())
            require(not (entry.flag_bits & 1))
            kind = (entry.external_attr >> 16) & 0o170000
            require(kind in (0, 0o100000))
            require(entry.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED))
            require(entry.file_size <= MAX_JSON + 64 * 1024)
            with archive.open(entry) as source:
                ciphertext = source.read(MAX_JSON + 64 * 1024 + 1)
            require(len(ciphertext) == entry.file_size)
            require(ciphertext.startswith(b"age-encryption.org/v1\n"))
            return ciphertext
    except (zipfile.BadZipFile, OSError, RuntimeError, ValueError) as exc:
        raise WatchError("invalid_response") from exc


def encrypt(raw: bytes, recipient: str) -> bytes:
    require(len(raw) <= MAX_JSON and NATIVE_RECIPIENT.fullmatch(recipient) is not None)
    # No token inheritance; only native age recipients, no plugins or shell invocation.
    env = {key: os.environ[key] for key in ("PATH", "SYSTEMROOT") if key in os.environ}
    try:
        result = subprocess.run(["age", "--encrypt", "--recipient", recipient], input=raw,
                                capture_output=True, check=True, timeout=30, env=env)
    except (OSError, subprocess.SubprocessError) as exc:
        raise WatchError("encryption_failed") from exc
    require(result.stdout.startswith(b"age-encryption.org/v1\n"))
    return result.stdout


def write_ciphertext(path: Path, ciphertext: bytes) -> None:
    require(path.suffix == ".age" and ciphertext.startswith(b"age-encryption.org/v1\n"))
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(ciphertext)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--catalogue", type=Path,
                        default=Path(__file__).with_name("downstream_reuse_signatures.json"))
    args = parser.parse_args(argv)
    try:
        recipient = os.environ.get("REUSE_WATCH_AGE_RECIPIENT", "")
        # Validate native recipient checksum/tool BEFORE network collection.
        encrypt(b"", recipient)
        catalogue = read_catalogue(args.catalogue, Path(__file__).resolve().parents[1])
        run = {"repository": os.environ["GITHUB_REPOSITORY"],
               "repository_id": int(os.environ["GITHUB_REPOSITORY_ID"]),
               "source_commit": os.environ["GITHUB_SHA"], "workflow": WORKFLOW,
               "run_id": int(os.environ["GITHUB_RUN_ID"]),
               "attempt": int(os.environ["GITHUB_RUN_ATTEMPT"])}
        reader = GitHubReader(run["repository"], os.environ.get("GH_TOKEN", ""),
                              os.environ.get("REUSE_WATCH_SEARCH_TOKEN", ""))
        envelope = collect(reader, catalogue, run)
        raw = canonical(envelope)
        validate_envelope(raw, run)
        write_ciphertext(args.output, encrypt(raw, recipient))
        output = os.environ.get("GITHUB_OUTPUT")
        if output:
            with Path(output).open("a", encoding="utf-8") as stream:
                stream.write(f"coverage={envelope['status']}\n")
        print("Reuse observation snapshot encrypted.")
        return 0  # Workflow attests/uploads an accurate partial snapshot before failing its status.
    except (WatchError, OSError, ValueError, KeyError, TypeError, RecursionError):
        print(
            "Reuse observation collection failed; no plaintext report is published.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
