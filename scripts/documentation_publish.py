#!/usr/bin/env python3
"""Publish a verified static site through immutable B2 content and a Worker."""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import json
import os
import re
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

from contract_atlas.github_publication import GitHubPublication, require_fresh_snapshot
from contract_atlas.model import ContractAtlasError, canonical_bytes
from contract_atlas.publication import file_sha256, safe_relative
from contract_atlas.web_publication import (
    PAGES_LIMIT,
    SITE_FILENAME,
    Site,
    build_relay,
    public_base_url,
)

AGENT = "riverhog-documentation/1.0"
OWNER_TAG = "riverhog-documentation"
CHUNK = 1024 * 1024


class PublicationError(RuntimeError):
    """An error safe to report without credentials or provider response bodies."""


class ServiceError(PublicationError):
    def __init__(self, operation: str, status: int | None = None) -> None:
        self.status = status
        super().__init__(operation + (f" failed (HTTP {status})" if status else " failed"))


def _json_request(
    operation: str,
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: Any = None,
) -> dict[str, Any]:
    selected = {"User-Agent": AGENT, "Accept": "application/json", **(headers or {})}
    if isinstance(body, dict):
        body = canonical_bytes(body)
        selected["Content-Type"] = "application/json"
    request = Request(url, data=body, headers=selected, method=method)
    try:
        with urlopen(request, timeout=120) as response:
            raw = response.read(CHUNK + 1)
            if len(raw) > CHUNK:
                raise PublicationError("provider response exceeds its metadata budget")
            result = json.loads(raw)
            if not isinstance(result, dict) or result.get("success") is False:
                raise PublicationError(operation + " was rejected")
            return result
    except HTTPError as exc:
        status = exc.code
        exc.close()
        raise ServiceError(operation, status) from None
    except (URLError, TimeoutError, OSError):
        raise ServiceError(operation) from None
    except (TypeError, ValueError):
        raise PublicationError(operation + " returned invalid metadata") from None


def _verified_chunks(stream: Any, digest: str, size: int) -> Iterator[bytes]:
    observed = hashlib.sha256()
    count = 0
    while chunk := stream.read(CHUNK):
        observed.update(chunk)
        count += len(chunk)
        yield chunk
    if count != size or observed.hexdigest() != digest:
        raise PublicationError("local publication content changed during upload")


def _verify_download_once(url: str, digest: str, size: int) -> None:
    request = Request(url, headers={"User-Agent": AGENT, "Accept-Encoding": "identity"})
    try:
        with urlopen(request, timeout=60) as response:
            observed = hashlib.sha256()
            count = 0
            while chunk := response.read(CHUNK):
                count += len(chunk)
                if count > size:
                    raise PublicationError("public content exceeds its verified size")
                observed.update(chunk)
            if response.status != 200 or count != size or observed.hexdigest() != digest:
                raise PublicationError("public content identity differs")
    except HTTPError as exc:
        status = exc.code
        exc.close()
        raise ServiceError("public content verification", status) from None
    except (URLError, TimeoutError, OSError):
        raise ServiceError("public content verification") from None


def verify_download(url: str, digest: str, size: int) -> None:
    for attempt in range(3):
        try:
            _verify_download_once(url, digest, size)
            return
        except ServiceError as exc:
            if (
                attempt == 2
                or exc.status is None
                or (exc.status not in (404, 429) and exc.status < 500)
            ):
                raise
            time.sleep(2**attempt)


class Storage(Protocol):
    base_url: str

    def upload(self, digest: str, path: Path, size: int) -> bool: ...


class Workers(Protocol):
    def url(self, name: str) -> str: ...

    def deploy(
        self, name: str, module: bytes, origin: str, mount: str, generation: str
    ) -> None: ...

    def remove(self, name: str, generation: str) -> None: ...


class B2Storage:
    def __init__(self, key_id: str, key: str, bucket_id: str, prefix: str) -> None:
        safe_relative(prefix.rstrip("/"))
        if not prefix.endswith("/"):
            raise PublicationError("content prefix must end in a slash")
        basic = base64.b64encode(f"{key_id}:{key}".encode()).decode()
        auth = _json_request(
            "storage authorization",
            "https://api.backblazeb2.com/b2api/v4/b2_authorize_account",
            headers={"Authorization": "Basic " + basic},
        )
        storage = auth["apiInfo"]["storageApi"]
        self.api = public_base_url(storage["apiUrl"].rstrip("/") + "/").rstrip("/")
        self.headers = {"Authorization": auth["authorizationToken"]}
        self.bucket_id = bucket_id
        allowed = storage["allowed"]
        if (
            "writeFiles" not in allowed["capabilities"]
            or (
                allowed.get("buckets") is not None
                and not any(bucket["id"] == bucket_id for bucket in allowed["buckets"])
            )
            or not prefix.startswith(allowed.get("namePrefix") or "")
        ):
            raise PublicationError("storage key does not allow the publication content prefix")
        buckets = _json_request(
            "publication bucket lookup",
            self.api + "/b2api/v4/b2_list_buckets",
            method="POST",
            headers=self.headers,
            body={"accountId": auth["accountId"], "bucketId": bucket_id},
        )
        selected = [bucket for bucket in buckets["buckets"] if bucket["bucketId"] == bucket_id]
        if len(selected) != 1 or selected[0]["bucketType"] != "allPublic":
            raise PublicationError("documentation requires the configured public bucket")
        bucket = selected[0]
        for rule in bucket.get("lifecycleRules", []):
            rule_prefix = rule.get("fileNamePrefix", "")
            if (prefix.startswith(rule_prefix) or rule_prefix.startswith(prefix)) and rule.get(
                "daysFromUploadingToHiding"
            ) is not None:
                raise PublicationError("publication storage expires live content")
        self.prefix = prefix
        self.base_url = public_base_url(
            storage["downloadUrl"].rstrip("/")
            + "/file/"
            + quote(bucket["bucketName"], safe="")
            + "/"
            + quote(prefix, safe="/")
        )
        self.local = threading.local()

    def _exists(self, digest: str, size: int) -> bool:
        try:
            with urlopen(
                Request(self.base_url + digest, method="HEAD", headers={"User-Agent": AGENT}),
                timeout=30,
            ) as response:
                if response.status != 200 or int(response.headers["Content-Length"]) != size:
                    raise PublicationError("immutable stored content has a different size")
                return True
        except HTTPError as exc:
            status = exc.code
            exc.close()
            if status == 404:
                return False
            raise ServiceError("stored content lookup", status) from None
        except (URLError, TimeoutError, OSError):
            raise ServiceError("stored content lookup") from None

    def upload(self, digest: str, path: Path, size: int) -> bool:
        for attempt in range(3):
            if self._exists(digest, size):
                return False
            with path.open("rb") as stream:
                sha1 = hashlib.file_digest(stream, "sha1").hexdigest()
                stream.seek(0)
                if not getattr(self.local, "upload", None):
                    self.local.upload = _json_request(
                        "upload authorization",
                        self.api + "/b2api/v4/b2_get_upload_url",
                        method="POST",
                        headers=self.headers,
                        body={"bucketId": self.bucket_id},
                    )
                upload = self.local.upload
                try:
                    result = _json_request(
                        "content upload",
                        upload["uploadUrl"],
                        method="POST",
                        headers={
                            "Authorization": upload["authorizationToken"],
                            "Content-Type": "application/octet-stream",
                            "Content-Length": str(size),
                            "X-Bz-File-Name": quote(self.prefix + digest, safe="/"),
                            "X-Bz-Content-Sha1": sha1,
                            "X-Bz-Info-sha256": digest,
                            "X-Bz-Info-b2-cache-control": quote(
                                "public, max-age=31536000, immutable", safe=""
                            ),
                        },
                        body=_verified_chunks(stream, digest, size),
                    )
                except ServiceError as exc:
                    self.local.upload = None
                    if attempt == 2 or (
                        exc.status is not None and exc.status < 500 and exc.status != 401
                    ):
                        raise
                    continue
            if result["contentLength"] != size or result["contentSha1"] != sha1:
                raise PublicationError("storage did not confirm the uploaded content")
            return True
        raise PublicationError("content upload did not confirm an outcome")


class CloudflareWorkers:
    def __init__(self, token: str, account: str) -> None:
        if re.fullmatch(r"[0-9a-fA-F]{32}", account) is None:
            raise PublicationError("Cloudflare account identifier is invalid")
        self.api = "https://api.cloudflare.com/client/v4/accounts/" + account + "/workers/"
        self.headers = {"Authorization": "Bearer " + token}
        result = _json_request(
            "Worker hostname lookup", self.api + "subdomain", headers=self.headers
        )["result"]
        self.subdomain = result["subdomain"]
        if re.fullmatch(r"[a-z0-9-]+", self.subdomain) is None:
            raise PublicationError("Worker hostname metadata is invalid")

    def url(self, name: str) -> str:
        if re.fullmatch(r"[a-z][a-z0-9-]{0,62}", name) is None:
            raise PublicationError("Worker name is invalid")
        return f"https://{name}.{self.subdomain}.workers.dev"

    def _settings(self, name: str) -> dict[str, Any] | None:
        self.url(name)
        try:
            result = _json_request(
                "Worker ownership lookup",
                self.api + "scripts/" + name + "/settings",
                headers=self.headers,
            )["result"]
            if not isinstance(result, dict):
                raise PublicationError("Worker ownership metadata is invalid")
            return result
        except ServiceError as exc:
            if exc.status == 404:
                return None
            raise

    def deploy(self, name: str, module: bytes, origin: str, mount: str, generation: str) -> None:
        settings = self._settings(name)
        if settings is not None and OWNER_TAG not in settings.get("tags", []):
            raise PublicationError("configured Worker belongs to another deployment")
        metadata = canonical_bytes(
            {
                "main_module": "index.js",
                "compatibility_date": "2026-10-24",
                "bindings": [
                    {"type": "plain_text", "name": "CONTENT_BASE_URL", "text": origin},
                    {"type": "plain_text", "name": "SITE_MOUNT", "text": mount},
                    {"type": "plain_text", "name": "PUBLICATION_SHA256", "text": generation},
                ],
                "tags": [OWNER_TAG, generation],
            }
        )
        boundary = "riverhog-" + uuid.uuid4().hex
        body = (
            (
                "--"
                + boundary
                + '\r\nContent-Disposition: form-data; name="metadata"\r\n'
                + "Content-Type: application/json\r\n\r\n"
            ).encode()
            + metadata
            + b"\r\n"
            + (
                "--"
                + boundary
                + '\r\nContent-Disposition: form-data; name="index.js"; filename="index.js"\r\n'
                + "Content-Type: application/javascript+module\r\n\r\n"
            ).encode()
            + module
            + ("\r\n--" + boundary + "--\r\n").encode()
        )
        _json_request(
            "Worker deployment",
            self.api + "scripts/" + name,
            method="PUT",
            headers={**self.headers, "Content-Type": "multipart/form-data; boundary=" + boundary},
            body=body,
        )
        _json_request(
            "Worker endpoint activation",
            self.api + "scripts/" + name + "/subdomain",
            method="POST",
            headers=self.headers,
            body={"enabled": True, "previews_enabled": False},
        )

    def remove(self, name: str, generation: str) -> None:
        if "-verify-" not in name:
            raise PublicationError("only a verification Worker may be removed")
        settings = self._settings(name)
        if settings is None:
            return
        bindings = {item["name"]: item.get("text") for item in settings.get("bindings", [])}
        if (
            OWNER_TAG not in settings.get("tags", [])
            or bindings.get("PUBLICATION_SHA256") != generation
        ):
            raise PublicationError("verification Worker ownership differs")
        _json_request(
            "verification Worker removal",
            self.api + "scripts/" + name,
            method="DELETE",
            headers=self.headers,
        )


def _parallel(phase: str, items: list[str], operation: Callable[[str], bool], jobs: int) -> int:
    """Keep pending network work bounded while accounting for every object."""

    iterator = iter(items)
    successful = 0
    completed = 0
    interval = max(1, len(items) // 10)
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
        pending: set[concurrent.futures.Future[bool]] = set()
        for _ in range(jobs):
            if (item := next(iterator, None)) is not None:
                pending.add(executor.submit(operation, item))
        while pending:
            done, pending = concurrent.futures.wait(
                pending, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                successful += bool(future.result())
                completed += 1
                if completed == 1 or completed % interval == 0 or completed == len(items):
                    print(
                        json.dumps({"phase": phase, "completed": completed, "total": len(items)}),
                        flush=True,
                    )
                if (item := next(iterator, None)) is not None:
                    pending.add(executor.submit(operation, item))
    return successful


def _ready(url: str, digest: str, size: int, verify: Callable[[str, str, int], None]) -> None:
    deadline = time.monotonic() + 45
    for attempt in range(6):
        try:
            verify(url, digest, size)
            return
        except (PublicationError, OSError) as exc:
            if isinstance(exc, ServiceError) and (
                exc.status is None or (exc.status not in (404, 429) and exc.status < 500)
            ):
                raise
            if attempt == 5 or time.monotonic() >= deadline:
                raise
            time.sleep(min(2**attempt, 5))


def verification_name(name: str, generation: str) -> str:
    return name[:36].rstrip("-") + "-verify-" + generation[:16]


def publish(
    site: Site,
    storage: Storage,
    workers: Workers,
    name: str,
    canonical_url: str,
    *,
    fresh: Callable[[], None],
    verify: Callable[[str, str, int], None] = verify_download,
    jobs: int = 8,
    staging_only: bool = False,
) -> dict[str, Any]:
    canonical_url = public_base_url(canonical_url)
    if not 1 <= jobs <= 32:
        raise PublicationError("publication concurrency must be between 1 and 32")
    mount = urlsplit(canonical_url).path
    if workers.url(name) + mount != canonical_url:
        raise PublicationError("configured public URL does not belong to the selected Worker")
    module = site.worker_module()
    fresh()
    objects = sorted(site.blobs)
    sizes = {digest: size for digest, size, _ in site.entries.values()}
    uploaded = _parallel(
        "upload",
        objects,
        lambda digest: storage.upload(digest, site.blobs[digest], sizes[digest]),
        jobs,
    )
    selected: dict[str, str] = {}
    for path, (digest, _, _) in sorted(site.entries.items()):
        selected.setdefault(digest, path)
    candidate = verification_name(name, site.manifest_sha256)
    attempted = False
    started = time.monotonic()
    try:
        fresh()
        attempted = True
        print(json.dumps({"phase": "verification-worker"}), flush=True)
        workers.deploy(candidate, module, storage.base_url, mount, site.manifest_sha256)
        base = workers.url(candidate) + mount
        manifest_digest, manifest_size, _ = site.entries[SITE_FILENAME]
        print(json.dumps({"phase": "verification-readiness"}), flush=True)
        _ready(base + SITE_FILENAME, manifest_digest, manifest_size, verify)

        def verify_one(digest: str) -> bool:
            verify(base + quote(selected[digest], safe="/"), digest, sizes[digest])
            return True

        verified = _parallel("verify", objects, verify_one, jobs)
        if verified != len(objects):
            raise PublicationError("publication content verification is incomplete")
        fresh()
        if not staging_only:
            print(json.dumps({"phase": "promotion"}), flush=True)
            workers.deploy(name, module, storage.base_url, mount, site.manifest_sha256)
            digest, size, _ = site.entries[SITE_FILENAME]
            _ready(canonical_url + SITE_FILENAME, digest, size, verify)
    finally:
        if attempted:
            print(json.dumps({"phase": "verification-cleanup"}), flush=True)
            workers.remove(candidate, site.manifest_sha256)
    return {
        "format": "riverhog-documentation-publication/v1",
        "site_manifest_sha256": site.manifest_sha256,
        "worker_module_sha256": hashlib.sha256(module).hexdigest(),
        "content_origin_sha256": hashlib.sha256(storage.base_url.encode()).hexdigest(),
        "file_count": len(site.entries),
        "unique_objects": len(objects),
        "uploaded_objects": uploaded,
        "existing_objects": len(objects) - uploaded,
        "verified_objects": verified,
        "staging_only": staging_only,
        "verification_and_promotion_seconds": time.monotonic() - started,
    }


def _credential(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise PublicationError("missing publishing credential: " + name)
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    relay = subparsers.add_parser("relay")
    relay.add_argument("--site", type=Path, required=True)
    relay.add_argument("--output", type=Path, required=True)
    relay.add_argument("--public-url", required=True)
    relay.add_argument("--budget", type=int, default=PAGES_LIMIT)
    deploy = subparsers.add_parser("publish")
    deploy.add_argument("--site", type=Path, required=True)
    deploy.add_argument("--public-url", required=True)
    deploy.add_argument("--worker", required=True)
    deploy.add_argument("--prefix", default="documentation/objects/")
    deploy.add_argument("--jobs", type=int, default=8)
    deploy.add_argument("--receipt", type=Path, required=True)
    deploy.add_argument("--repository", default="nashspence/riverhog")
    deploy.add_argument("--staging-only", action="store_true")
    cleanup = subparsers.add_parser("cleanup")
    cleanup.add_argument("--site", type=Path, required=True)
    cleanup.add_argument("--worker", required=True)
    args = parser.parse_args()
    try:
        if args.operation == "relay":
            result = build_relay(args.site, args.output, args.public_url, budget=args.budget)
            print(canonical_bytes({key: result[key] for key in ("format", "site_bytes")}).decode())
        elif args.operation == "cleanup":
            generation = file_sha256(args.site / SITE_FILENAME)
            workers = CloudflareWorkers(
                _credential("CLOUDFLARE_API_TOKEN"), _credential("CLOUDFLARE_ACCOUNT_ID")
            )
            workers.remove(verification_name(args.worker, generation), generation)
            print(json.dumps({"phase": "verification-cleanup-complete"}))
        else:
            site = Site.read(args.site)
            snapshot = site.manifest.get("snapshot")
            if snapshot is None and not args.staging_only:
                raise PublicationError("production publication requires a bound main snapshot")

            def fresh() -> None:
                if snapshot is not None:
                    require_fresh_snapshot(snapshot, GitHubPublication(args.repository).snapshot())

            result = publish(
                site,
                B2Storage(
                    _credential("B2_APPLICATION_KEY_ID"),
                    _credential("B2_APPLICATION_KEY"),
                    _credential("B2_BUCKET_ID"),
                    args.prefix,
                ),
                CloudflareWorkers(
                    _credential("CLOUDFLARE_API_TOKEN"), _credential("CLOUDFLARE_ACCOUNT_ID")
                ),
                args.worker,
                args.public_url,
                fresh=fresh,
                jobs=args.jobs,
                staging_only=args.staging_only,
            )
            args.receipt.parent.mkdir(parents=True, exist_ok=True)
            args.receipt.write_bytes(canonical_bytes(result))
            print(canonical_bytes(result).decode())
    except Exception as exc:
        detail = (
            str(exc)
            if isinstance(exc, (PublicationError, ContractAtlasError))
            else type(exc).__name__
        )
        parser.exit(2, f"Documentation publication failed: {detail}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
