"""Authenticate baseline custody through the existing product/preparation rails."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, cast

from .github_publication import GitHubPublication
from .model import ContractAtlasError, canonical_bytes, canonical_sha256
from .publication import file_sha256, unpack_release_contract, verify_published_candidate
from .workflow_evidence import collect_workflow_artifact

PROVENANCE_FILE = "baseline-provenance.json"


def capture_published(
    remote: GitHubPublication, selected: dict[str, Any], destination: Path
) -> Path:
    loaded = remote.load_product(selected, destination)
    path = cast(Path, loaded["root"])
    (destination / PROVENANCE_FILE).write_bytes(
        canonical_bytes({"kind": "published-product", "tag": loaded["tag"]})
    )
    return path


def resolve(
    directory: Path | None, *, initial: bool, remote: GitHubPublication | None = None
) -> tuple[Any, Any]:
    if directory is None:
        if not initial:
            raise ContractAtlasError("select a verified baseline or explicit initial review")
        return None, None
    if initial:
        raise ContractAtlasError("baseline and initial review are mutually exclusive")
    # This selector locates evidence. Its own assertions/digests are never trusted.
    try:
        selector = json.loads((directory.parent / PROVENANCE_FILE).read_bytes())
    except (OSError, ValueError) as exc:
        raise ContractAtlasError("requested baseline lacks upstream custody selection") from exc
    remote = remote or GitHubPublication("nashspence/riverhog")
    local = verify_published_candidate(directory)
    with tempfile.TemporaryDirectory(prefix="riverhog-documentation-baseline-") as temporary:
        scratch = Path(temporary)
        if selector.get("kind") == "published-product":
            matches = [
                row for row in remote.snapshot()["products"] if row["tag"] == selector.get("tag")
            ]
            if len(matches) != 1:
                raise ContractAtlasError("requested published baseline is unavailable")
            loaded = remote.load_product(matches[0], scratch / "product")
            verified = loaded["root"]
            custody = {
                "kind": "published-product",
                "manifest_sha256": loaded["release_manifest_sha256"],
                "verification": {
                    "release_id": loaded["release_id"],
                    "attestation_sha256": loaded["attestation_sha256"],
                    "assets": loaded["assets"],
                },
            }
        elif selector.get("kind") == "selected-prepared-candidate":
            run_id = int(selector["run_id"])
            # The Actions API identifies the original workflow authority. The
            # collector authenticates its repository, main ref, workflow and
            # attempt; a later main commit must not invalidate archived custody.
            original_run = remote.api(f"actions/runs/{run_id}")
            proof = collect_workflow_artifact(
                remote,
                run_id,
                "preparation",
                local["source_sha"],
                local["documentation"]["tag"][1:],
                scratch / "prepared",
                authority_sha=original_run["head_sha"],
            )
            import release

            evidence = scratch / "prepared/evidence"
            release.verify_release_evidence(
                release.ROOT,
                evidence,
                public_key=scratch / "prepared/evidence.preparation.pub",
                archived_documentation=True,
            )
            manifest = json.loads((evidence / "release-manifest.json").read_bytes())
            verified = scratch / "candidate"
            unpack_release_contract(evidence, manifest["contract"], verified)
            custody = {
                "kind": "selected-prepared-candidate",
                "manifest_sha256": file_sha256(evidence / "release-manifest.json"),
                "verification": proof,
            }
        else:
            raise ContractAtlasError("unsupported baseline custody; a digest alone is insufficient")
        authenticated = verify_published_candidate(verified)
        if canonical_sha256(authenticated) != canonical_sha256(local):
            raise ContractAtlasError("requested baseline differs from authenticated upstream bytes")
        record = json.loads((verified / "documentation-audit.json").read_bytes())
        if (
            record.get("format") != "riverhog-documentation-audit/v1"
            or record.get("stage") != "prepared"
            or record.get("state") not in {"PASS", "REVIEW"}
        ):
            raise ContractAtlasError("baseline has no usable archived prepared audit")
        custody.update(
            snapshot_sha256=canonical_sha256(record["current"]),
            source_sha=authenticated["source_sha"],
        )
        # Preserve the archived policy and result; never run today's audit on the old release.
        return record["current"], custody
