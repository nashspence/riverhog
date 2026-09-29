"""Small storage-neutral constructors. No path, host or native handle is required."""

from __future__ import annotations

import base64
import hashlib
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from riverhog_provenance_contracts import canonical_document, require_canonical_uuid_urn

from .constants import ASSIGNED_IDENTITY_POLICY, PACKAGE_NAME, PACKAGE_VERSION, PROFILE


def new_id() -> str:
    return f"urn:uuid:{uuid.uuid4()}"


def software_agent_id(name: str = PACKAGE_NAME, version: str = PACKAGE_VERSION) -> str:
    # Software identity, not source-host identity or source artifact identity.
    return (
        f"urn:uuid:{uuid.uuid5(uuid.NAMESPACE_URL, PROFILE + '/software/' + name + '/' + version)}"
    )


def utc_now() -> str:
    ns = time.time_ns()
    seconds, nanos = divmod(ns, 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{nanos:09d}Z"


def reference(object_id: str, object_type: str) -> dict[str, str]:
    require_canonical_uuid_urn(object_id)
    return {"scope": "local", "object_id": object_id, "object_type": object_type}


def evidence(agent_id: str, basis: str = "identity_assignment", **detail: Any) -> dict[str, Any]:
    require_canonical_uuid_urn(agent_id)
    return {"basis": basis, "asserted_by_agent_id": agent_id, **detail}


def assertion(
    record_type: str,
    agent_id: str,
    *,
    object_id: str | None = None,
    assertion_id: str | None = None,
    evidence_items: list[dict[str, Any]] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    value = {
        "assertion_id": assertion_id or new_id(),
        "id": object_id or new_id(),
        "type": record_type,
        "evidence": evidence_items or [evidence(agent_id)],
        **fields,
    }
    # Return an independent portable document, without silently coercing values.
    canonical_document(value)
    return value


def artifact(
    agent_id: str,
    *,
    object_id: str | None = None,
    continuity_policy_uri: str = ASSIGNED_IDENTITY_POLICY,
) -> dict[str, Any]:
    return assertion(
        "artifact", agent_id, object_id=object_id, continuity_policy_uri=continuity_policy_uri
    )


def byte_string(data: bytes) -> dict[str, str]:
    if type(data) is not bytes:
        raise TypeError("source bytes must be bytes")
    return {
        "encoding": "base64",
        "data": base64.b64encode(data).decode("ascii"),
        "byte_length": str(len(data)),
    }


def content_description(data: bytes) -> dict[str, Any]:
    return {
        "size_bytes": str(len(data)),
        "digests": [
            {"algorithm": "sha-256", "value": hashlib.sha256(data).hexdigest()},
        ],
    }


def fingerprint(content: dict[str, Any]) -> tuple[int, str]:
    return int(content["size_bytes"]), next(
        item["value"] for item in content["digests"] if item["algorithm"] == "sha-256"
    )
