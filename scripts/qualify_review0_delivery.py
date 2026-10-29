"""Drive the two independent Review0 and generic delivery recipes in Compose.

The smoke script runs this source inside the Stove0 API container so both
published HTTP authorities are exercised. It is not a product runtime module.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

from riverhog_client import ApiClient
from riverhog_protocol import canonical_json_sha256
from riverhog_protocol.errors import RiverhogError
from stove0_protocol import (
    CollectionRootIdentityRef,
    EvaluationDefinition,
    EvaluationDefinitionPayload,
    EvaluationMatrix,
    EvaluationMatrixPayload,
    EvaluationVariant,
    RecipeIdentityRef,
)

STOVE0 = "http://127.0.0.1:8080"
TOKEN = "stove0-compose-smoke-token"


def stove(
    path: str,
    *,
    payload: object | None = None,
    method: str | None = None,
    timeout: float = 45,
    maximum_response_octets: int | None = None,
) -> dict[str, object]:
    request = urllib.request.Request(
        STOVE0 + path,
        data=(json.dumps(payload, separators=(",", ":")).encode() if payload is not None else None),
        method=method or ("POST" if payload is not None else "GET"),
        headers={
            "Authorization": "Bearer " + TOKEN,
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if maximum_response_octets is None:
                return json.load(response)
            content = response.read(maximum_response_octets + 1)
            if len(content) > maximum_response_octets:
                raise ValueError("qualification diagnostic response exceeds its byte budget")
            return json.loads(content)
    except urllib.error.HTTPError as exc:
        if maximum_response_octets is not None:
            raise
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Stove0 {request.method} {path}: HTTP {exc.code}: {detail}") from exc


def riverhog() -> ApiClient:
    token = open("/run/secrets/stove0_riverhog_token", encoding="utf-8").read().strip()
    return ApiClient("http://app:8000", token, allow_insecure_http=True)


def _diagnostic_token(value: object) -> str | None:
    return (
        value
        if isinstance(value, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9_./:-]{0,159}", value)
        else None
    )


def _diagnostic_id(value: object) -> str | None:
    return value if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) else None


def _failure_code(value: object) -> str | None:
    return _diagnostic_token(value.get("code")) if isinstance(value, dict) else None


def _report_failure(
    stage: str,
    *,
    evaluation: dict[str, object] | None = None,
    work: dict[str, object] | None = None,
    error: Exception | None = None,
) -> dict[str, object]:
    """Expose bounded control state without payloads, config or failure messages.

    At most four work reads follow exact work identities, each with a five-second
    timeout and 128 KiB response admission. Diagnostic failure cannot turn the
    original failed qualification into success. GitHub retains the annotation
    through its structured check API as well as ordinary command output.
    """
    result: dict[str, object] = {
        "format": "riverhog-review-qualification-diagnostic/v1",
        "stage": stage,
    }
    if error is not None:
        result["error_type"] = _diagnostic_token(type(error).__name__)
        cause = error if isinstance(error, urllib.error.HTTPError) else error.__cause__
        if isinstance(cause, urllib.error.HTTPError):
            result["http_status"] = cause.code
        frames = []
        trace = error.__traceback__
        while trace is not None and len(frames) < 8:
            frames.append(
                {
                    "function": _diagnostic_token(trace.tb_frame.f_code.co_name),
                    "line": trace.tb_lineno,
                }
            )
            trace = trace.tb_next
        result["frames"] = frames
    pending = []
    if evaluation is not None:
        children = evaluation.get("children") or []
        result["evaluation"] = {
            "evaluation_id": _diagnostic_id(evaluation.get("evaluation_id")),
            "phase": _diagnostic_token(evaluation.get("phase")),
            "child_count": len(children),
            "children": [
                {
                    "work_id": _diagnostic_id(child.get("work_id")),
                    "state": _diagnostic_token(child.get("state")),
                }
                for child in children[:1]
            ],
        }
        pending.extend(child.get("work_id") for child in children[:1])
    if work is not None:
        pending.append(work.get("work_id"))
    seen = set()
    observed = []
    while pending and len(seen) < 4:
        work_id = _diagnostic_id(pending.pop(0))
        if work_id is None or work_id in seen:
            continue
        seen.add(work_id)
        try:
            current = (
                work
                if work is not None and work.get("work_id") == work_id
                else stove(f"/v1/work/{work_id}", timeout=5, maximum_response_octets=128 * 1024)
            )
        except (OSError, ValueError, RuntimeError) as error:
            observed.append(
                {
                    "work_id": work_id,
                    "diagnostic_error": type(error).__name__,
                    "http_status": error.code
                    if isinstance(error, urllib.error.HTTPError)
                    else None,
                }
            )
            continue
        status = current.get("target_status") or {}
        observed.append(
            {
                "work_id": work_id,
                "phase": _diagnostic_token(current.get("phase")),
                "failure_code": _failure_code(current.get("failure")),
                "inapplicable_code": _failure_code(current.get("inapplicable")),
                "target_state": _diagnostic_token(status.get("state")),
                "target_failure_code": _failure_code(status.get("failure")),
                "target_inapplicable_code": _failure_code(status.get("inapplicable")),
            }
        )
        acceptance = current.get("preview_acceptance") or {}
        pending.extend(plan.get("work_id") for plan in acceptance.get("target_plans", [])[:4])
    result["works"] = observed
    result["truncated"] = bool(pending)
    encoded = json.dumps(result, separators=(",", ":"), sort_keys=True)
    escaped = encoded.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print("::error title=Review0 qualification::" + escaped, file=sys.stderr)
    return result


def await_evaluation(evaluation_id: str) -> dict[str, object]:
    deadline = time.monotonic() + 600
    last: dict[str, object] = {}
    while time.monotonic() < deadline:
        last = stove(f"/v1/evaluations/{evaluation_id}")
        children = last["children"]
        assert isinstance(children, list) and len(children) == 1
        child = children[0]
        if child["state"] == "complete":
            assert last["phase"] == "complete", last
            return child
        if child["state"] in {"failed", "canceled", "inapplicable"}:
            raise AssertionError(_report_failure("review-evaluation", evaluation=last))
        time.sleep(0.5)
    raise TimeoutError(_report_failure("review-evaluation", evaluation=last))


def review() -> None:
    receipt = json.loads(os.environ["REVIEW_INPUT_RECEIPT"])
    input_ref = CollectionRootIdentityRef.model_validate(
        {
            "collection_id": receipt["collection_id"],
            "archive_root_sha256": receipt["archive_root_sha256"],
            "artifact_set_identity": receipt["artifact_set_identity"],
        }
    )
    with riverhog() as client:
        inventory = client.get_portable_collection_inventory(
            int(input_ref.collection_id), limit=100
        )
        assert inventory.complete, inventory
        assert len(inventory.artifacts) == 1, inventory
        source = inventory.artifacts[0]
        artifact_id = (
            "a-"
            + canonical_json_sha256(
                {"collection_id": input_ref.collection_id, "artifact_id": str(source.artifact_id)}
            )[:32]
        )
    sample_plan = {
        "format": "review0-sample-plan/v1",
        "selection_method": "evenly-spaced/v1",
        "samples_per_artifact": 1,
        "window_duration_ms": 1000,
        "windows": [{"artifact_id": artifact_id, "start_ms": 500, "duration_ms": 1000}],
    }
    sample_plan["sample_plan_sha256"] = canonical_json_sha256(sample_plan)
    recipe = stove("/v1/recipes/stove0.review/v1")
    definition = EvaluationDefinition.seal(
        EvaluationDefinitionPayload(
            purpose="trial",
            recipe=RecipeIdentityRef(id="stove0.review/v1", revision="1", sha256=recipe["sha256"]),
            inputs=(input_ref,),
            common_intent={"review_sample_plan": sample_plan},
            matrix=EvaluationMatrix.seal(
                EvaluationMatrixPayload(
                    variants=(
                        EvaluationVariant(
                            id="opus-96",
                            parameters={
                                "review_variant": {
                                    "id": "opus-96",
                                    "portable_intent": {
                                        "codec": "opus",
                                        "container": "opus",
                                        "bitrate_kbps": 96,
                                    },
                                    "target_options": {},
                                }
                            },
                        ),
                    )
                )
            ),
        )
    )
    created = stove("/v1/evaluations", payload=definition.model_dump(mode="json"))
    child = await_evaluation(created["evaluation_id"])
    output = child["output"]
    assert isinstance(output, dict)
    output_id = int(output["collection_id"])
    with riverhog() as client:
        collection = client.get_collection(output_id)
        assert collection["archive_root_sha256"] == output["archive_root_sha256"]
        assert collection["artifact_set_identity"] == output["artifact_set_identity"]
        tags = client.list_collection_tags(
            output_id,
            revision=collection["tag_revision"],
            tag_set_identity=collection["tag_set_identity"],
            page_size=100,
        )
        assert tags["tags"] == ["review0/output"], tags
        copies = client.list_collection_archive_copies(output_id)
        assert [(row["store"], row["state"]) for row in copies["copies"]] == [
            ("review-local", "uploaded")
        ], copies
        derivation = client.get_collection_derivation(output_id)
        assert derivation.document_sha256 == output["derivation_sha256"]
        assert client.get_collection(int(input_ref.collection_id))["id"] == str(
            input_ref.collection_id
        )
    print(json.dumps({"collection_id": output_id, "source_collection_id": input_ref.collection_id}))


def delivery() -> None:
    output_id = int(os.environ["REVIEW_OUTPUT_COLLECTION_ID"])
    source_id = int(os.environ["REVIEW_SOURCE_COLLECTION_ID"])
    policy = stove("/v1/admission-policies/review0-output-delivery:backfill", method="POST")
    assert policy["baseline_mode"] == "backfill", policy
    deadline = time.monotonic() + 600
    admission: dict[str, object] | None = None
    while time.monotonic() < deadline:
        page = stove("/v1/admissions?page_size=100&sort=admission_id&order=asc")
        matches = [
            item
            for item in page["admissions"]
            if int(item["intent"]["collection"]["collection_id"]) == output_id
        ]
        assert len(matches) <= 1, matches
        if matches and matches[0]["state"] == "work_bound":
            admission = matches[0]
            break
        time.sleep(0.5)
    if admission is None:
        raise AssertionError(_report_failure("delivery-admission"))
    assert admission["intent"]["policy_id"] == "review0-output-delivery", admission
    work_id = admission["work_id"]
    last: dict[str, object] = {}
    while time.monotonic() < deadline:
        last = stove(f"/v1/work/{work_id}")
        if last["phase"] == "complete":
            break
        if last["phase"] in {"failed", "canceled", "inapplicable", "abandon_pending"}:
            raise AssertionError(_report_failure("delivery-work", work=last))
        time.sleep(0.5)
    else:
        raise TimeoutError(_report_failure("delivery-work", work=last))
    assert last["work"]["recipe"]["id"] == "stove0.rclone-delivery/v1", last
    plans = last["preview_acceptance"]["target_plans"]
    assert len(plans) == 1, plans
    child = stove(f"/v1/work/{plans[0]['work_id']}")
    assert child["phase"] == "complete", child
    assert child["work"]["inputs"][0]["collection_id"] == str(output_id), child
    receipt = child["target_status"]["effect_receipt"]
    result = receipt["result"]
    assert result["format"] == "stove0-rclone-delivery-receipt/v1", result
    assert result["verification"] == "rclone-download-check-and-manifest-readback/v1"
    with riverhog() as client:
        claim = client.get_processing_claim(child["claim"]["claim_id"])
        assert claim.effect_settlement_sha256 is not None, claim
        try:
            client.get_collection(output_id)
        except RiverhogError as exc:
            assert exc.code == "not_found", exc
        else:
            raise AssertionError("retired Review0 output remains in Riverhog")
        assert client.get_collection(source_id)["id"] == str(source_id)
    print(
        json.dumps(
            {
                "collection_id": output_id,
                "delivery_id": result["delivery_id"],
                "effect_settlement_sha256": claim.effect_settlement_sha256,
                "manifest_sha256": result["manifest_sha256"],
                "source_collection_id": source_id,
                "work_id": work_id,
            }
        )
    )


def run(mode: str) -> None:
    operations = {"review": review, "delivery": delivery}
    if mode not in operations:
        raise SystemExit("expected review or delivery")
    try:
        operations[mode]()
    except Exception as error:
        reported = (
            len(error.args) == 1
            and isinstance(error.args[0], dict)
            and error.args[0].get("format") == "riverhog-review-qualification-diagnostic/v1"
        )
        if not reported:
            _report_failure(mode, error=error)
        raise


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("expected review or delivery")
    run(sys.argv[1])
