"""Drive the two independent Review0 and generic delivery recipes in Compose.

The smoke script runs this source inside the Stove0 API container so both
published HTTP authorities are exercised. It is not a product runtime module.
"""

from __future__ import annotations

import json
import os
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
        with urllib.request.urlopen(request, timeout=45) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Stove0 {request.method} {path}: HTTP {exc.code}: {detail}") from exc


def riverhog() -> ApiClient:
    token = open("/run/secrets/stove0_riverhog_token", encoding="utf-8").read().strip()
    return ApiClient("http://app:8000", token, allow_insecure_http=True)


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
            raise AssertionError(last)
        time.sleep(0.5)
    raise TimeoutError(last)


def review() -> None:
    receipt = json.loads(os.environ["REVIEW_INPUT_RECEIPT"])
    input_ref = CollectionRootIdentityRef.model_validate(
        {
            "collection_id": receipt["collection_id"],
            "archive_root_sha256": receipt["archive_root_sha256"],
            "content_identity": receipt["content_identity"],
        }
    )
    with riverhog() as client:
        inventory = client.get_portable_collection_inventory(
            int(input_ref.collection_id), limit=100
        )
        assert inventory.complete, inventory
        source = next(file for file in inventory.files if file.path == "review-input.wav")
        artifact_id = (
            "a-"
            + canonical_json_sha256(
                {"collection_id": input_ref.collection_id, "path": source.path}
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
        assert collection["content_identity"] == output["content_identity"]
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
    assert admission is not None, "derived Review0 collection was not admitted"
    assert admission["intent"]["policy_id"] == "review0-output-delivery", admission
    work_id = admission["work_id"]
    last: dict[str, object] = {}
    while time.monotonic() < deadline:
        last = stove(f"/v1/work/{work_id}")
        if last["phase"] == "complete":
            break
        if last["phase"] in {"failed", "canceled", "inapplicable", "abandon_pending"}:
            raise AssertionError(last)
        time.sleep(0.5)
    else:
        raise TimeoutError(last)
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


if __name__ == "__main__":
    if sys.argv[1:] == ["review"]:
        review()
    elif sys.argv[1:] == ["delivery"]:
        delivery()
    else:
        raise SystemExit("expected review or delivery")
