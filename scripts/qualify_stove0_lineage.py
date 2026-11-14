"""Verify published processing lineage and report bounded source-owned failures."""

from __future__ import annotations

import json
import os
import sys
from collections import deque
from types import FunctionType

import httpx
from riverhog_client import ApiClient
from riverhog_protocol.errors import RiverhogError

from tests.support.qualification.artifact_readouts import root_bound_artifacts


def readouts(client, collection_id):
    result = {}
    for artifact, hint in root_bound_artifacts(client, collection_id):
        assert hint is not None, artifact
        name = "/".join(hint)
        assert name not in result, name
        result[name] = artifact.model_dump(mode="json")
    return result


def qualify() -> None:
    with ApiClient() as client:
        inputs = [client.get_collection(int(os.environ["INPUT_COLLECTION_ID"]))]
        outputs = [client.get_collection(int(os.environ["OUTPUT_COLLECTION_ID"]))]
        input_artifacts = readouts(client, inputs[0]["id"])
        output_artifacts = readouts(client, outputs[0]["id"])
        audio_count = int(os.environ["STOVE0_SMOKE_FILE_COUNT"])
        assert len(input_artifacts) == audio_count + 1
        expected_names = {
            name
            for ordinal in range(audio_count)
            for name in (f"smoke-{ordinal:04d}.opus", f"smoke-{ordinal:04d}.opus.xmp")
        } | {"smoke-0000.xmp"}
        assert set(output_artifacts) == expected_names
        source_xmp = input_artifacts["smoke-0000.xmp"]
        retained_xmp = output_artifacts["smoke-0000.xmp"]
        assert (retained_xmp["bytes"], retained_xmp["sha256"]) == (
            source_xmp["bytes"],
            source_xmp["sha256"],
        )
        elapsed_seconds = int(os.environ["STOVE0_SMOKE_ELAPSED_NS"]) / 1_000_000_000
        input_bytes = sum(int(row["bytes"]) for row in input_artifacts.values())
        derivation = client.get_collection_derivation(outputs[0]["id"])
        assert derivation["derivation"]["format"] == "riverhog-collection-derivation/v1"
        authority = derivation["derivation"]["input_set_sha256"]
        ordinal = 0
        roots = []
        while True:
            page = client.list_processing_claim_inputs(
                derivation["derivation"]["claim"]["id"],
                identity_sha256=authority,
                start_ordinal=ordinal,
            )
            assert page.identity.sha256 == authority
            roots.extend(page.inputs)
            if page.next_ordinal is None:
                break
            ordinal = page.next_ordinal
        assert [row.collection_id for row in roots] == [int(inputs[0]["id"])]
        print(
            json.dumps(
                {
                    "format": "stove0-final-image-scale/v1",
                    "qualification_lane": os.environ["STOVE0_SMOKE_PROCESSING_LANE"],
                    "recipe_id": os.environ["STOVE0_SMOKE_RECIPE_ID"],
                    "elapsed_seconds": elapsed_seconds,
                    "input_bytes": input_bytes,
                    "input_artifacts": len(input_artifacts),
                    "items_per_second": len(input_artifacts) / elapsed_seconds,
                    "measurement": "ftp-ingress-through-opus-derived-publication",
                    "mib_per_second": input_bytes / 1048576 / elapsed_seconds,
                    "output_bytes": sum(int(row["bytes"]) for row in output_artifacts.values()),
                    "output_artifacts": len(output_artifacts),
                },
                sort_keys=True,
            )
        )


def qualification_failure_annotation(error: Exception) -> None:
    """Report fixed failure classes and owned source frames, without payloads."""
    owned = {
        value.__code__
        for value in globals().values()
        if isinstance(value, FunctionType) and value.__globals__ is globals()
    }
    frames: deque[dict[str, object]] = deque(maxlen=3)
    trace = error.__traceback__
    while trace is not None:
        if trace.tb_frame.f_code in owned:
            frames.append({"function": trace.tb_frame.f_code.co_name, "line": trace.tb_lineno})
        trace = trace.tb_next
    diagnostic: dict[str, object] = {
        "format": "riverhog-processing-lineage-failure-diagnostic/v1",
        "failure_class": "unexpected",
        "source_frames": list(frames),
    }
    if isinstance(error, RiverhogError):
        status = error.observed_status
        if type(status) is int and 400 <= status <= 599:
            diagnostic.update(failure_class="http-status", http_status=status)
    else:
        for error_type, category in (
            (AssertionError, "assertion"),
            ((TimeoutError, httpx.TimeoutException), "timeout"),
            (httpx.TransportError, "transport"),
            (OSError, "filesystem"),
            (ValueError, "value"),
            (KeyError, "key"),
            (TypeError, "type"),
            (RuntimeError, "runtime"),
        ):
            if isinstance(error, error_type):
                diagnostic["failure_class"] = category
                break
    print(
        "::error title=Processing lineage qualification::"
        + json.dumps(diagnostic, sort_keys=True, separators=(",", ":")),
        file=sys.stderr,
        flush=True,
    )


def main() -> None:
    try:
        qualify()
    except Exception as error:
        try:
            qualification_failure_annotation(error)
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
