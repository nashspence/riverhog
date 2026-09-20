(
    'Measure completed stored payload through the adapter; no nominal capacity '
    'baseline.'
)
from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any
from uuid import uuid4

from riverhog_storage_adapter_protocol import (
    DeletePrefixRequest, ObjectLocator, ObjectReadRequest, WriteCompleteRequest,
    WriteSegmentListRequest, WriteStartRequest, validate_completed_write_response,
)
from riverhog_storage_adapter_support import StorageAdapterClient
from scripts import performance_objectives as performance

_MIB = 1024 * 1024


def _chunks(byte_count: int, *, value: int) -> Iterator[bytes]:
    # Preserves the existing fixture. Its compressibility is part of the workload;
    # do not claim it represents incompressible/encrypted payload without a new fixture.
    block = bytes([value]) * _MIB
    remaining = byte_count
    while remaining:
        chunk = block[:min(remaining, len(block))]
        yield chunk
        remaining -= len(chunk)


def run(*, base_url: str, token_file: Path, payload_bytes: int,
        context: Mapping[str, Any] | None = None,
        reference: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if type(payload_bytes) is not int or payload_bytes < 1:
        raise ValueError("payload bytes must be positive")
    if context is not None:
        context = performance.validate_context(context)
        if context["byte_domain"] != "stored-payload" or context["concurrency"] != 1:
            raise performance.PerformanceError(
                "probe requires stored payload and sequential concurrency one"
            )
        if context["cache_state"] != "read-after-write":
            raise performance.PerformanceError("probe context must declare read-after-write")
        if context["completion_boundary"] != "adapter-complete-and-verified-readback":
            raise performance.PerformanceError("incorrect adapter completion boundary")
    if reference is not None and context is None:
        raise performance.PerformanceError("a reference requires explicit comparison context")
    if reference is not None and reference.get("format") != "riverhog-storage-adapter-goodput/v2":
        raise performance.PerformanceError("incompatible adapter reference format")
    start_source = performance.source_identity()
    definition_start = performance.definition_sha256()
    client = StorageAdapterClient.from_token_file(base_url, token_file=token_file,
                                                 allow_insecure_http=True, timeout=300)
    prefix = f"goodput/{uuid4().hex}"
    object_path = f"{prefix}/payload.bin"
    try:
        descriptor = client.descriptor()
        segment_bytes = descriptor.maximum_segment_bytes
        if type(segment_bytes) is not int or segment_bytes <= 0:
            raise performance.PerformanceError("invalid admitted segment size")
        request = WriteStartRequest(object_path=object_path, expected_bytes=payload_bytes,
                                    content_type="application/octet-stream",
                                    required_identity_assertions={
                                        "riverhog-conformance": "goodput/v1"
                                    },
                                    placement="immediate")
        expected = hashlib.sha256()
        expected_offset, expected_number = 0, 1
        while expected_offset < payload_bytes:
            count = min(segment_bytes, payload_bytes - expected_offset)
            for chunk in _chunks(count, value=expected_number % 251):
                expected.update(chunk)
            expected_offset += count
            expected_number += 1
        upload_started = time.perf_counter()
        session = client.begin_write(request)
        admitted = time.perf_counter()
        offset, number = 0, 1
        while offset < payload_bytes:
            count = min(segment_bytes, payload_bytes - offset)
            client.write_segment(session=session, number=number, stored_bytes=count,
                                 content=_chunks(count, value=number % 251))
            offset += count
            number += 1
        written = time.perf_counter()
        after_number = 0
        traversal_token: str | None = None
        accepted_segments = 0
        completion_authority = None
        while True:
            page = client.list_segments(WriteSegmentListRequest(session=session,
                    after_number=after_number,
                                                               traversal_token=traversal_token))
            accepted_segments += len(page.segments)
            if page.next_after_number is None:
                completion_authority = page.completion
                break
            if page.next_after_number <= after_number:
                raise RuntimeError("storage-adapter segment traversal did not advance")
            after_number = page.next_after_number
            traversal_token = page.traversal_token
        if completion_authority is None or accepted_segments != number - 1:
            raise RuntimeError("storage-adapter goodput traversal is incomplete")
        completion = WriteCompleteRequest(session=session, completion=completion_authority,
                                          expected_bytes=payload_bytes,
                                          expected_content_type=request.content_type,
                                          required_identity_assertions=(
                                              request.required_identity_assertions
                                          ),
                                          expected_placement=request.placement)
        completed = client.complete_write(completion)
        validate_completed_write_response(completion, completed)
        completed_at = time.perf_counter()
        observed = hashlib.sha256()
        observed_bytes = 0
        read_started = time.perf_counter()
        with client.read_object(ObjectReadRequest(object=ObjectLocator(
                                                  object_path=completed.object_path,
                                                                       revision=completed.revision),
                                                  expected_bytes=payload_bytes)) as stream:
            for chunk in stream.content:
                observed.update(chunk)
                observed_bytes += len(chunk)
        read_seconds = time.perf_counter() - read_started
        if observed_bytes != payload_bytes or observed.digest() != expected.digest():
            raise RuntimeError("storage-adapter goodput probe changed the payload")
        finish_source = performance.source_identity()
        if definition_start != performance.definition_sha256():
            raise performance.PerformanceError("performance definitions changed during probe")
        # Actual segmentation affects the byte fixture and work, so bind it in addition
        # to the operator's workload/environment/path fingerprint declarations.
        scenario = f"storage-adapter-sequential-segment-{segment_bytes}"
        samples = {}
        evaluations = {}
        for direction, seconds in (("upload", completed_at - upload_started), ("read",
                read_seconds)):
            measured = performance.sample(identity=f"storage-{direction}-goodput",
                scenario=scenario,
                                          workload="synthetic-one-object-read-after-write",
                completed_bytes=payload_bytes,
                                          elapsed_seconds=seconds, verified=True, context=context,
                                          source_start=start_source,
                source_finish=finish_source, run_id=str(uuid4()))
            samples[direction] = measured
            baseline = reference["samples"][direction] if reference is not None else None
            evaluations[direction] = performance.evaluate_sample(measured, baseline)
        return {"format": "riverhog-storage-adapter-goodput/v2", "samples": samples,
                "evaluations": evaluations, "admission_seconds": admitted - upload_started,
                "completion_seconds": completed_at - written, "write_seconds": written - admitted,
                "payload_bytes": payload_bytes, "segment_bytes": segment_bytes}
    finally:
        try:
            client.delete_prefix(DeletePrefixRequest(object_prefix=f"{prefix}/"))
        finally:
            client.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="storage-adapter-goodput-probe", description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--payload-bytes", type=int, default=128 * _MIB,
                        help="fixture size, not a throughput or capacity claim")
    parser.add_argument("--context", type=Path)
    parser.add_argument("--reference", type=Path, help="matching measured v2 probe result")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = run(base_url=args.base_url, token_file=args.token_file,
        payload_bytes=args.payload_bytes,
                 context=performance.read_json(args.context) if args.context else None,
                 reference=performance.read_json(args.reference) if args.reference else None)
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

__all__ = ["main", "run"]
