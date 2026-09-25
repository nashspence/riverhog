"""Measure the public storage-adapter path with bounded synthetic content."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path

from riverhog_storage_adapter_protocol import (
    DeletePrefixRequest,
    ObjectLocator,
    ObjectReadRequest,
    WriteCompleteRequest,
    WriteSegmentListRequest,
    WriteStartRequest,
    validate_completed_write_response,
)
from riverhog_storage_adapter_support import StorageAdapterClient

from scripts import performance_objectives as performance

_MIB = 1024 * 1024


def _chunks(byte_count: int, *, value: int) -> Iterator[bytes]:
    block = bytes([value]) * _MIB
    remaining = byte_count
    while remaining:
        chunk = block[: min(remaining, len(block))]
        yield chunk
        remaining -= len(chunk)


def run(
    *,
    base_url: str,
    token_file: Path,
    payload_bytes: int,
    context: Mapping[str, object] | None = None,
    reference: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if type(payload_bytes) is not int or payload_bytes < 1:
        raise ValueError("payload bytes must be positive")
    if context is not None:
        context = performance.validate_context(context)
        if (
            context["byte_domain"] != "stored-payload"
            or context["cache_state"] != "read-after-write"
            or context["concurrency"] != 1
            or context["completion_boundary"] != "adapter-complete-and-verified-readback"
        ):
            raise performance.PerformanceError("storage probe comparison context is incompatible")
    if reference is not None:
        if context is None:
            raise performance.PerformanceError("a measured reference requires comparison context")
        if reference.get("format") != "riverhog-storage-adapter-goodput/v2" or not isinstance(
            reference.get("samples"), Mapping
        ):
            raise performance.PerformanceError("storage probe reference format is incompatible")
    client = StorageAdapterClient.from_token_file(
        base_url,
        token_file=token_file,
        allow_insecure_http=True,
        timeout=300,
    )
    prefix = f"goodput/{uuid.uuid4().hex}"
    object_path = f"{prefix}/payload.bin"
    try:
        descriptor = client.descriptor()
        segment_bytes = descriptor.maximum_segment_bytes
        if type(segment_bytes) is not int or segment_bytes < 1:
            raise performance.PerformanceError("adapter segment maximum is invalid")
        request = WriteStartRequest(
            object_path=object_path,
            expected_bytes=payload_bytes,
            content_type="application/octet-stream",
            required_identity_assertions={"riverhog-conformance": "goodput/v1"},
            placement_policy="immediate_default",
        )
        expected = hashlib.sha256()
        expected_offset = 0
        expected_number = 1
        while expected_offset < payload_bytes:
            current_bytes = min(segment_bytes, payload_bytes - expected_offset)
            for chunk in _chunks(current_bytes, value=expected_number % 251):
                expected.update(chunk)
            expected_offset += current_bytes
            expected_number += 1

        upload_started = time.perf_counter()
        session = client.begin_write(request)
        admitted = time.perf_counter()
        offset = 0
        number = 1
        while offset < payload_bytes:
            current_bytes = min(segment_bytes, payload_bytes - offset)
            value = number % 251
            client.write_segment(
                session=session,
                number=number,
                stored_bytes=current_bytes,
                content=_chunks(current_bytes, value=value),
            )
            offset += current_bytes
            number += 1
        written = time.perf_counter()
        after_number = 0
        traversal_token: str | None = None
        accepted_segments = 0
        completion_precondition = None
        while True:
            page = client.list_segments(
                WriteSegmentListRequest(
                    session=session,
                    after_number=after_number,
                    traversal_token=traversal_token,
                )
            )
            accepted_segments += len(page.segments)
            if page.next_after_number is None:
                completion_precondition = page.completion
                break
            after_number = page.next_after_number
            traversal_token = page.traversal_token
        if completion_precondition is None or accepted_segments != number - 1:
            raise RuntimeError("storage-adapter goodput traversal is incomplete")
        completion = WriteCompleteRequest(
            session=session,
            completion=completion_precondition,
            expected_bytes=payload_bytes,
            expected_content_type=request.content_type,
            required_identity_assertions=request.required_identity_assertions,
            expected_placement_policy=request.placement_policy,
        )
        completed = client.complete_write(completion)
        validate_completed_write_response(completion, completed)
        completed_at = time.perf_counter()
        upload_seconds = completed_at - upload_started

        observed = hashlib.sha256()
        observed_bytes = 0
        read_started = time.perf_counter()
        with client.read_object(
            ObjectReadRequest(
                object=ObjectLocator(
                    object_path=completed.object_path,
                    revision=completed.revision,
                ),
                expected_bytes=payload_bytes,
            )
        ) as stream:
            for chunk in stream.content:
                observed.update(chunk)
                observed_bytes += len(chunk)
        read_seconds = time.perf_counter() - read_started
        if observed_bytes != payload_bytes or observed.digest() != expected.digest():
            raise RuntimeError("storage-adapter goodput probe changed the payload")

        upload_rate = payload_bytes / _MIB / upload_seconds
        read_rate = payload_bytes / _MIB / read_seconds
        samples: dict[str, dict[str, object]] = {}
        evaluations: dict[str, dict[str, object]] = {}
        reference_samples = reference["samples"] if reference is not None else None
        for direction, seconds in (("upload", upload_seconds), ("read", read_seconds)):
            measured = performance.sample(
                objective_id=f"storage-{direction}-goodput",
                scenario=f"storage-adapter-sequential-segment-{segment_bytes}",
                workload="synthetic-one-object-read-after-write",
                context=context,
                completed_bytes=payload_bytes,
                elapsed_seconds=seconds,
                completion_verified=True,
                run_id=str(uuid.uuid4()),
            )
            samples[direction] = measured
            prior = reference_samples.get(direction) if reference_samples is not None else None
            evaluations[direction] = performance.evaluate_sample(measured, prior)
        return {
            "format": "riverhog-storage-adapter-goodput/v2",
            "admission_seconds": round(admitted - upload_started, 6),
            "completion_seconds": round(completed_at - written, 6),
            "payload_bytes": payload_bytes,
            "segment_bytes": segment_bytes,
            "read_seconds": round(read_seconds, 6),
            "read_mib_per_second": read_rate,
            "upload_seconds": round(upload_seconds, 6),
            "upload_mib_per_second": upload_rate,
            "write_seconds": round(written - admitted, 6),
            "samples": samples,
            "evaluations": evaluations,
        }
    finally:
        try:
            client.delete_prefix(DeletePrefixRequest(object_prefix=f"{prefix}/"))
        finally:
            client.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="storage-adapter-goodput-probe")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--payload-bytes", type=int, default=128 * _MIB)
    parser.add_argument("--context", type=Path, help="safe measured-comparison context JSON")
    parser.add_argument("--reference", type=Path, help="matching measured v2 probe result")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    print(
        json.dumps(
            run(
                base_url=args.base_url,
                token_file=args.token_file,
                payload_bytes=args.payload_bytes,
                context=performance.read_json(args.context) if args.context else None,
                reference=performance.read_json(args.reference) if args.reference else None,
            ),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main", "run"]
