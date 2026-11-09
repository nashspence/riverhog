"""Original cancel response from bounded counts and admitted complete result width."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from riverhog_canonical_json import canonical_json_bytes, format_scalar, parse_scalar
from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session
from time_formats import utc_timestamp_now

from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import ArchiveCopyJobRecord as CopyJob
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadCopyIntentRecord as CopyIntent
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_models import CollectionUploadTagRecord as Tag
from riverhog_core.incremental_emission import VolumeEmissionCapacity, VolumeEmissionLimits
from riverhog_core.incremental_plan import parse_incremental_volume_planner_checkpoint
from riverhog_core.services.archive_upload_terminal_adjudication import (
    PendingReason,
    terminal_pending_reason,
)
from riverhog_core.services.archive_upload_terminal_control import (
    require_prepared_terminal_result,
    require_terminal_control_capacity,
    terminal_control_bytes,
)
from riverhog_core.services.archive_upload_terminal_observation import (
    ArchiveUploadTerminalObservation,
)
from riverhog_core.services.archive_upload_terminal_tags import load_terminal_tag_selector
from riverhog_core.services.catalog_retirement import metadata_row_bytes
from riverhog_core.services.upload_session_projection import upload_session_payload

_PREPARATION = "archive-upload-cancel-result-preparation/v1"
_RESULT = "archive-upload-cancel-result/v1"
_COUNTERS = ("tag_count", "archive_uploaded_bytes", "archive_uploaded_units", "archive_total_units")
_INTEGERS = (*_COUNTERS, "files_total", "bytes_total", "description_revision", "tag_revision")
_FIELDS = frozenset(
    (
        "collection_id",
        "created_at",
        "ingest_source",
        "description",
        "description_revision",
        "description_identity",
        "description_publication",
        "tag_revision",
        "tag_set_identity",
        "tag_publication",
        "tag_count",
        "provenance_identity",
        "delivery_context_id",
        "construction_identity_sha256",
        "completion_journal_id",
        "completion_receipt",
        "archive_store",
        "use_cache",
        "copy_to",
        "copy_intents",
        "encryption_format",
        "passphrase_id",
        "state",
        "custody_mode",
        "registration_constraints",
        "files_total",
        "bytes_total",
        "upload_state_expires_at",
        "custody",
        "orphaned_at",
        "latest_failure",
        "archive_phase",
        "archive_phase_updated_at",
        "archive_next_attempt_at",
        "archive_storage_prefix",
        "archive_uploaded_bytes",
        "archive_total_bytes",
        "archive_uploaded_units",
        "archive_total_units",
        "collection",
    )
)


def _encoded_payload(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    for name in _INTEGERS:
        value = result[name]
        if value is not None:
            result[name] = format_scalar("nonnegative", value)
    result["custody"] = {
        name: format_scalar("nonnegative", value) if type(value) is int else value
        for name, value in result["custody"].items()
    }
    return result


def decode_cancel_terminal_result(encoded: str) -> dict[str, Any]:
    """Recover the original public fields, without read authority or confirmation."""
    try:
        document = json.loads(encoded)
        if (
            not isinstance(document, dict)
            or set(document) != {"format", "payload"}
            or document["format"] != _RESULT
            or canonical_json_bytes(document).decode() != encoded
        ):
            raise ValueError("original cancel format")
        payload = document["payload"]
        if not isinstance(payload, dict) or set(payload) != _FIELDS:
            raise ValueError("original cancel fields")
        result = dict(payload)
        parse_scalar("sequence63", result["collection_id"])
        for name in _INTEGERS:
            if result[name] is None and name in {"description_revision", "tag_revision"}:
                continue
            result[name] = parse_scalar("nonnegative", result[name])
        custody = result["custody"]
        if custody == {"state": "complete"}:
            pass
        elif (
            isinstance(custody, dict)
            and set(custody) == {"state", "files", "bytes"}
            and custody["state"] == "pending"
        ):
            files = parse_scalar("nonnegative", custody["files"])
            octets = parse_scalar("nonnegative", custody["bytes"])
            if (
                files > result["files_total"]
                or octets > result["bytes_total"]
                or (files == result["files_total"] and octets == result["bytes_total"])
            ):
                raise ValueError("original cancel custody")
            result["custody"] = {"state": "pending", "files": files, "bytes": octets}
        else:
            raise ValueError("original cancel custody")
        if result["state"] != "canceled" or result["archive_phase"] != "canceled":
            raise ValueError("original cancel disposition")
        if any(
            result[name] is not None
            for name in (
                "provenance_identity",
                "upload_state_expires_at",
                "orphaned_at",
                "latest_failure",
                "archive_next_attempt_at",
                "archive_total_bytes",
                "collection",
            )
        ):
            raise ValueError("original cancel terminal fields")
        copies = result["copy_intents"]
        if not isinstance(copies, list):
            raise ValueError("original cancel copy fields")
        destinations: set[str] = set()
        for item in copies:
            if (
                not isinstance(item, dict)
                or set(item)
                != {"destination_store", "state", "failure_code", "job_state", "job_created"}
                or not isinstance(item["destination_store"], str)
                or item["state"] != "canceled"
                or (item["destination_store"] in destinations)
                or (item["job_created"] is not None and type(item["job_created"]) is not bool)
                or any(
                    item[name] is not None and not isinstance(item[name], str)
                    for name in ("failure_code", "job_state")
                )
            ):
                raise ValueError("original cancel copy fields")
            destinations.add(item["destination_store"])
        return result
    except (ValueError, TypeError, KeyError, RuntimeError) as error:
        raise Conflict("original cancel terminal result is invalid") from error


def _capacity(used: int, limits: VolumeEmissionLimits) -> None:
    if used > limits.metadata_bytes:
        raise VolumeEmissionCapacity(
            dimension="metadata_bytes", required=used, admitted=limits.metadata_bytes
        )


@dataclass(frozen=True, slots=True)
class CancelResultProjectionProgress:
    phase: str
    prepared: bool = False
    rows: int = 0
    copy_rows: int = 0
    metadata_bytes: int = 0
    pending_reason: PendingReason | None = None


class ArchiveUploadCancelResultProjection:
    """Collect exact counts, then encode the whole admitted response once.

    Tags and visible volume counters use durable indexed windows limited by
    input_transitions. The final complete metadata response has its own width:
    copy outcomes stream once into the original byte reservation, charging every
    selected row and encoded output byte. That output-width step has no hidden
    destination-count ceiling and does not rebuild a growing list per count
    window. Insufficient bytes explicitly refuse the transaction. Rejected or
    interrupted attempts retain their actual work and original progress.

    Every turn reloads current original terminal rights and drained resources.
    Preparation performs no provider work, copy-job mutation, disposal, cleanup
    adjudication, or confirmation. Normal public factories select activation.
    """

    def __init__(
        self, factory: SessionFactory, observation: ArchiveUploadTerminalObservation
    ) -> None:
        self._factory, self._observation = factory, observation

    def advance(
        self,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None],
        limits: VolumeEmissionLimits,
    ) -> CancelResultProjectionProgress:
        if not callable(guard):
            raise ValueError("original cancel projection requires current authority")
        with session_scope(self._factory) as session:
            observed = self._observation.observe(session, collection_id, operation_id, guard=guard)
            terminal = session.get(Terminal, operation_id)
            assert terminal is not None
            selector = load_terminal_tag_selector(session, operation_id)
            if terminal.kind != "cancel":
                raise Conflict("cancel result projection requires its original operation kind")
            if terminal.original_result_json is not None:
                decode_cancel_terminal_result(require_prepared_terminal_result(terminal, selector))
                used = terminal_control_bytes(terminal, selector)
                _capacity(used, limits)
                return CancelResultProjectionProgress(
                    terminal.phase, prepared=True, metadata_bytes=used
                )
            if terminal.phase != "draining" or terminal.result_json is not None:
                raise Conflict("original cancel result must precede cleanup")
            reason = terminal_pending_reason(observed)
            if reason is not None:
                return CancelResultProjectionProgress(terminal.phase, pending_reason=reason)
            if observed.records.payload_sealed:
                raise Conflict("sealed upload cannot prepare a cancel result")
            upload = session.get(Upload, collection_id)
            assert upload is not None
            used = terminal_control_bytes(terminal, selector) + metadata_row_bytes(
                {column.name: getattr(upload, column.name) for column in Upload.__table__.columns}
            )
            _capacity(used, limits)
            previous = terminal.result_preparation_json
            try:
                state = None if previous is None else json.loads(previous)
                stamp = utc_timestamp_now() if state is None else state["timestamp"]
                base = upload_session_payload(
                    upload,
                    tag_count=0,
                    custodied_files=upload.safe_release_artifact_count,
                    custodied_bytes=upload.safe_release_artifact_bytes,
                    uploaded_bytes=0,
                    uploaded_units=0,
                    total_units=0,
                    copy_intents=[],
                    policy=parse_incremental_volume_planner_checkpoint(
                        upload.planner_checkpoint_json
                    ).policy,
                    state="canceled",
                )
                base.update(
                    latest_failure=None,
                    archive_phase="canceled",
                    archive_phase_updated_at=stamp,
                    archive_next_attempt_at=None,
                )
                source = hashlib.sha256(
                    canonical_json_bytes(
                        {
                            "payload": _encoded_payload(base),
                            "frontier": format_scalar("nonnegative", upload.planner_next_sequence),
                            "tag_root": upload.tag_staging_root_sha256,
                            "tag_identity": upload.tag_staging_set_identity,
                            "registration_revision": format_scalar(
                                "nonnegative", upload.registration_revision
                            ),
                            "custody_revision": observed.records.custody_revision,
                        }
                    )
                ).hexdigest()
                if state is None:
                    state = {
                        "format": _PREPARATION,
                        "operation_id": operation_id,
                        "source": source,
                        "timestamp": stamp,
                        "stage": "tags",
                        "after": None,
                        **{name: "0" for name in _COUNTERS},
                    }
                elif (
                    not isinstance(state, dict)
                    or set(state)
                    != {
                        "format",
                        "operation_id",
                        "source",
                        "timestamp",
                        "stage",
                        "after",
                        *_COUNTERS,
                    }
                    or state["format"] != _PREPARATION
                    or state["operation_id"] != operation_id
                    or state["source"] != source
                    or not isinstance(stamp, str)
                    or state["stage"] not in {"tags", "volumes", "result"}
                    or (state["after"] is not None and not isinstance(state["after"], str))
                    or canonical_json_bytes(state).decode() != previous
                ):
                    raise ValueError("original cancel preparation")
                for name in _COUNTERS:
                    parse_scalar("nonnegative", state[name])
            except (ValueError, TypeError, KeyError, RuntimeError) as error:
                raise Conflict("original cancel result preparation changed") from error
            rows = 0
            copies = []
            stage = state["stage"]
            if stage in {"tags", "volumes"}:
                model = Tag if stage == "tags" else Volume
                key = Tag.tag_sha256 if stage == "tags" else Volume.object_id
                fields = (
                    (key,)
                    if stage == "tags"
                    else (key, Volume.uploaded_bytes, Volume.uploaded_units, Volume.total_units)
                )
                query = select(*fields).where(model.collection_id == collection_id)
                if state["after"] is not None:
                    old = (
                        session.execute(query.where(key == state["after"])).mappings().one_or_none()
                    )
                    if old is None:
                        raise Conflict("original cancel result cursor changed")
                    used += metadata_row_bytes(dict(old))
                    _capacity(used, limits)
                    query = query.where(key > state["after"])
                with session.execute(
                    query.order_by(key)
                    .limit(limits.input_transitions)
                    .execution_options(stream_results=True, yield_per=1)
                ).mappings() as window:
                    for row in window:
                        used += metadata_row_bytes(dict(row))
                        _capacity(used, limits)
                        rows += 1
                        state["after"] = row[key.key]
                        names = ("tag_count",) if stage == "tags" else _COUNTERS[1:]
                        values = (
                            (1,)
                            if stage == "tags"
                            else tuple(row[name.removeprefix("archive_")] for name in names)
                        )
                        for name, value in zip(names, values, strict=True):
                            state[name] = format_scalar(
                                "nonnegative", parse_scalar("nonnegative", state[name]) + value
                            )
                if rows < limits.input_transitions:
                    state["stage"] = "volumes" if stage == "tags" else "result"
                    state["after"] = None
            else:
                query = (
                    select(
                        CopyIntent.destination_store,
                        CopyIntent.state,
                        CopyIntent.failure_code,
                        CopyIntent.job_created,
                    )
                    .where(CopyIntent.collection_id == collection_id)
                    .order_by(CopyIntent.destination_store)
                )
                with session.execute(
                    query.execution_options(stream_results=True, yield_per=1)
                ).mappings() as intents:
                    for intent in intents:
                        used += metadata_row_bytes(dict(intent))
                        _capacity(used, limits)
                        job = None
                        if intent["state"] == "handed_off":
                            job = (
                                session.execute(
                                    select(CopyJob.state).where(
                                        CopyJob.collection_id == collection_id,
                                        CopyJob.destination_store == intent["destination_store"],
                                    )
                                )
                                .mappings()
                                .one_or_none()
                            )
                            if job is not None:
                                used += metadata_row_bytes(dict(job))
                                _capacity(used, limits)
                        item = {
                            "destination_store": intent["destination_store"],
                            "state": "canceled",
                            "failure_code": intent["failure_code"],
                            "job_created": intent["job_created"],
                            "job_state": None if job is None else job["state"],
                        }
                        used += len(canonical_json_bytes(item))
                        _capacity(used, limits)
                        copies.append(item)
                base.update({name: parse_scalar("nonnegative", state[name]) for name in _COUNTERS})
                base["copy_intents"] = copies
                encoded = canonical_json_bytes(
                    {"format": _RESULT, "payload": _encoded_payload(base)}
                ).decode()
                used += len(encoded.encode())
                _capacity(used, limits)
                decode_cancel_terminal_result(encoded)
                terminal.result_preparation_json = None
                terminal.original_result_json = encoded
                require_prepared_terminal_result(terminal, selector)
                return CancelResultProjectionProgress(
                    terminal.phase, prepared=True, copy_rows=len(copies), metadata_bytes=used
                )
            encoded = canonical_json_bytes(state).decode()
            used += len(encoded.encode())
            _capacity(used, limits)
            terminal.result_preparation_json = encoded
            require_terminal_control_capacity(terminal, selector)
            return CancelResultProjectionProgress(terminal.phase, rows=rows, metadata_bytes=used)
