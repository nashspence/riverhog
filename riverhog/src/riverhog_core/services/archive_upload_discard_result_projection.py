"""Resumable original discard-result projection before terminal cleanup."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from riverhog_canonical_json import canonical_json_bytes, format_scalar, parse_scalar
from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.incremental_emission import VolumeEmissionCapacity, VolumeEmissionLimits
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
from riverhog_core.services.upload_custody_projection import upload_custody_payload

_PREPARATION = "archive-upload-discard-result-preparation/v1"
_RESULT = "archive-upload-discard-result/v1"


def _payload(values: dict[str, int], collection_id: int) -> dict[str, object]:
    custody = upload_custody_payload(
        files=values["artifact_count"],
        byte_count=values["artifact_bytes"],
        custodied_files=values["safe_release_artifact_count"],
        custodied_bytes=values["safe_release_artifact_bytes"],
    )
    return {
        "status": "discarded",
        "collection_id": format_scalar("sequence63", collection_id),
        "files": format_scalar("nonnegative", values["artifact_count"]),
        "bytes": format_scalar("nonnegative", values["artifact_bytes"]),
        "custody": {
            key: format_scalar("nonnegative", value) if type(value) is int else value
            for key, value in custody.items()
        },
        "archive_objects": "0",
    }


def decode_discard_terminal_result(encoded: str) -> dict[str, object]:
    """Recover exact public integer fields from the original internal string codecs.

    This projection supplies neither current read permission nor confirmation.
    The caller must obtain the original bytes through its current-authorized
    confirmed-outcome owner before exposing this public result.
    """
    try:
        value = json.loads(encoded)
        if (
            not isinstance(value, dict)
            or set(value) != {"format", "payload"}
            or value["format"] != _RESULT
            or canonical_json_bytes(value).decode() != encoded
        ):
            raise ValueError("original result format")
        payload = value["payload"]
        if (
            not isinstance(payload, dict)
            or set(payload)
            != {"status", "collection_id", "files", "bytes", "custody", "archive_objects"}
            or payload["status"] != "discarded"
        ):
            raise ValueError("original discard fields")
        parse_scalar("sequence63", payload["collection_id"])
        result = dict(payload)
        for name in ("files", "bytes", "archive_objects"):
            result[name] = parse_scalar("nonnegative", payload[name])
        custody = payload["custody"]
        if not isinstance(custody, dict):
            raise ValueError("original custody fields")
        if custody == {"state": "complete"}:
            actual = upload_custody_payload(
                files=result["files"],
                byte_count=result["bytes"],
                custodied_files=result["files"],
                custodied_bytes=result["bytes"],
            )
        elif set(custody) == {"state", "files", "bytes"} and custody["state"] == "pending":
            actual = upload_custody_payload(
                files=result["files"],
                byte_count=result["bytes"],
                custodied_files=parse_scalar("nonnegative", custody["files"]),
                custodied_bytes=parse_scalar("nonnegative", custody["bytes"]),
            )
            if actual["state"] != "pending":
                raise ValueError("original custody disposition")
        else:
            raise ValueError("original custody fields")
        result["custody"] = actual
        return result
    except (ValueError, TypeError, KeyError, RuntimeError) as error:
        raise Conflict("original discard terminal result is invalid") from error


@dataclass(frozen=True, slots=True)
class TerminalResultProjectionProgress:
    phase: str
    prepared: bool = False
    rows: int = 0
    metadata_bytes: int = 0
    pending_reason: PendingReason | None = None


class ArchiveUploadDiscardResultProjection:
    """Freeze one complete public result through admitted original metadata windows.

    Every quantum rechecks current terminal authority and the original closed
    construction's known writer/receiving facts. A durable keyset counts visible
    original archive objects once. The exact accepted custody scalars remain
    frozen across interruption; changed source facts refuse continuation.
    Preparation does not adjudicate cleanup or confirm any terminal outcome.
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
    ) -> TerminalResultProjectionProgress:
        if not callable(guard):
            raise ValueError("original result projection requires current authority")
        with session_scope(self._factory) as session:
            observed = self._observation.observe(session, collection_id, operation_id, guard=guard)
            terminal = session.get(Terminal, operation_id)
            assert terminal is not None
            selector = load_terminal_tag_selector(session, operation_id)
            if terminal.kind != "discard":
                raise Conflict("discard result projection requires its original operation kind")
            if terminal.original_result_json is not None:
                encoded = require_prepared_terminal_result(terminal, selector)
                decode_discard_terminal_result(encoded)
                charge = terminal_control_bytes(terminal, selector)
                if charge > limits.metadata_bytes:
                    raise VolumeEmissionCapacity(
                        dimension="metadata_bytes", required=charge, admitted=limits.metadata_bytes
                    )
                return TerminalResultProjectionProgress(
                    terminal.phase, prepared=True, metadata_bytes=charge
                )
            if terminal.phase != "draining" or terminal.result_json is not None:
                raise Conflict("original result projection must precede cleanup")
            reason = terminal_pending_reason(observed)
            if reason is not None:
                return TerminalResultProjectionProgress(terminal.phase, pending_reason=reason)
            records = observed.records
            if records.custody_revision != records.selected_custody_revision:
                return TerminalResultProjectionProgress(
                    terminal.phase, pending_reason="original-result"
                )
            used = terminal_control_bytes(terminal, selector)
            values = dict(
                session.execute(
                    select(
                        Upload.artifact_count,
                        Upload.artifact_bytes,
                        Upload.safe_release_artifact_count,
                        Upload.safe_release_artifact_bytes,
                        Upload.planner_next_sequence,
                    ).where(Upload.collection_id == collection_id)
                )
                .mappings()
                .one()
            )
            used += metadata_row_bytes(values)
            expected = _payload(values, collection_id)
            if terminal.result_preparation_json is None:
                state: dict[str, Any] = {
                    "format": _PREPARATION,
                    "operation_id": operation_id,
                    "custody_revision": records.custody_revision,
                    "volume_frontier": format_scalar(
                        "nonnegative", values["planner_next_sequence"]
                    ),
                    "after": None,
                    "payload": expected,
                }
            else:
                try:
                    state = json.loads(terminal.result_preparation_json)
                    if (
                        not isinstance(state, dict)
                        or set(state)
                        != {
                            "format",
                            "operation_id",
                            "custody_revision",
                            "volume_frontier",
                            "after",
                            "payload",
                        }
                        or state["format"] != _PREPARATION
                        or state["operation_id"] != operation_id
                        or state["custody_revision"] != records.custody_revision
                        or state["volume_frontier"]
                        != format_scalar("nonnegative", values["planner_next_sequence"])
                        or canonical_json_bytes(state).decode() != terminal.result_preparation_json
                    ):
                        raise ValueError("original preparation identity")
                    payload = state["payload"]
                    if (
                        not isinstance(payload, dict)
                        or set(payload) != set(expected)
                        or any(
                            payload[name] != value
                            for name, value in expected.items()
                            if name != "archive_objects"
                        )
                    ):
                        raise ValueError("original result source changed")
                    count = parse_scalar("nonnegative", payload["archive_objects"])
                    after = state["after"]
                    if (after is None) != (count == 0) or (
                        after is not None and (not isinstance(after, str))
                    ):
                        raise ValueError("original result cursor")
                except (ValueError, TypeError, KeyError, RuntimeError) as error:
                    raise Conflict("original discard result preparation changed") from error
            if used > limits.metadata_bytes:
                raise VolumeEmissionCapacity(
                    dimension="metadata_bytes", required=used, admitted=limits.metadata_bytes
                )
            query = select(Volume.object_id).where(Volume.collection_id == collection_id)
            if state["after"] is not None:
                previous = session.scalar(query.where(Volume.object_id == state["after"]))
                if previous != state["after"]:
                    raise Conflict("original discard result preparation changed")
                used += metadata_row_bytes({"object_id": previous})
                query = query.where(Volume.object_id > state["after"])
            rows = session.execute(
                query.order_by(Volume.object_id).limit(limits.input_transitions)
            ).all()
            used += sum(metadata_row_bytes({"object_id": row[0]}) for row in rows)
            count = parse_scalar("nonnegative", state["payload"]["archive_objects"]) + len(rows)
            state["payload"]["archive_objects"] = format_scalar("nonnegative", count)
            if rows:
                state["after"] = rows[-1][0]
            complete = len(rows) < limits.input_transitions
            if complete:
                encoded = canonical_json_bytes(
                    {"format": _RESULT, "payload": state["payload"]}
                ).decode()
                decode_discard_terminal_result(encoded)
                terminal.result_preparation_json = None
                terminal.original_result_json = encoded
                require_prepared_terminal_result(terminal, selector)
            else:
                encoded = canonical_json_bytes(state).decode()
                terminal.result_preparation_json = encoded
                require_terminal_control_capacity(terminal, selector)
            used += len(encoded.encode())
            if used > limits.metadata_bytes:
                raise VolumeEmissionCapacity(
                    dimension="metadata_bytes", required=used, admitted=limits.metadata_bytes
                )
            return TerminalResultProjectionProgress(
                terminal.phase, prepared=complete, rows=len(rows), metadata_bytes=used
            )
