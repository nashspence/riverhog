"""Original terminal control/result storage, separate from disposal authority."""

from __future__ import annotations

import json

from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol.errors import Conflict

from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord
from riverhog_core.services.archive_upload_terminal_tags import TerminalTagSelector


def terminal_control_bytes(
    terminal: ArchiveUploadTerminalRecord, selector: TerminalTagSelector
) -> int:
    """Charge the complete encoded row and original tag reference.

    Reserve the longest current phase and a complete timestamp before accepting
    the original result. Frozen selection fields do not grow during cleanup.
    Prepared and confirmed result fields share this same original allowance;
    confirmation transfers the exact bytes rather than retaining two copies.
    """
    row = {
        column.name: getattr(terminal, column.name)
        for column in ArchiveUploadTerminalRecord.__table__.columns
    }
    row["phase"] = "needs-reconfirmation"
    row["drain_next_attempt_at"] = "9999-12-31T23:59:59.999999999Z"
    return len(
        canonical_json_bytes(
            {
                "terminal": row,
                "tag_selector": {"terminal_id": terminal.id, **selector.payload()},
            }
        )
    )


def require_terminal_control_capacity(
    terminal: ArchiveUploadTerminalRecord, selector: TerminalTagSelector
) -> None:
    capacity = terminal.control_capacity_bytes
    if type(capacity) is not int or capacity < 1:
        raise Conflict("upload terminal original byte reservation is invalid")
    if terminal_control_bytes(terminal, selector) > capacity:
        raise Conflict(
            "archive upload terminal request exceeds its byte grant including result storage"
        )


def require_prepared_terminal_result(
    terminal: ArchiveUploadTerminalRecord, selector: TerminalTagSelector
) -> str:
    """Validate retained original receiving space; this grants no cleanup rights."""
    value = terminal.original_result_json
    if value is None or terminal.result_json is not None:
        raise Conflict("upload terminal has no retained original result reservation")
    try:
        parsed = json.loads(value)
        if not isinstance(parsed, dict) or canonical_json_bytes(parsed).decode() != value:
            raise ValueError("noncanonical original result")
    except (ValueError, TypeError) as exc:
        raise Conflict("upload terminal original result is invalid") from exc
    require_terminal_control_capacity(terminal, selector)
    return value
