from __future__ import annotations

import importlib.metadata
import json
import re
import sys
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any, cast

import httpx
import typer
from http_api_contracts import ErrorOut
from riverhog_application_access import ApplicationPermission
from riverhog_client import (
    COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES,
    ApiClient,
    IncrementalCollectionProducer,
    ProducerArtifactIdentity,
)
from riverhog_protocol.collection_description import validate_collection_description
from riverhog_protocol.errors import RiverhogError
from riverhog_protocol.paths import (
    PathNormalizationError,
    normalize_collection_id,
)
from riverhog_provenance import (
    resolve_provenance_observer,
)
from time_formats import parse_duration

from a_riverhog_cli.application_keys_output import (
    format_app_key_created,
    format_app_key_revoked,
    format_app_key_rotated,
    format_app_keys,
    format_apps,
)
from a_riverhog_cli.cli_support import (
    emit,
    error_document,
    format_lifecycle_events,
    format_list_ids,
)
from a_riverhog_cli.directory_upload import prepare_upload, preview_upload
from a_riverhog_cli.local import local_app
from a_riverhog_cli.output import (
    format_app_access,
    format_app_access_selectors,
    format_app_access_set,
    format_archive_copy_job,
    format_archive_copy_jobs,
    format_archive_copy_retirement_plan,
    format_archive_copy_retirement_result,
    format_archive_copy_selectors,
    format_archive_store,
    format_archive_stores,
    format_collection_archive_copies,
    format_collection_deletion_plan,
    format_collection_deletion_result,
    format_collection_description,
    format_collection_summary,
    format_collection_tag_membership,
    format_collection_tag_mutation,
    format_collection_tags,
    format_collection_upload,
    format_collection_upload_discard_plan,
    format_collection_upload_discard_result,
    format_collection_upload_files,
    format_collection_uploads,
    format_collections,
    format_download_quota,
    format_download_quotas,
    format_file_provenance,
    format_file_selectors,
    format_find,
    format_provenance_files,
    format_provenance_journal_agents,
    format_provenance_trace,
    format_provenance_verification_job,
    format_retrieval_cache_object,
    format_retrieval_cache_objects,
    format_retrieval_cache_selectors,
    format_retrieval_cache_status,
    format_tags,
)

_ERROR_RESPONSE_OUTPUT = {
    "kind": "python-model",
    "identity": "http-api-contracts.ErrorOut",
    "schema": ErrorOut.model_json_schema(),
}


def _cli_local_json(identity: str, schema: dict[str, object]) -> dict[str, object]:
    return {"kind": "cli-local-json-schema", "identity": identity, "schema": schema}


_STATE_STATUS_OUTPUT = _cli_local_json(
    "state-schema-status/v1",
    {
        "type": "object",
        "additionalProperties": False,
        "required": ["name", "condition", "current_revision", "head_revision"],
        "properties": {
            "name": {"type": "string"},
            "condition": {
                "enum": ["empty", "current", "upgrade_required", "unversioned", "incompatible"]
            },
            "current_revision": {"type": ["string", "null"]},
            "head_revision": {"type": "string"},
        },
    },
)
_LOCAL_COLLECTION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["collection_id", "created_at", "tag_count", "status", "files", "bytes"],
    "properties": {
        "collection_id": {"type": "integer", "minimum": 1},
        "created_at": {"type": "string"},
        "tag_count": {"type": "integer", "minimum": 0},
        "status": {"enum": ["desired", "remote-deleted", "synchronizing"]},
        "files": {"type": "integer", "minimum": 0},
        "bytes": {"type": "integer", "minimum": 0},
    },
}
_LOCAL_SYNC_OUTPUT = _cli_local_json(
    "a-riverhog-cli-local-sync-result/v1",
    {
        "type": "object",
        "required": ["status", "materialized_files"],
        "properties": {
            "status": {
                "enum": [
                    "requested",
                    "ready",
                    "cache-miss",
                    "current",
                    "materialized",
                ]
            },
            "materialized_files": {"type": "integer", "minimum": 0},
            "restore_policy": {"enum": ["allow", "never"]},
            "unavailable_files": {"type": "integer", "minimum": 0},
            "retrieval_id": {"type": "string"},
            "retrieval": {"type": "object"},
        },
        "additionalProperties": False,
    },
)

app = typer.Typer(
    help="Command-line client for Riverhog.",
    add_completion=False,
)

# These repeated options pass their complete batch to the named HTTP input through ApiClient.
_CLI_OCCURRENCE_AUTHORITIES = {
    "collection list": {"tag": {"operation_id": "list_collections", "parameter": "tags"}},
}

_CLI_RESULT_CONTRACT = {
    "format": "riverhog-cli-result-contract/v1",
    "identity_prefix": "a-riverhog-cli-result",
    "default_profile": "human-json",
    "profiles": {
        "human-json": {
            "id": "a-riverhog-cli-human-json/v1",
            "structured_output": "optional-json",
            "human_json_relationship": "same-semantic-result",
            "success": [
                {
                    "id": "completed",
                    "exit_status": 0,
                    "stdout": {
                        "human": "noncontractual-presentation-of-command-result",
                        "json": "$command-json-output",
                    },
                    "stderr": {"all": "empty"},
                }
            ],
            "failures": [
                {
                    "id": "usage",
                    "exit_status": 2,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-usage-diagnostic"},
                },
                {
                    "id": "operational",
                    "exit_status": 1,
                    "stdout": {
                        "human": "empty",
                        "json": _ERROR_RESPONSE_OUTPUT,
                    },
                    "stderr": {
                        "human": "noncontractual-diagnostic",
                        "json": "empty",
                    },
                },
            ],
        }
    },
    "command_profiles": {},
    "command_overrides": {
        "collection upload start": {
            "success": [
                {
                    "id": "completed",
                    "exit_status": 0,
                    "stdout": {
                        "human": "noncontractual-presentation-of-command-result",
                        "json": "$command-json-output",
                    },
                    "stderr": {"all": "noncontractual-progress"},
                }
            ],
            "failures": [
                {
                    "id": "usage",
                    "exit_status": 2,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-usage-diagnostic"},
                },
                {
                    "id": "operational",
                    "exit_status": 1,
                    "stdout": {
                        "human": "empty",
                        "json": _ERROR_RESPONSE_OUTPUT,
                    },
                    "stderr": {
                        "human": "noncontractual-diagnostic-or-progress",
                        "json": "noncontractual-progress",
                    },
                },
                {
                    "id": "custody-timeout",
                    "exit_status": 124,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-progress"},
                },
            ],
        },
        "collection upload watch": {
            "success": [
                {
                    "id": "completed",
                    "exit_status": 0,
                    "stdout": {
                        "human": "noncontractual-presentation-of-command-result",
                        "json": "$command-json-output",
                    },
                    "stderr": {"all": "noncontractual-progress"},
                }
            ],
            "failures": [
                {
                    "id": "usage",
                    "exit_status": 2,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-usage-diagnostic"},
                },
                {
                    "id": "operational",
                    "exit_status": 1,
                    "stdout": {
                        "human": "empty",
                        "json": _ERROR_RESPONSE_OUTPUT,
                    },
                    "stderr": {
                        "human": "noncontractual-diagnostic-or-progress",
                        "json": "noncontractual-progress",
                    },
                },
                {
                    "id": "custody-timeout",
                    "exit_status": 124,
                    "stdout": {
                        "human": "noncontractual-presentation-of-command-result",
                        "json": "$command-json-output",
                    },
                    "stderr": {"all": "noncontractual-progress"},
                },
            ],
        },
        "archive copy-job watch": {
            "failures": [
                {
                    "id": "usage",
                    "exit_status": 2,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-usage-diagnostic"},
                },
                {
                    "id": "operational",
                    "exit_status": 1,
                    "stdout": {
                        "human": "empty",
                        "json": _ERROR_RESPONSE_OUTPUT,
                    },
                    "stderr": {
                        "human": "noncontractual-diagnostic",
                        "json": "empty",
                    },
                },
                {
                    "id": "terminal-job-failure",
                    "exit_status": 1,
                    "stdout": {
                        "human": "noncontractual-presentation-of-command-result",
                        "json": "$command-json-output",
                    },
                    "stderr": {"all": "empty"},
                },
            ]
        },
        **{
            command: {
                "success": [
                    {
                        "id": "planned",
                        "exit_status": 0,
                        "stdout": {
                            "human": "noncontractual-presentation-of-command-result",
                            "json": "$command-json-output",
                        },
                        "stderr": {"all": "empty"},
                    },
                    {
                        "id": "executed",
                        "exit_status": 0,
                        "stdout": {
                            "human": "noncontractual-presentation-of-command-result",
                            "json": "$command-json-output",
                        },
                        "stderr": {"all": "empty"},
                    },
                ],
                "failures": [
                    {
                        "id": "usage",
                        "exit_status": 2,
                        "stdout": {"all": "empty"},
                        "stderr": {"all": "noncontractual-usage-diagnostic"},
                    },
                    {
                        "id": "operational",
                        "exit_status": 1,
                        "stdout": {
                            "human": "empty",
                            "json": _ERROR_RESPONSE_OUTPUT,
                        },
                        "stderr": {
                            "human": "noncontractual-diagnostic",
                            "json": "empty",
                        },
                    },
                    {
                        "id": "blocked",
                        "exit_status": 1,
                        "stdout": {"human": "noncontractual-presentation-of-command-result"},
                        "stderr": {"human": "empty"},
                    },
                    {
                        "id": "confirmation-declined",
                        "exit_status": 1,
                        "stdout": {"human": "noncontractual-presentation-of-command-result"},
                        "stderr": {"human": "noncontractual-diagnostic"},
                    },
                ],
            }
            for command in (
                "archive retire",
                "collection delete",
                "collection upload discard",
            )
        },
        "local audit": {
            "failures": [
                {
                    "id": "usage",
                    "exit_status": 2,
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-usage-diagnostic"},
                },
                {
                    "id": "operational",
                    "exit_status": 1,
                    "stdout": {
                        "human": "empty",
                        "json": _ERROR_RESPONSE_OUTPUT,
                    },
                    "stderr": {
                        "human": "noncontractual-diagnostic",
                        "json": "empty",
                    },
                },
                {
                    "id": "audit-issues",
                    "exit_status": 1,
                    "stdout": {
                        "human": "noncontractual-presentation-of-command-result",
                        "json": "$command-json-output",
                    },
                    "stderr": {"all": "empty"},
                },
            ]
        },
    },
    "executable_groups": [],
    "outcome_selectors": {
        "completed": {"kind": "command-completed"},
        "planned": {"kind": "option-equals", "parameter": "dry_run", "value": True},
        "executed": {"kind": "option-equals", "parameter": "dry_run", "value": False},
        "usage": {"kind": "parser-rejected-invocation"},
        "operational": {"kind": "application-error"},
        "custody-timeout": {
            "kind": "custody-deadline-expired",
            "state": "not-finalized",
        },
        "terminal-job-failure": {
            "kind": "archive-copy-state",
            "state": "failed",
        },
        "blocked": {"kind": "plan-reported-blockers"},
        "confirmation-declined": {"kind": "interactive-confirmation-mismatch"},
        "audit-issues": {"kind": "local-audit-problem-count-positive"},
    },
    "output_authorities": {
        "archive retire": {
            "outcomes": {
                "planned": {
                    "kind": "operation-response",
                    "operation_id": "plan_archive_copy_retirement",
                },
                "executed": {
                    "kind": "operation-response",
                    "operation_id": "retire_archive_copy",
                },
            }
        },
        "collection delete": {
            "outcomes": {
                "planned": {
                    "kind": "operation-response",
                    "operation_id": "plan_collection_deletion",
                },
                "executed": {
                    "kind": "operation-response",
                    "operation_id": "delete_collection",
                },
            }
        },
        "collection upload discard": {
            "outcomes": {
                "planned": {
                    "kind": "operation-response",
                    "operation_id": "plan_collection_upload_discard",
                },
                "executed": {
                    "kind": "operation-response",
                    "operation_id": "discard_collection_upload",
                },
            }
        },
        "collection describe": {
            "kind": "operation-response",
            "operation_id": "replace_collection_description",
        },
        "collection provenance verify": {
            "kind": "openapi-schema",
            "schema": "CollectionProvenanceVerificationJobOut",
        },
        "collection tag add": {
            "kind": "operation-response",
            "operation_id": "add_collection_tag",
        },
        "collection tag contains": {
            "kind": "operation-response",
            "operation_id": "collection_contains_tag",
        },
        "collection tag list": {
            "kind": "operation-response",
            "operation_id": "list_collection_tags",
        },
        "collection tag remove": {
            "kind": "operation-response",
            "operation_id": "remove_collection_tag",
        },
        "collection upload start": {
            "kind": "operation-response",
            "operation_id": "get_collection_upload_session",
        },
        "collection provenance export": _cli_local_json(
            "a-riverhog-cli-provenance-journal-export/v1",
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["collection_id", "journal_id", "output", "bytes", "sha256"],
                "properties": {
                    "collection_id": {"type": "integer", "minimum": 1},
                    "journal_id": {"type": "string"},
                    "output": {"type": "string"},
                    "bytes": {"type": "integer", "minimum": 0},
                    "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                },
            },
        ),
        "local add": _cli_local_json(
            "a-riverhog-cli-local-add-result/v1",
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["status", "collection"],
                "properties": {
                    "status": {"const": "added"},
                    "collection": _LOCAL_COLLECTION_SCHEMA,
                },
            },
        ),
        "local remove": _cli_local_json(
            "a-riverhog-cli-local-remove-result/v1",
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["status", "collection_id", "local_files", "retrievals_canceled"],
                "properties": {
                    "status": {"const": "removed"},
                    "collection_id": {"type": "integer", "minimum": 1},
                    "local_files": {"const": "retained"},
                    "retrievals_canceled": {"type": "array", "items": {"type": "string"}},
                },
            },
        ),
        "local evict": _cli_local_json(
            "a-riverhog-cli-local-evict-result/v1",
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["status", "collection_id", "retrievals_canceled"],
                "properties": {
                    "status": {"const": "evicted"},
                    "collection_id": {"type": "integer", "minimum": 1},
                    "retrievals_canceled": {"type": "array", "items": {"type": "string"}},
                },
            },
        ),
        "local list": _cli_local_json(
            "a-riverhog-cli-local-collection-list/v1",
            {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "page_size",
                    "next_page_token",
                    "sort",
                    "order",
                    "query",
                    "collections",
                ],
                "properties": {
                    "page_size": {"type": "integer", "minimum": 1, "maximum": 100},
                    "next_page_token": {"type": ["string", "null"]},
                    "sort": {"enum": ["bytes", "collection_id", "created_at", "files", "status"]},
                    "order": {"enum": ["asc", "desc"]},
                    "query": {"type": ["string", "null"]},
                    "collections": {"type": "array", "items": _LOCAL_COLLECTION_SCHEMA},
                },
            },
        ),
        "local show": _cli_local_json(
            "a-riverhog-cli-local-collection/v1", _LOCAL_COLLECTION_SCHEMA
        ),
        "local sync": _LOCAL_SYNC_OUTPUT,
        "local repair": _LOCAL_SYNC_OUTPUT,
        "local audit": _cli_local_json(
            "a-riverhog-cli-local-audit-result/v1",
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["status", "problems", "samples", "samples_truncated"],
                "properties": {
                    "status": {"enum": ["ok", "issues"]},
                    "problems": {"type": "integer", "minimum": 0},
                    "samples": {"type": "array", "items": {"type": "string"}, "maxItems": 100},
                    "samples_truncated": {"type": "boolean"},
                },
            },
        ),
        "local provenance-observer list": _cli_local_json(
            "riverhog-provenance-observer-provider-list/v1",
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["format", "providers"],
                "properties": {
                    "format": {"const": "riverhog-provenance-observer-provider-list/v1"},
                    "providers": {"type": "array", "items": {"type": "object"}},
                },
            },
        ),
        "local provenance-observer show": _cli_local_json(
            "riverhog-provenance-observer-binding/v1",
            {
                "type": "object",
                "required": [
                    "format",
                    "name",
                    "observer_id",
                    "contract_provider",
                    "contract_id",
                    "contract_sha256",
                    "schema_dialect",
                    "format_policy",
                    "schema_ids",
                ],
                "properties": {"format": {"const": "riverhog-provenance-observer-binding/v1"}},
            },
        ),
        "local state status": _STATE_STATUS_OUTPUT,
        "local state upgrade": _STATE_STATUS_OUTPUT,
        "local state verify": _STATE_STATUS_OUTPUT,
    },
    "version_distribution": "a-riverhog-cli",
}
collection_app = typer.Typer(help="Collection catalog and upload operations.")
collection_tag_app = typer.Typer(help="Exact collection tag authority.")
collection_upload_app = typer.Typer(help="Collection upload sessions.")
collection_provenance_app = typer.Typer(help="Collection file provenance.")
archive_app = typer.Typer(help="Archive-store operations.")
archive_store_app = typer.Typer(help="Configured archive stores.")
archive_copy_app = typer.Typer(help="Archive-copy jobs.")
application_app = typer.Typer(help="Application access.")
app_key_app = typer.Typer(help="Application key management.")
app_key_access_app = typer.Typer(help="Per-key permission and resource access.")
app_key_quota_app = typer.Typer(help="Per-key remote-download quotas.")
tag_app = typer.Typer(help="Searchable collection tags.")
event_app = typer.Typer(help="Lifecycle event inspection.")
catalog_sync_app = typer.Typer(help="Bounded native catalog synchronization steps.")
retrieval_app = typer.Typer(help="Retrieval operations.")
retrieval_cache_app = typer.Typer(help="Retrieval-cache inspection.")
app_key_app.add_typer(app_key_access_app, name="access")
app_key_app.add_typer(app_key_quota_app, name="quota")
application_app.add_typer(app_key_app, name="key")
app.add_typer(collection_app, name="collection")
collection_app.add_typer(collection_tag_app, name="tag")
collection_app.add_typer(collection_upload_app, name="upload")
collection_app.add_typer(collection_provenance_app, name="provenance")
app.add_typer(archive_app, name="archive")
archive_app.add_typer(archive_store_app, name="store")
archive_app.add_typer(archive_copy_app, name="copy-job")
app.add_typer(application_app, name="app")
app.add_typer(tag_app, name="tag")
app.add_typer(event_app, name="event")
app.add_typer(catalog_sync_app, name="catalog-sync")
app.add_typer(retrieval_app, name="retrieval")
retrieval_app.add_typer(retrieval_cache_app, name="cache")
app.add_typer(local_app, name="local")

_API_CLIENT: ApiClient | None = None


def client() -> ApiClient:
    global _API_CLIENT
    if _API_CLIENT is None:
        _API_CLIENT = ApiClient()
    return _API_CLIENT


def _close_client() -> None:
    global _API_CLIENT
    if _API_CLIENT is not None:
        _API_CLIENT.close()
        _API_CLIENT = None


def _version_callback(value: bool) -> None:
    if not value:
        return
    typer.echo(importlib.metadata.version("a-riverhog-cli"))
    raise typer.Exit()


@app.callback()
def _root(
    ctx: typer.Context,
    _version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Show the installed Riverhog CLI version",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    ctx.call_on_close(_close_client)


@event_app.command("list")
def event_list_cmd(
    after: Annotated[
        str | None,
        typer.Option("--after", help="Return events after this cursor"),
    ] = None,
    limit: Annotated[int, typer.Option("--limit", min=1, max=100)] = 100,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List application-visible lifecycle events."""

    payload = (
        client()
        .list_lifecycle_events(after=after, limit=limit)
        .model_dump(mode="json", exclude_none=True)
    )
    emit(payload if json_mode else format_lifecycle_events(payload), json_mode=json_mode)


@catalog_sync_app.command("checkpoint")
def catalog_sync_checkpoint_cmd(
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Capture one exact authorized synchronization checkpoint."""

    payload = client().create_catalog_sync_checkpoint().model_dump(mode="json")
    human = "\n".join(
        (
            f"source: {payload['source_identity']}",
            f"authorization view: {payload['authorization_view_identity']}",
            f"catalog cursor: {payload['catalog_cursor']}",
        )
    )
    emit(payload if json_mode else human, json_mode=json_mode)


@catalog_sync_app.command("collections")
def catalog_sync_collections_cmd(
    cursor: Annotated[str, typer.Option("--cursor", help="Exact catalog continuation")],
    limit: Annotated[int, typer.Option("--limit", min=1, max=100)] = 100,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Apply one bounded collection-frontier read step."""

    result = client().list_catalog_sync_collections(cursor, limit=limit)
    payload = result.model_dump(mode="json")
    lines = [f"collections: {len(result.collections)}"]
    for item in result.collections:
        lines.append(
            f"- {item.collection_id}  revision={item.revision}  "
            f"archive_root_sha256={item.archive_root_sha256}  "
            f"artifact_set_identity={item.artifact_set_identity}"
        )
        if item.description is not None:
            lines.append(f"  description: {item.description}")
        lines.append(f"  description revision: {item.description_revision}")
        lines.append(f"  description identity: {item.description_identity}")
    lines.append(f"next cursor: {result.next_cursor or '-'}")
    lines.append(f"changes cursor: {result.changes_cursor or '-'}")
    emit(payload if json_mode else "\n".join(lines), json_mode=json_mode)


@catalog_sync_app.command("changes")
def catalog_sync_changes_cmd(
    cursor: Annotated[str, typer.Option("--cursor", help="Exact change continuation")],
    limit: Annotated[int, typer.Option("--limit", min=1, max=100)] = 100,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Apply one bounded fixed-horizon change step."""

    result = client().list_catalog_sync_changes(cursor, limit=limit)
    payload = result.model_dump(mode="json")
    lines = [
        f"changes: {len(result.changes)}",
        f"through revision: {result.through_revision}",
        f"caught up: {'yes' if result.caught_up else 'no'}",
    ]
    lines.extend(
        f"- {item.operation} collection={item.collection_id} revision={item.revision}"
        for item in result.changes
    )
    lines.append(f"next cursor: {result.next_cursor}")
    emit(payload if json_mode else "\n".join(lines), json_mode=json_mode)


_APP_SORT_FIELDS = {"name", "keys", "active_keys", "last_used_at"}
_APP_KEY_SORT_FIELDS = {"id", "created_at", "expires_at", "last_used_at"}
_APP_KEY_ACCESS_SORT_FIELDS = {"app", "key_id", "permission", "resource", "created_at"}
_APP_KEY_QUOTA_SORT_FIELDS = {
    "app",
    "key_id",
    "monthly_bytes",
    "accounted_bytes",
    "reserved_bytes",
    "remaining_bytes",
}
_BYTE_SIZE_RE = re.compile(r"^(?P<count>\d+)(?P<unit>b|kib|mib|gib|tib)?$", re.IGNORECASE)
_BYTE_SIZE_FACTORS = {
    "": 1,
    "b": 1,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}


def _list_order(sort: str, order: str, *, fields: set[str]) -> str:
    if sort not in fields:
        raise typer.BadParameter(
            f"sort must be one of {', '.join(sorted(fields))}",
            param_hint="--sort",
        )
    normalized_order = order.casefold()
    if normalized_order not in {"asc", "desc"}:
        raise typer.BadParameter("order must be asc or desc", param_hint="--order")
    return normalized_order


def _monthly_quota_bytes(value: str) -> int | None:
    candidate = value.strip().casefold()
    if candidate == "unlimited":
        return None
    match = _BYTE_SIZE_RE.fullmatch(candidate)
    if match is None:
        raise typer.BadParameter(
            "quota must be unlimited or a byte size such as 500GiB",
            param_hint="LIMIT",
        )
    return int(match.group("count")) * _BYTE_SIZE_FACTORS[match.group("unit") or ""]


def _access(value: str) -> dict[str, str]:
    permission, separator, resource = value.strip().partition("=")
    if not permission:
        raise typer.BadParameter("access requires a permission", param_hint="--allow")
    if separator and not resource:
        raise typer.BadParameter("access resource must not be empty", param_hint="--allow")
    return {"permission": permission, "resource": resource if separator else "*"}


def _access_selector(value: str) -> tuple[str, str, dict[str, str]]:
    app_name, separator, remainder = value.partition("::")
    key_id, second_separator, allow = remainder.partition("::")
    if not separator or not second_separator or not app_name or not key_id or not allow:
        raise typer.BadParameter(
            "access selector must be APP::KEY_ID::PERMISSION or APP::KEY_ID::PERMISSION=RESOURCE",
            param_hint="SELECTOR",
        )
    return app_name, key_id, _access(allow)


def _archive_copy_selector(value: str) -> tuple[int, str]:
    collection_id, separator, destination_store = value.partition("::")
    if not separator or not collection_id or not destination_store:
        raise typer.BadParameter(
            "archive-copy selector must be COLLECTION_ID::DESTINATION_STORE",
            param_hint="SELECTOR",
        )
    try:
        normalized_collection_id = normalize_collection_id(collection_id)
    except PathNormalizationError as exc:
        raise typer.BadParameter(str(exc), param_hint="SELECTOR") from exc
    return normalized_collection_id, destination_store


def _retrieval_cache_selector(value: str) -> tuple[int, str, str]:
    collection_id, separator, remainder = value.partition("::")
    source_store, second_separator, object_id = remainder.partition("::")
    if (
        not separator
        or not second_separator
        or not collection_id
        or not source_store
        or not object_id
    ):
        raise typer.BadParameter(
            "retrieval-cache selector must be COLLECTION_ID::SOURCE_STORE::OBJECT_ID",
            param_hint="SELECTOR",
        )
    try:
        normalized_collection_id = normalize_collection_id(collection_id)
    except PathNormalizationError as exc:
        raise typer.BadParameter(str(exc), param_hint="SELECTOR") from exc
    return normalized_collection_id, source_store, object_id


@application_app.command("list")
def app_list_cmd(
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "name",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "asc",
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Substring match over app names"),
    ] = None,
    active: Annotated[
        bool | None,
        typer.Option("--active/--inactive", help="Filter by usable key availability"),
    ] = None,
    ids: Annotated[bool, typer.Option("--ids", help="Emit one app name per line")] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List applications with key summaries."""

    if ids and json_mode:
        raise typer.BadParameter("--ids and --json cannot be used together")
    normalized_order = _list_order(sort, order, fields=_APP_SORT_FIELDS)
    api = client()
    payload = api.list_apps(
        page_size=page_size,
        page_token=page_token,
        q=query,
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
        active=active,
    )
    if ids:
        emit(format_list_ids(payload, "apps", id_key="name"), json_mode=False)
        return
    emit(payload if json_mode else format_apps(payload), json_mode=json_mode)


@tag_app.command("list")
def tag_list_cmd(
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    query: Annotated[str | None, typer.Option("--query", "-q")] = None,
    ids: Annotated[bool, typer.Option("--ids", help="Emit one tag per line")] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List visible searchable tags and collection counts."""

    if ids and json_mode:
        raise typer.BadParameter("--ids and --json cannot be used together")
    payload = client().list_tags(
        page_size=page_size,
        page_token=page_token,
        q=query,
    )
    if ids:
        emit(format_list_ids(payload, "tags", id_key="tag"), json_mode=False)
        return
    emit(payload if json_mode else format_tags(payload), json_mode=json_mode)


@collection_tag_app.command("list")
def collection_tag_list_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    revision: Annotated[int | None, typer.Option("--revision", min=1)] = None,
    tag_set_identity: Annotated[str | None, typer.Option("--tag-set-identity")] = None,
    ids: Annotated[bool, typer.Option("--ids", help="Emit one tag per line")] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List one exact immutable revision of a collection's tag set."""

    if ids and json_mode:
        raise typer.BadParameter("--ids and --json cannot be used together")
    revision, tag_set_identity = _collection_tag_authority(
        collection_id,
        revision=revision,
        tag_set_identity=tag_set_identity,
    )
    payload = client().list_collection_tags(
        collection_id,
        revision=revision,
        tag_set_identity=tag_set_identity,
        page_size=page_size,
        page_token=page_token,
    )
    if ids:
        emit("\n".join(str(tag) for tag in payload.get("tags", [])), json_mode=False)
        return
    emit(payload if json_mode else format_collection_tags(payload), json_mode=json_mode)


@collection_tag_app.command("contains")
def collection_tag_contains_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    tag: Annotated[str, typer.Argument(help="Exact case-sensitive tag")],
    revision: Annotated[int | None, typer.Option("--revision", min=1)] = None,
    tag_set_identity: Annotated[str | None, typer.Option("--tag-set-identity")] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Test exact membership in one immutable tag-set revision."""

    revision, tag_set_identity = _collection_tag_authority(
        collection_id,
        revision=revision,
        tag_set_identity=tag_set_identity,
    )
    payload = client().collection_contains_tag(
        collection_id,
        tag=tag,
        revision=revision,
        tag_set_identity=tag_set_identity,
    )
    emit(
        payload if json_mode else format_collection_tag_membership(payload),
        json_mode=json_mode,
    )


@collection_tag_app.command("add")
def collection_tag_add_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    tag: Annotated[str, typer.Argument(help="Exact case-sensitive tag")],
    revision: Annotated[int | None, typer.Option("--revision", min=1)] = None,
    tag_set_identity: Annotated[str | None, typer.Option("--tag-set-identity")] = None,
    operation_id: Annotated[str | None, typer.Option("--operation-id")] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Conditionally add one tag using a stable operation identity."""

    revision, tag_set_identity = _collection_tag_authority(
        collection_id,
        revision=revision,
        tag_set_identity=tag_set_identity,
    )
    payload = client().add_collection_tag(
        collection_id,
        tag=tag,
        operation_id=operation_id or str(uuid.uuid4()),
        expected_revision=revision,
        expected_tag_set_identity=tag_set_identity,
    )
    emit(
        payload if json_mode else format_collection_tag_mutation(payload),
        json_mode=json_mode,
    )


@collection_tag_app.command("remove")
def collection_tag_remove_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    tag: Annotated[str, typer.Argument(help="Exact case-sensitive tag")],
    revision: Annotated[int | None, typer.Option("--revision", min=1)] = None,
    tag_set_identity: Annotated[str | None, typer.Option("--tag-set-identity")] = None,
    operation_id: Annotated[str | None, typer.Option("--operation-id")] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Conditionally remove one tag using a stable operation identity."""

    revision, tag_set_identity = _collection_tag_authority(
        collection_id,
        revision=revision,
        tag_set_identity=tag_set_identity,
    )
    payload = client().remove_collection_tag(
        collection_id,
        tag=tag,
        operation_id=operation_id or str(uuid.uuid4()),
        expected_revision=revision,
        expected_tag_set_identity=tag_set_identity,
    )
    emit(
        payload if json_mode else format_collection_tag_mutation(payload),
        json_mode=json_mode,
    )


def _collection_tag_authority(
    collection_id: int,
    *,
    revision: int | None,
    tag_set_identity: str | None,
) -> tuple[int, str]:
    if (revision is None) != (tag_set_identity is None):
        raise typer.BadParameter("--revision and --tag-set-identity must be supplied together")
    if revision is not None and tag_set_identity is not None:
        return revision, tag_set_identity
    collection = client().get_collection(collection_id)
    current_revision = collection.get("tag_revision")
    current_identity = collection.get("tag_set_identity")
    if not isinstance(current_revision, int) or not isinstance(current_identity, str):
        raise RuntimeError("collection response omitted tag authority")
    return current_revision, current_identity


@app_key_app.command("create")
def app_key_create_cmd(
    app_name: Annotated[str, typer.Argument(help="Application name")],
    allow: Annotated[
        list[str],
        typer.Option(
            "--allow",
            help="PERMISSION or PERMISSION=RESOURCE; repeat for more than one.",
        ),
    ],
    expires_in: Annotated[
        str | None,
        typer.Option("--expires-in", help="Optional key lifetime such as 30d"),
    ] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Create a key and show its token once."""

    expires_in_seconds: int | None = None
    if expires_in is not None:
        try:
            expires_in_seconds = int(parse_duration(expires_in).total_seconds())
        except ValueError as exc:
            raise typer.BadParameter(str(exc), param_hint="--expires-in") from exc
        if expires_in_seconds < 1:
            raise typer.BadParameter(
                "expiry must be at least one second",
                param_hint="--expires-in",
            )
    payload = client().create_app_key(
        app_name,
        access=[_access(current) for current in allow],
        expires_in_seconds=expires_in_seconds,
    )
    emit(payload if json_mode else format_app_key_created(payload), json_mode=json_mode)


@app_key_app.command("list")
def app_key_list_cmd(
    app_name: Annotated[str, typer.Argument(help="Application name")],
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "created_at",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "desc",
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Substring match over key ids"),
    ] = None,
    active: Annotated[
        bool | None,
        typer.Option("--active/--inactive", help="Filter by key usability"),
    ] = None,
    ids: Annotated[bool, typer.Option("--ids", help="Emit one key id per line")] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List keys without exposing their tokens."""

    if ids and json_mode:
        raise typer.BadParameter("--ids and --json cannot be used together")
    normalized_order = _list_order(sort, order, fields=_APP_KEY_SORT_FIELDS)
    api = client()
    payload = api.list_app_keys(
        app_name,
        page_size=page_size,
        page_token=page_token,
        q=query,
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
        active=active,
    )
    if ids:
        emit(format_list_ids(payload, "keys"), json_mode=False)
        return
    emit(payload if json_mode else format_app_keys(payload), json_mode=json_mode)


@app_key_app.command("revoke")
def app_key_revoke_cmd(
    app_name: Annotated[str, typer.Argument(help="External application name")],
    key_id: Annotated[str, typer.Argument(help="Key id")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Immediately revoke one application key."""

    payload = client().revoke_app_key(app_name, key_id)
    emit(payload if json_mode else format_app_key_revoked(payload), json_mode=json_mode)


@app_key_app.command("rotate")
def app_key_rotate_cmd(
    app_name: Annotated[str, typer.Argument(help="Application name")],
    key_id: Annotated[str, typer.Argument(help="Stable key id")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Replace one key token while preserving its identity and policy."""

    payload = client().rotate_app_key(app_name, key_id)
    emit(payload if json_mode else format_app_key_rotated(payload), json_mode=json_mode)


@app_key_access_app.command("list")
def app_key_access_list_cmd(
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "permission",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "asc",
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Substring match over permissions and resources"),
    ] = None,
    app_name: Annotated[
        str | None,
        typer.Option("--app", help="Restrict results to one application"),
    ] = None,
    key_id: Annotated[
        str | None,
        typer.Option("--key", help="Restrict results to one key"),
    ] = None,
    permission: Annotated[
        str | None,
        typer.Option("--permission", help="Restrict results to one exact permission"),
    ] = None,
    resource: Annotated[
        str | None,
        typer.Option("--resource", help="Restrict results to one exact resource"),
    ] = None,
    active: Annotated[
        bool | None,
        typer.Option("--active/--inactive", help="Filter by key usability"),
    ] = None,
    selectors: Annotated[
        bool,
        typer.Option("--selectors", help="Emit one actionable access selector per line"),
    ] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List exact application-key permission and resource bindings."""

    if selectors and json_mode:
        raise typer.BadParameter("--selectors and --json cannot be used together")
    normalized_order = _list_order(sort, order, fields=_APP_KEY_ACCESS_SORT_FIELDS)
    api = client()
    payload = api.list_app_key_access(
        page_size=page_size,
        page_token=page_token,
        q=query,
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
        app=app_name,
        key_id=key_id,
        permission=cast(ApplicationPermission | None, permission),
        resource=resource,
        active=active,
    )
    if selectors:
        emit(format_app_access_selectors(payload), json_mode=False)
        return
    emit(payload if json_mode else format_app_access(payload), json_mode=json_mode)


@app_key_access_app.command("set")
def app_key_access_set_cmd(
    app_name: Annotated[str, typer.Argument(help="Application name")],
    key_id: Annotated[str, typer.Argument(help="Key id")],
    allow: Annotated[
        list[str],
        typer.Option(
            "--allow",
            help="Replacement PERMISSION or PERMISSION=RESOURCE; repeat as needed.",
        ),
    ],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Replace every permission and resource binding for a key."""

    payload = client().replace_app_key_access(
        app_name,
        key_id,
        access=[_access(current) for current in allow],
    )
    emit(payload if json_mode else format_app_access_set(payload), json_mode=json_mode)


@app_key_access_app.command("add")
def app_key_access_add_cmd(
    app_name: Annotated[str, typer.Argument(help="Application name")],
    key_id: Annotated[str, typer.Argument(help="Key id")],
    allow: Annotated[str, typer.Argument(help="PERMISSION or PERMISSION=RESOURCE")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Add one exact permission and resource binding."""

    access = _access(allow)
    payload = client().add_app_key_access(
        app_name,
        key_id,
        permission=cast(Any, access["permission"]),
        resource=access["resource"],
    )
    emit(payload if json_mode else format_app_access_set(payload), json_mode=json_mode)


@app_key_access_app.command("remove")
def app_key_access_remove_cmd(
    selector: Annotated[
        str,
        typer.Argument(help="APP::KEY_ID::PERMISSION or APP::KEY_ID::PERMISSION=RESOURCE"),
    ],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Remove one exact permission and resource binding."""

    app_name, key_id, access = _access_selector(selector)
    payload = client().remove_app_key_access(
        app_name,
        key_id,
        permission=cast(Any, access["permission"]),
        resource=access["resource"],
    )
    emit(payload if json_mode else format_app_access_set(payload), json_mode=json_mode)


@app_key_quota_app.command("show")
def app_key_quota_show_cmd(
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Show the current key's remote-download quota and usage."""

    payload = client().get_download_quota()
    emit(payload if json_mode else format_download_quota(payload), json_mode=json_mode)


@app_key_quota_app.command("set")
def app_key_quota_set_cmd(
    app_name: Annotated[str, typer.Argument(help="Application name")],
    key_id: Annotated[str, typer.Argument(help="Key id")],
    limit: Annotated[
        str,
        typer.Argument(help="Monthly limit such as 500GiB, 0 to block, or unlimited"),
    ],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Assign a monthly remote-download quota without resetting usage."""

    payload = client().set_app_key_download_quota(
        app_name,
        key_id,
        monthly_bytes=_monthly_quota_bytes(limit),
    )
    emit(payload if json_mode else format_download_quota(payload), json_mode=json_mode)


@app_key_quota_app.command("list")
def app_key_quota_list_cmd(
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "app",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "asc",
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Substring match over app names and key ids"),
    ] = None,
    app_name: Annotated[
        str | None,
        typer.Option("--app", help="Filter by application name"),
    ] = None,
    active: Annotated[
        bool | None,
        typer.Option("--active/--inactive", help="Filter by key usability"),
    ] = None,
    ids: Annotated[bool, typer.Option("--ids", help="Emit one key id per line")] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List per-key quotas and current-month accounting."""

    if ids and json_mode:
        raise typer.BadParameter("--ids and --json cannot be used together")
    if ids and app_name is None:
        raise typer.BadParameter("--ids requires --app so each key id is actionable")
    normalized_order = _list_order(sort, order, fields=_APP_KEY_QUOTA_SORT_FIELDS)
    api = client()
    payload = api.list_download_quotas(
        page_size=page_size,
        page_token=page_token,
        q=query,
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
        app=app_name,
        active=active,
    )
    if ids:
        emit(format_list_ids(payload, "quotas"), json_mode=False)
        return
    emit(payload if json_mode else format_download_quotas(payload), json_mode=json_mode)


def _archive_wait_status(payload: Mapping[str, object]) -> str:
    phase = payload.get("archive_phase")
    status = f", archive_phase={phase}" if phase else ""
    uploaded_units = payload.get("archive_uploaded_units")
    total_units = payload.get("archive_total_units")
    if isinstance(uploaded_units, int) and isinstance(total_units, int) and total_units > 0:
        status += f", units={uploaded_units}/{total_units}"
    latest_failure = payload.get("latest_failure")
    if latest_failure:
        status += f", latest_failure={latest_failure}"
    next_attempt = payload.get("archive_next_attempt_at")
    if next_attempt:
        status += f", archive_next_attempt_at={next_attempt}"
    return status


_COLLECTION_SORT_FIELDS = {
    "id",
    "created_at",
    "bytes",
    "files",
}
_FIND_SORT_FIELDS = {
    "file_ref",
    "collection_id",
    "path",
    "bytes",
}


@collection_app.command("list")
def collection_list_cmd(
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "id",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "asc",
    query: Annotated[
        str | None,
        typer.Option(
            "--query",
            "-q",
            help="Substring match over collection ids, descriptions, paths, and tags",
        ),
    ] = None,
    encryption_format: Annotated[
        str | None,
        typer.Option("--encryption-format", help="Restrict results to one archive format"),
    ] = None,
    passphrase_id: Annotated[
        str | None,
        typer.Option("--passphrase-id", help="Restrict results to one opaque key ID"),
    ] = None,
    tag: Annotated[
        list[str] | None,
        typer.Option("--tag", help="Require an exact tag; repeat for all-of selection"),
    ] = None,
    ids: Annotated[
        bool,
        typer.Option("--ids", help="Emit one collection id per line"),
    ] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List collections with archive summaries."""

    if ids and json_mode:
        raise typer.BadParameter("--ids and --json cannot be used together")
    normalized_order = _list_order(sort, order, fields=_COLLECTION_SORT_FIELDS)
    api = client()
    payload = api.list_collections(
        page_size=page_size,
        page_token=page_token,
        q=query,
        tags=tag or (),
        encryption_format=encryption_format,
        passphrase_id=passphrase_id,
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
    )
    if ids:
        emit(format_list_ids(payload, "collections"), json_mode=False)
        return
    emit(payload if json_mode else format_collections(payload), json_mode=json_mode)


@collection_app.command("archive-copies")
def collection_archive_copies_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List the durable archive copies of one collection."""

    api = client()
    payload = api.list_collection_archive_copies(
        collection_id,
        page_size=page_size,
        page_token=page_token,
    )
    emit(
        payload if json_mode else format_collection_archive_copies(payload),
        json_mode=json_mode,
    )


@collection_upload_app.command("start")
def upload_cmd(
    root: Annotated[Path, typer.Argument(help="Local collection root directory")],
    idempotency_key: Annotated[str | None, typer.Option("--idempotency-key")] = None,
    archive_store: Annotated[str | None, typer.Option("--archive-store")] = None,
    use_cache: Annotated[bool | None, typer.Option("--use-cache/--no-use-cache")] = None,
    copy_to: Annotated[list[str] | None, typer.Option("--copy-to")] = None,
    description: Annotated[str | None, typer.Option("--description")] = None,
    tag: Annotated[list[str] | None, typer.Option("--tag")] = None,
    provenance_observer: Annotated[
        str | None,
        typer.Option("--provenance-observer", envvar="A_RIVERHOG_CLI_PROVENANCE_OBSERVER"),
    ] = None,
    source_host_id: Annotated[
        str | None,
        typer.Option("--source-host-id", envvar="A_RIVERHOG_CLI_SOURCE_HOST_ID"),
    ] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
) -> None:
    """Upload every regular source as an opaque artifact with canonical provenance."""

    key = idempotency_key or uuid.uuid4().hex
    resolved_root = root.expanduser().resolve()
    if description is not None:
        try:
            description = validate_collection_description(description)
        except ValueError as exc:
            raise typer.BadParameter(str(exc), param_hint="--description") from exc
    try:
        if dry_run:
            preview = preview_upload(resolved_root)
            payload = {
                "format": "a-riverhog-cli-upload-preview/v1",
                "idempotency_key": key,
                "artifact_count": len(preview),
                "total_bytes": sum(int(item["bytes"]) for item in preview),
                "sources": preview[:5],
            }
            emit(
                payload if json_mode else f"would upload {len(preview)} artifacts",
                json_mode=json_mode,
            )
            return
        upload = prepare_upload(resolved_root, key)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="root") from exc
    selected_observer = (
        resolve_provenance_observer(provenance_observer)
        if provenance_observer is not None
        else None
    )
    if selected_observer is not None and source_host_id is None:
        raise typer.BadParameter(
            "a selected native provenance observer requires --source-host-id",
            param_hint="--source-host-id",
        )
    typer.echo(f"upload retry key: {key}", err=True)
    try:
        version = importlib.metadata.version("a-riverhog-cli")
    except importlib.metadata.PackageNotFoundError:
        version = "development"
    producer = IncrementalCollectionProducer(
        client(),
        producer_app="a-riverhog-cli",
        adapter_id="a-riverhog-cli.directory/v1",
        adapter_version=version,
        ingest_source="local-directory",
        source_event_id=key,
        source_context={"kind": "local-directory"},
        idempotency_key=key,
        archive_store=cast(Any, archive_store),
        use_cache=use_cache,
        copy_to=cast(Any, copy_to),
        description=description,
        tags=tag or (),
    )
    try:
        if producer.constraints is not None:
            for start in range(0, len(upload.sources), COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES):
                inputs = []
                expected = {}
                for source in upload.sources[
                    start : start + COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES
                ]:
                    observation = (
                        selected_observer.observe_native_file(
                            source.path,
                            host_id=source_host_id,
                            naming_view_id=upload.source_naming_view_id,
                        )
                        if selected_observer is not None and source_host_id is not None
                        else None
                    )
                    item = source.producer_file(observation=observation)
                    inputs.append(item)
                    expected[item.artifact_id] = ProducerArtifactIdentity(
                        item.artifact_id, source.bytes, source.sha256
                    )
                producer.append_inputs(inputs, expected_identities=expected)
        result = producer.finish()
    finally:
        producer.stop()
    emit(
        result.receipt if json_mode else format_collection_upload(result.receipt),
        json_mode=json_mode,
    )


_COLLECTION_UPLOAD_SORT_FIELDS = {"id", "created_at", "state", "bytes", "files"}


@collection_upload_app.command("list")
def upload_list_cmd(
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "created_at",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "desc",
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Search session ids or ingest sources"),
    ] = None,
    state: Annotated[
        str | None,
        typer.Option("--state", help="Restrict results to one exact session state"),
    ] = None,
    ids: Annotated[
        bool,
        typer.Option("--ids", help="Emit one collection/upload id per line"),
    ] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List collection upload sessions visible to the current application."""

    if ids and json_mode:
        raise typer.BadParameter("--ids and --json cannot be used together")
    normalized_order = _list_order(sort, order, fields=_COLLECTION_UPLOAD_SORT_FIELDS)
    api = client()
    payload = api.list_collection_upload_sessions(
        page_size=page_size,
        page_token=page_token,
        q=query,
        state=cast(Any, state),
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
    )
    if ids:
        emit(
            format_list_ids(payload, "uploads", id_key="collection_id"),
            json_mode=False,
        )
        return
    emit(payload if json_mode else format_collection_uploads(payload), json_mode=json_mode)


@collection_upload_app.command("show")
def upload_show_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection upload session id")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Show one collection upload session."""

    payload = client().get_collection_upload_session(collection_id)
    emit(payload if json_mode else format_collection_upload(payload), json_mode=json_mode)


@collection_upload_app.command("files")
def upload_files_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection upload session id")],
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List exact registered artifacts and their Riverhog custody state."""

    api = client()
    payload = api.list_collection_upload_session_files(
        collection_id,
        page_size=page_size,
        page_token=page_token,
    )
    emit(payload if json_mode else format_collection_upload_files(payload), json_mode=json_mode)


@collection_upload_app.command("cancel")
def upload_cancel_cmd(
    collection_id: Annotated[int, typer.Argument(help="Open collection upload session id")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Cancel an open collection upload session."""

    payload = client().cancel_collection_upload_session(collection_id)
    emit(payload if json_mode else format_collection_upload(payload), json_mode=json_mode)


@collection_upload_app.command("watch")
def upload_watch_cmd(
    collection_id: Annotated[
        int,
        typer.Argument(help="Collection upload/session id to monitor until finalized"),
    ],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Wait for collection finalization to finish."""

    deadline = time.monotonic() + 24 * 60 * 60
    payload = client().get_collection_upload_session(collection_id)
    while payload.get("state") != "finalized" and time.monotonic() < deadline:
        time.sleep(5.0)
        payload = client().get_collection_upload_session(collection_id)
    emit(payload if json_mode else format_collection_upload(payload), json_mode=json_mode)
    if payload.get("state") != "finalized":
        raise typer.Exit(124)


@collection_upload_app.command("discard")
def upload_discard_cmd(
    collection_id: Annotated[int, typer.Argument(help="Orphaned upload session id")],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "--plan",
            help="Show the custody-loss plan and confirmation challenge",
        ),
    ] = False,
    confirm: Annotated[
        str | None,
        typer.Option("--confirm", help="Short-lived challenge returned by a prior plan"),
    ] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Permanently discard one retained orphaned upload session."""

    if dry_run and confirm is not None:
        raise typer.BadParameter("--dry-run and --confirm cannot be used together")
    api = client()
    if dry_run:
        payload = api.plan_collection_upload_discard(collection_id)
        emit(
            payload if json_mode else format_collection_upload_discard_plan(payload),
            json_mode=json_mode,
        )
        return
    if confirm is not None:
        payload = api.discard_collection_upload(collection_id, challenge=confirm)
        emit(
            payload if json_mode else format_collection_upload_discard_result(payload),
            json_mode=json_mode,
        )
        return
    if json_mode:
        raise typer.BadParameter("--json requires --dry-run or --confirm")
    plan = api.plan_collection_upload_discard(collection_id)
    emit(format_collection_upload_discard_plan(plan), json_mode=False)
    blockers = plan.get("blockers")
    if isinstance(blockers, list) and blockers:
        raise typer.Exit(1)
    challenge = plan.get("challenge")
    if not isinstance(challenge, str) or not challenge:
        raise typer.BadParameter("server did not return an upload discard challenge")
    typed_id = typer.prompt("Type the complete upload session id to discard", type=int)
    if typed_id != collection_id:
        typer.echo("Upload session id did not match; nothing was discarded.", err=True)
        raise typer.Exit(1)
    payload = api.discard_collection_upload(collection_id, challenge=challenge)
    emit(format_collection_upload_discard_result(payload), json_mode=False)


@app.command("find")
def find_cmd(
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Substring match over collection file references"),
    ] = None,
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "file_ref",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "asc",
    collection: Annotated[
        int | None,
        typer.Option("--collection", help="Restrict results to one collection"),
    ] = None,
    selectors: Annotated[
        bool,
        typer.Option("--selectors", help="Emit one COLLECTION_ID::PATH selector per line"),
    ] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Search files across collections."""

    if selectors and json_mode:
        raise typer.BadParameter("--selectors and --json cannot be used together")
    if sort not in _FIND_SORT_FIELDS:
        raise typer.BadParameter(
            f"sort must be one of {', '.join(sorted(_FIND_SORT_FIELDS))}",
            param_hint="--sort",
        )
    normalized_order = order.casefold()
    if normalized_order not in {"asc", "desc"}:
        raise typer.BadParameter("order must be asc or desc", param_hint="--order")
    api = client()
    payload = api.search(
        query,
        page_size=page_size,
        page_token=page_token,
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
        collection=collection,
    )
    if selectors:
        emit(format_file_selectors(payload), json_mode=False)
        return
    emit(payload if json_mode else format_find(payload), json_mode=json_mode)


@collection_app.command("show")
def show_cmd(
    collection: Annotated[int, typer.Argument(help="Collection id")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Show collection storage and archive details."""

    payload = client().get_collection(collection)
    emit(payload if json_mode else format_collection_summary(payload), json_mode=json_mode)


@collection_app.command("describe")
def collection_describe_cmd(
    collection: Annotated[int, typer.Argument(help="Collection id")],
    description: Annotated[
        str | None,
        typer.Option("--description", help="Replacement human description"),
    ] = None,
    clear: Annotated[
        bool,
        typer.Option("--clear", help="Remove the current description"),
    ] = False,
    if_match: Annotated[
        str | None,
        typer.Option(
            "--if-match",
            help="Expected current description identity; fetched when omitted",
        ),
    ] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Conditionally replace or clear one collection description."""

    if (description is None) == (not clear):
        raise typer.BadParameter("provide exactly one of --description or --clear")
    api = client()
    expected_identity = if_match
    if expected_identity is None:
        current = api.get_collection(collection)
        value = current.get("description_identity")
        if not isinstance(value, str):
            raise RuntimeError("collection response omitted description identity")
        expected_identity = value
    payload = api.replace_collection_description(
        collection,
        None if clear else description,
        expected_identity=expected_identity,
    )
    emit(payload if json_mode else format_collection_description(payload), json_mode=json_mode)


_PROVENANCE_SORT_FIELDS = {"path", "bytes", "status"}


@collection_provenance_app.command("list")
def provenance_list_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "path",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "asc",
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Substring match over collection paths"),
    ] = None,
    status: Annotated[
        str | None,
        typer.Option("--status", help="Restrict to captured or omitted files"),
    ] = None,
    selectors: Annotated[
        bool,
        typer.Option("--selectors", help="Emit one COLLECTION_ID::PATH selector per line"),
    ] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List one bounded page of file provenance accounting."""

    if selectors and json_mode:
        raise typer.BadParameter("--selectors and --json cannot be used together")
    normalized_order = _list_order(sort, order, fields=_PROVENANCE_SORT_FIELDS)
    if status is not None and status not in {"captured", "omitted"}:
        raise typer.BadParameter("status must be captured or omitted", param_hint="--status")
    api = client()
    payload = api.list_collection_provenance(
        collection_id,
        page_size=page_size,
        page_token=page_token,
        q=query,
        status=cast(Any, status),
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
    )
    if selectors:
        emit(format_file_selectors(payload), json_mode=False)
        return
    emit(payload if json_mode else format_provenance_files(payload), json_mode=json_mode)


@collection_provenance_app.command("show")
def provenance_show_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    path: Annotated[str, typer.Argument(help="Collection-relative file path")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Show one file's current provenance binding and journal summary."""

    payload = client().get_collection_file_provenance(collection_id, path)
    emit(payload if json_mode else format_file_provenance(payload), json_mode=json_mode)


@collection_provenance_app.command("trace")
def provenance_trace_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    path: Annotated[str, typer.Argument(help="Collection-relative file path")],
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Trace one file through reachable provenance journals and references."""

    api = client()
    payload = api.trace_collection_file_provenance(
        collection_id,
        path,
        page_size=page_size,
        page_token=page_token,
    )
    emit(payload if json_mode else format_provenance_trace(payload), json_mode=json_mode)


@collection_provenance_app.command("agents")
def provenance_agents_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    journal_id: Annotated[str, typer.Argument(help="Exact provenance journal id")],
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List agents asserted by one exact provenance journal."""

    api = client()
    payload = api.list_collection_provenance_journal_agents(
        collection_id,
        journal_id,
        page_size=page_size,
        page_token=page_token,
    )
    emit(payload if json_mode else format_provenance_journal_agents(payload), json_mode=json_mode)


@collection_provenance_app.command("export")
def provenance_export_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    journal_id: Annotated[str, typer.Argument(help="Exact provenance journal id")],
    output: Annotated[Path, typer.Option("--output", "-o", help="Destination .json-seq file")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit a JSON export receipt")] = False,
) -> None:
    """Export one exact canonical RFC 7464 journal."""

    destination = output.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    byte_count, sha256 = client().download_collection_provenance_journal(
        collection_id,
        journal_id,
        output=destination,
    )
    if json_mode:
        emit(
            {
                "collection_id": collection_id,
                "journal_id": journal_id,
                "output": str(destination),
                "bytes": byte_count,
                "sha256": sha256,
            },
            json_mode=True,
        )
        return
    typer.echo(str(destination))


@collection_provenance_app.command("verify")
def provenance_verify_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    wait: Annotated[
        bool,
        typer.Option("--wait/--no-wait", help="Wait for the server-owned verification job"),
    ] = True,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Start verification of exact journals, bindings, identity, and projection."""

    api = client()
    payload = api.request_collection_provenance_verification(collection_id)
    while wait and payload["state"] in {"queued", "running", "canceling"}:
        time.sleep(0.25)
        payload = api.get_collection_provenance_verification(collection_id)
    emit(payload if json_mode else format_provenance_verification_job(payload), json_mode=json_mode)


@collection_provenance_app.command("verification-show")
def provenance_verification_show_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Show the current server-owned provenance verification job."""

    payload = client().get_collection_provenance_verification(collection_id)
    emit(payload if json_mode else format_provenance_verification_job(payload), json_mode=json_mode)


@collection_provenance_app.command("verification-cancel")
def provenance_verification_cancel_cmd(
    collection_id: Annotated[int, typer.Argument(help="Collection id")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Cancel the current server-owned provenance verification job."""

    payload = client().cancel_collection_provenance_verification(collection_id)
    emit(payload if json_mode else format_provenance_verification_job(payload), json_mode=json_mode)


_ARCHIVE_STORE_SORT_FIELDS = {
    "store",
    "read_mode",
    "read_priority",
    "collections",
    "objects",
    "stored_bytes",
}

_RETRIEVAL_CACHE_SORT_FIELDS = {
    "collection_id",
    "source_store",
    "object_id",
    "stored_bytes",
    "cached_at",
    "verified_at",
    "protected_until",
}


@retrieval_cache_app.command("status")
def retrieval_cache_status_cmd(
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Show retrieval-cache policy and current authorized totals."""

    payload = client().retrieval_cache_status()
    emit(payload if json_mode else format_retrieval_cache_status(payload), json_mode=json_mode)


@retrieval_cache_app.command("list")
def retrieval_cache_list_cmd(
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "cached_at",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "desc",
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Match collection, store, or object identity"),
    ] = None,
    collection_id: Annotated[
        int | None,
        typer.Option("--collection", min=1, help="Require one collection"),
    ] = None,
    source_store: Annotated[
        str | None,
        typer.Option("--source-store", help="Require one archive source store"),
    ] = None,
    cache_store: Annotated[
        str | None,
        typer.Option("--cache-store", help="Require one retrieval-cache store"),
    ] = None,
    state: Annotated[
        str | None,
        typer.Option("--state", help="Require ready, delete_pending, or deleting state"),
    ] = None,
    protection: Annotated[
        str | None,
        typer.Option("--protection", help="Require protected or unleased objects"),
    ] = None,
    expires_before: Annotated[
        str | None,
        typer.Option("--expires-before", help="Require protection expiry at or before timestamp"),
    ] = None,
    expires_after: Annotated[
        str | None,
        typer.Option("--expires-after", help="Require protection expiry at or after timestamp"),
    ] = None,
    selectors: Annotated[
        bool,
        typer.Option(
            "--selectors",
            help="Emit one COLLECTION_ID::SOURCE_STORE::OBJECT_ID selector per line",
        ),
    ] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List cache availability visible through authorized collection access."""

    if selectors and json_mode:
        raise typer.BadParameter("--selectors and --json cannot be used together")
    normalized_order = _list_order(sort, order, fields=_RETRIEVAL_CACHE_SORT_FIELDS)
    api = client()
    payload = api.list_retrieval_cache_objects(
        page_size=page_size,
        page_token=page_token,
        q=query,
        collection_id=collection_id,
        source_store=source_store,
        cache_store=cast(Any, cache_store),
        state=cast(Any, state),
        protection=cast(Any, protection),
        expires_before=expires_before,
        expires_after=expires_after,
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
    )
    if selectors:
        emit(format_retrieval_cache_selectors(payload), json_mode=False)
        return
    emit(payload if json_mode else format_retrieval_cache_objects(payload), json_mode=json_mode)


@retrieval_cache_app.command("show")
def retrieval_cache_show_cmd(
    selector: Annotated[
        str,
        typer.Argument(help="COLLECTION_ID::SOURCE_STORE::OBJECT_ID"),
    ],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Show one cached encrypted archive object and its lease protection."""

    collection_id, source_store, object_id = _retrieval_cache_selector(selector)
    payload = client().get_retrieval_cache_object(collection_id, source_store, object_id)
    emit(payload if json_mode else format_retrieval_cache_object(payload), json_mode=json_mode)


@archive_store_app.command("list")
def archive_store_list_cmd(
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "store",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "asc",
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Substring match over store configuration"),
    ] = None,
    ids: Annotated[
        bool,
        typer.Option("--ids", help="Emit one archive store name per line"),
    ] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List configured archive stores and their current storage totals."""

    if ids and json_mode:
        raise typer.BadParameter("--ids and --json cannot be used together")
    normalized_order = _list_order(sort, order, fields=_ARCHIVE_STORE_SORT_FIELDS)
    api = client()
    payload = api.list_archive_stores(
        page_size=page_size,
        page_token=page_token,
        q=query,
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
    )
    if ids:
        emit(format_list_ids(payload, "stores", id_key="store"), json_mode=False)
        return
    emit(payload if json_mode else format_archive_stores(payload), json_mode=json_mode)


@archive_store_app.command("show")
def archive_store_show_cmd(
    store: Annotated[str, typer.Argument(help="Archive store name")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Show one archive store and its current storage totals."""

    payload = client().get_archive_store(store)
    emit(payload if json_mode else format_archive_store(payload), json_mode=json_mode)


@collection_app.command("delete")
def collection_delete_cmd(
    collection_id: Annotated[int, typer.Argument(help="Exact accepted collection id")],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "--plan",
            help="Show the deletion plan and confirmation challenge without deleting",
        ),
    ] = False,
    confirm: Annotated[
        str | None,
        typer.Option(
            "--confirm",
            help="Short-lived confirmation challenge returned by a prior plan",
        ),
    ] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Begin permanent deletion of one accepted collection and its archive."""

    if dry_run and confirm is not None:
        raise typer.BadParameter("--dry-run and --confirm cannot be used together")
    api = client()
    if dry_run:
        payload = api.plan_collection_deletion(collection_id)
        emit(
            payload if json_mode else format_collection_deletion_plan(payload),
            json_mode=json_mode,
        )
        return
    if confirm is not None:
        payload = api.delete_collection(collection_id, challenge=confirm)
        emit(
            payload if json_mode else format_collection_deletion_result(payload),
            json_mode=json_mode,
        )
        return
    if json_mode:
        raise typer.BadParameter("--json requires --dry-run or --confirm")

    plan = api.plan_collection_deletion(collection_id)
    emit(format_collection_deletion_plan(plan), json_mode=False)
    blockers = plan.get("blockers")
    if isinstance(blockers, list) and blockers:
        raise typer.Exit(1)
    challenge = plan.get("challenge")
    if not isinstance(challenge, str) or not challenge:
        raise typer.BadParameter("server did not return a collection deletion challenge")
    typed_id = typer.prompt("Type the complete collection id to delete", type=int)
    if typed_id != collection_id:
        typer.echo("Collection id did not match; nothing was deleted.", err=True)
        raise typer.Exit(1)
    payload = api.delete_collection(collection_id, challenge=challenge)
    emit(format_collection_deletion_result(payload), json_mode=False)


@archive_copy_app.command("start")
def archive_copy_cmd(
    collection_id: Annotated[int, typer.Argument(help="Exact collection id")],
    destination_store: Annotated[
        str,
        typer.Option("--to", help="Destination archive store"),
    ],
    source_store: Annotated[
        str | None,
        typer.Option("--from", help="Source archive store; chosen automatically when omitted"),
    ] = None,
    use_cache: Annotated[
        bool | None,
        typer.Option("--use-cache/--no-use-cache", help="Override retrieval-cache placement"),
    ] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Copy one collection between archive stores."""

    payload = client().create_or_resume_archive_copy_job(
        collection_id,
        destination_store=destination_store,
        source_store=source_store,
        use_cache=use_cache,
    )
    emit(payload if json_mode else format_archive_copy_job(payload), json_mode=json_mode)


@archive_copy_app.command("intents")
def archive_copy_intents_cmd(
    collection_id: Annotated[int, typer.Argument(help="Published collection id")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Inspect the upload's durable copy choices and handoff receipts."""

    payload = client().get_upload_copy_intents(collection_id)
    if json_mode:
        emit(payload, json_mode=True)
        return
    lines = [
        f"collection {payload['collection_id']} upload copy intents",
        f"archive store: {payload['archive_store']}",
        f"use cache: {'yes' if payload['use_cache'] else 'no'}",
    ]
    for intent in payload["intents"]:
        line = f"- {intent['destination_store']}: {intent['state']}"
        if intent.get("job_state"):
            line += f" (job {intent['job_state']})"
        if intent.get("failure_code"):
            line += f" ({intent['failure_code']})"
        lines.append(line)
    emit("\n".join(lines), json_mode=False)


_ARCHIVE_COPY_JOB_SORT_FIELDS = {
    "collection_id",
    "source_store",
    "destination_store",
    "state",
    "requested_at",
}


@archive_copy_app.command("list")
def archive_copy_list_cmd(
    page_size: Annotated[int, typer.Option("--page-size", min=1, max=100)] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "requested_at",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "desc",
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Substring match over archive-copy jobs"),
    ] = None,
    state: Annotated[
        str | None,
        typer.Option("--state", help="Exact archive-copy job state"),
    ] = None,
    selectors: Annotated[
        bool,
        typer.Option(
            "--selectors",
            help="Emit one COLLECTION_ID::DESTINATION_STORE selector per line",
        ),
    ] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """List current and terminal archive-copy jobs."""

    if selectors and json_mode:
        raise typer.BadParameter("--selectors and --json cannot be used together")
    normalized_order = _list_order(sort, order, fields=_ARCHIVE_COPY_JOB_SORT_FIELDS)
    api = client()
    payload = api.list_archive_copy_jobs(
        page_size=page_size,
        page_token=page_token,
        q=query,
        state=cast(Any, state),
        sort=cast(Any, sort),
        order=cast(Any, normalized_order),
    )
    if selectors:
        emit(format_archive_copy_selectors(payload), json_mode=False)
        return
    emit(payload if json_mode else format_archive_copy_jobs(payload), json_mode=json_mode)


@archive_copy_app.command("show")
def archive_copy_show_cmd(
    selector: Annotated[
        str,
        typer.Argument(help="COLLECTION_ID::DESTINATION_STORE"),
    ],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Show one archive-copy job."""

    collection_id, destination_store = _archive_copy_selector(selector)
    payload = client().get_archive_copy_job(
        collection_id,
        destination_store=destination_store,
    )
    emit(payload if json_mode else format_archive_copy_job(payload), json_mode=json_mode)


@archive_copy_app.command("cancel")
def archive_copy_cancel_cmd(
    selector: Annotated[
        str,
        typer.Argument(help="COLLECTION_ID::DESTINATION_STORE"),
    ],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Cancel one active archive-copy job."""

    collection_id, destination_store = _archive_copy_selector(selector)
    payload = client().cancel_archive_copy_job(
        collection_id,
        destination_store=destination_store,
    )
    emit(payload if json_mode else format_archive_copy_job(payload), json_mode=json_mode)


@archive_copy_app.command("watch")
def archive_copy_watch_cmd(
    selector: Annotated[
        str,
        typer.Argument(help="COLLECTION_ID::DESTINATION_STORE"),
    ],
    interval: Annotated[
        float,
        typer.Option("--interval", min=0.1, help="Polling interval in seconds"),
    ] = 1.0,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Wait for one archive-copy job to finish."""

    collection_id, destination_store = _archive_copy_selector(selector)
    api = client()
    while True:
        payload = api.get_archive_copy_job(
            collection_id,
            destination_store=destination_store,
        )
        state = str(payload.get("state", ""))
        if state in {"completed", "failed", "canceled"}:
            emit(payload if json_mode else format_archive_copy_job(payload), json_mode=json_mode)
            if state != "completed":
                raise typer.Exit(1)
            return
        time.sleep(interval)


@archive_app.command("retire")
def archive_retire_cmd(
    collection_id: Annotated[int, typer.Argument(help="Exact collection id")],
    store: Annotated[
        str,
        typer.Option("--store", help="Archive store whose copy will be permanently deleted"),
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "--plan",
            help="Show the retirement plan and confirmation challenge without deleting",
        ),
    ] = False,
    confirm: Annotated[
        str | None,
        typer.Option(
            "--confirm",
            help="Short-lived confirmation challenge returned by a prior plan",
        ),
    ] = None,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Retire one archive copy by deleting it after another passes verification."""

    if dry_run and confirm is not None:
        raise typer.BadParameter("--dry-run and --confirm cannot be used together")
    api = client()
    if dry_run:
        payload = api.plan_archive_copy_retirement(collection_id, store=store)
        emit(
            payload if json_mode else format_archive_copy_retirement_plan(payload),
            json_mode=json_mode,
        )
        return
    if confirm is not None:
        payload = api.retire_archive_copy(
            collection_id,
            store=store,
            challenge=confirm,
        )
        emit(
            payload if json_mode else format_archive_copy_retirement_result(payload),
            json_mode=json_mode,
        )
        return
    if json_mode:
        raise typer.BadParameter("--json requires --dry-run or --confirm")

    plan = api.plan_archive_copy_retirement(collection_id, store=store)
    emit(format_archive_copy_retirement_plan(plan), json_mode=False)
    blockers = plan.get("blockers")
    if isinstance(blockers, list) and blockers:
        raise typer.Exit(1)
    challenge = plan.get("challenge")
    if not isinstance(challenge, str) or not challenge:
        raise typer.BadParameter("server did not return an archive copy retirement challenge")
    typed_id = typer.prompt(
        "Type the complete collection id whose copy will be deleted from this store",
        type=int,
    )
    if typed_id != collection_id:
        typer.echo("Collection id did not match; no archive copy was deleted.", err=True)
        raise typer.Exit(1)
    typed_store = typer.prompt("Type the archive store whose copy will be deleted")
    if typed_store != store:
        typer.echo("Archive store did not match; no archive copy was deleted.", err=True)
        raise typer.Exit(1)
    payload = api.retire_archive_copy(
        collection_id,
        store=store,
        challenge=challenge,
    )
    emit(format_archive_copy_retirement_result(payload), json_mode=False)


def _error_code(exc: BaseException) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        if exc.response.status_code >= 500:
            return "service_unavailable"
        return "http_error"
    if isinstance(exc, httpx.TransportError):
        return "transport_error"
    if isinstance(exc, RiverhogError):
        return exc.code
    return "error"


def _error_message(exc: BaseException) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"service returned HTTP {exc.response.status_code}"
    return str(exc) or type(exc).__name__


def _json_requested(argv: list[str]) -> bool:
    return "--json" in argv


def _emit_cli_error(exc: BaseException, *, json_mode: bool) -> None:
    code = _error_code(exc)
    message = _error_message(exc)
    if json_mode:
        details = exc.details if isinstance(exc, RiverhogError) else None
        typer.echo(
            json.dumps(
                error_document(code=code, message=message, details=details),
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return
    if isinstance(exc, httpx.TransportError):
        typer.echo(f"a-riverhog-cli: transport error: {message}", err=True)
        return
    typer.echo(f"a-riverhog-cli: {message}", err=True)


def main() -> int:
    try:
        app()
    except (
        httpx.HTTPStatusError,
        httpx.TransportError,
        RiverhogError,
        FileExistsError,
        FileNotFoundError,
        NotADirectoryError,
    ) as exc:
        _emit_cli_error(exc, json_mode=_json_requested(sys.argv[1:]))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
