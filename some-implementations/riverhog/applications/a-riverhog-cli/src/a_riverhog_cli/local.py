from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Annotated, Any, cast

import typer
from http_api_contracts import BrowseTokenCodec, BrowseTokenError
from riverhog_archive_contracts import (
    PAGE_BYTES_MAX,
    RETAINED_HISTORY_EXTENT,
    MemberHistoryBinding,
    MemberHistoryStore,
    SourceMemberHistoryBindingProof,
    provenance_structure_identity,
    provenance_structure_object_id,
    provenance_structure_object_path,
    read_bounded_history_object,
)
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
from riverhog_client import (
    ApiClient,
    RestorePolicy,
    RetrievalDownload,
    configured_download_concurrency,
    configured_download_window,
    download_retrieval_files,
)
from riverhog_materialization import (
    DestinationRules,
    MemberAdvice,
    plan_materialization,
    primary_sidecar_components,
    shared_journal_components,
)
from riverhog_protocol import (
    ArtifactId,
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
    PortableCollectionArtifact,
    PortableCollectionIdentityBuilder,
    validate_collection_tag,
)
from riverhog_protocol.collection_production_provenance import COLLECTION_MEMBER_ROLE
from riverhog_protocol.errors import InvalidState, NotFound
from riverhog_protocol.paths import normalize_collection_id
from riverhog_protocol.provenance_transport import MaterializationHintDocument
from riverhog_protocol.transport import RETRIEVAL_ARTIFACT_BATCH_MAX
from riverhog_provenance import (
    MemberHistoryClosure,
    list_provenance_observers,
    resolve_provenance_observer,
    selected_delivery_occurrence,
    validate_journal_chunks,
)
from state_schema import StateSchemaError

from a_riverhog_cli.cli_support import emit, format_list_ids
from a_riverhog_cli.local_state import state_schema as local_state_schema
from a_riverhog_cli.output import format_local_collection, format_local_collections

local_app = typer.Typer(
    no_args_is_help=True,
    help="Maintain selected archive collections in a local directory.",
)
local_state_app = typer.Typer(no_args_is_help=True, help="Manage local durable state.")
local_provenance_observer_app = typer.Typer(
    no_args_is_help=True,
    help="Inspect explicitly composable local provenance observers.",
)
local_app.add_typer(local_state_app, name="state")
local_app.add_typer(local_provenance_observer_app, name="provenance-observer")

LOCAL_LIST_PAGE_SIZE_MAX = 100
LOCAL_LIST_TOKEN_LIFETIME_SECONDS = 24 * 60 * 60
LOCAL_LIST_SORT_FIELDS = {
    "bytes": "bytes",
    "collection_id": "collection_id",
    "created_at": "created_at",
    "files": "files",
    "status": "status",
}
LOCAL_AUDIT_SAMPLE_LIMIT = 100
LOCAL_CATALOG_RECONCILE_BATCH = 100
RETRIEVAL_RENEW_INTERVAL_MAX_SECONDS = 60 * 60


@local_provenance_observer_app.command("list")
def provenance_observer_list(
    ids: Annotated[
        bool,
        typer.Option("--ids", help="Emit one provider name per line."),
    ] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    """List installed observer metadata without executing provider code."""

    if ids and json_mode:
        raise typer.BadParameter("--ids and --json cannot be used together")
    providers = [item.as_dict() for item in list_provenance_observers()]
    payload = {
        "format": "riverhog-provenance-observer-provider-list/v1",
        "providers": providers,
    }
    human = [f"provenance observers: {len(providers)}"]
    human.extend(
        f"- {item['name']}  distribution={item['distribution'] or 'unknown'}  "
        f"version={item['version'] or 'unknown'}"
        for item in providers
    )
    if ids:
        emit(format_list_ids(payload, "providers", id_key="name"), json_mode=False)
        return
    emit(payload if json_mode else "\n".join(human), json_mode=json_mode)


@local_provenance_observer_app.command("show")
def provenance_observer_show(
    name: Annotated[str, typer.Argument(help="Exact installed observer provider name")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    """Load one selected provider and show its exact observer/contract identity."""

    try:
        resolved = resolve_provenance_observer(name)
        payload = resolved.as_dict()
    except (TypeError, ValueError) as exc:
        raise typer.BadParameter(str(exc), param_hint="name") from exc
    human = "\n".join(
        (
            f"provenance observer {payload['name']}",
            f"observer: {payload['observer_id']}",
            f"contract provider: {payload['contract_provider']}",
            f"contract: {payload['contract_id']}",
            f"contract sha256: {payload['contract_sha256']}",
            f"schemas: {len(resolved.contract.schemas)}",
        )
    )
    emit(payload if json_mode else human, json_mode=json_mode)


def _target(*, create: bool = True) -> Path:
    raw = os.getenv("A_RIVERHOG_CLI_LOCAL_ROOT", "").strip()
    if not raw:
        raise typer.BadParameter("A_RIVERHOG_CLI_LOCAL_ROOT is required")
    target = Path(raw).expanduser().resolve()
    if create:
        target.mkdir(parents=True, exist_ok=True)
    return target


def _database(target: Path) -> Path:
    raw = os.getenv("A_RIVERHOG_CLI_LOCAL_DATABASE", "").strip()
    return Path(raw).expanduser().resolve() if raw else target / ".a-riverhog-cli.sqlite3"


def _catalog_database(target: Path) -> Path:
    database = _database(target)
    return database.with_name(database.name + ".catalog")


def _connect(target: Path) -> sqlite3.Connection:
    database = _database(target)
    local_state_schema(database).validate()
    db = sqlite3.connect(f"{database.as_uri()}?mode=rw", uri=True)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def _state_command(command: str, *, json_mode: bool) -> None:
    target = _target(create=command == "upgrade")
    schema = local_state_schema(_database(target))
    try:
        if command == "status":
            status = schema.status()
        elif command == "upgrade":
            status = schema.upgrade()
        else:
            status = schema.validate()
    except StateSchemaError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    payload = status.as_dict()
    emit(
        payload
        if json_mode
        else (
            f"a-riverhog-cli local state: {payload['condition']} "
            f"({payload['current_revision'] or 'none'} -> {payload['head_revision']})"
        ),
        json_mode=json_mode,
    )


@local_state_app.command("status")
def state_status(
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    """Show the current and required local-state revisions."""

    _state_command("status", json_mode=json_mode)


@local_state_app.command("upgrade")
def state_upgrade(
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    """Explicitly upgrade local state to the current revision."""

    _state_command("upgrade", json_mode=json_mode)


@local_state_app.command("verify")
def state_verify(
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    """Verify the current revision and exact local-state schema."""

    _state_command("verify", json_mode=json_mode)


def _collection_directory(target: Path, collection_id: int) -> Path:
    directory = target / str(normalize_collection_id(collection_id))
    if directory.is_symlink():
        raise InvalidState("local collection directory is a symbolic link")
    directory.mkdir(mode=0o700, exist_ok=True)
    return directory


def _output(target: Path, collection_id: int, components: Sequence[str]) -> Path:
    if not components or any(
        not isinstance(part, str)
        or not part
        or part in {".", ".."}
        or "/" in part
        or "\\" in part
        or "\x00" in part
        for part in components
    ):
        raise InvalidState("local materialization mapping contains an unsafe component")
    output = _collection_directory(target, collection_id)
    for component in components[:-1]:
        output /= component
        if output.is_symlink():
            raise InvalidState("local materialization parent is a symbolic link")
        output.mkdir(mode=0o700, exist_ok=True)
    return output / components[-1]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _matches(path: Path, byte_count: int, sha256: str) -> bool:
    return (
        path.is_file()
        and not path.is_symlink()
        and path.stat().st_size == byte_count
        and _sha256(path) == sha256
    )


def _publish_file(source: Path, output: Path, *, byte_count: int, sha256: str) -> None:
    if not _matches(source, byte_count, sha256):
        raise InvalidState("staged materialization differs from its exact identity")
    try:
        os.link(source, output)
    except FileExistsError as exc:
        if not _matches(output, byte_count, sha256):
            raise InvalidState(f"local file would be overwritten: {output}") from exc
    if os.name != "nt":
        descriptor = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _prefix_chunks(path: Path, byte_count: int) -> Iterator[bytes]:
    with path.open("rb") as source:
        remaining = byte_count
        while remaining:
            chunk = source.read(min(8 * 1024 * 1024, remaining))
            if not chunk:
                raise InvalidState("canonical primary journal prefix is incomplete")
            remaining -= len(chunk)
            yield chunk


def _publish_prefix(source: Path, output: Path, *, byte_count: int, sha256: str) -> None:
    if _matches(output, byte_count, sha256):
        return
    if output.exists() or output.is_symlink():
        raise InvalidState(f"local primary provenance would be overwritten: {output}")
    with tempfile.NamedTemporaryFile(
        mode="wb", prefix=".primary-", dir=output.parent, delete=False
    ) as stream:
        staging = Path(stream.name)
        digest = hashlib.sha256()
        try:
            for chunk in _prefix_chunks(source, byte_count):
                stream.write(chunk)
                digest.update(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            staging.unlink(missing_ok=True)
            raise
    try:
        if digest.hexdigest() != sha256:
            raise InvalidState("canonical primary journal prefix differs from its anchor")
        _publish_file(staging, output, byte_count=byte_count, sha256=sha256)
    finally:
        staging.unlink(missing_ok=True)


def _windows_path_budget(probe: Path) -> int:
    """Qualify the process/filesystem's native long-path behavior."""
    destination = probe.joinpath(*("path" * 20 for _ in range(4)))
    try:
        destination.mkdir(parents=True)
        (destination / "leaf").write_bytes(b"x")
    except OSError as exc:
        if getattr(exc, "winerror", None) != 206:
            raise
        return 240
    return 32767


def _destination_rules(target: Path) -> DestinationRules:
    with tempfile.TemporaryDirectory(prefix=".rules-", dir=target) as name:
        probe = Path(name)
        (probe / "case").write_bytes(b"x")
        case_sensitive = not (probe / "CASE").exists()
        (probe / "e\u0301").write_bytes(b"x")
        unicode_equivalence = "NFC" if (probe / "\u00e9").exists() else "exact"
        fallback_path_bytes = _windows_path_budget(probe) if os.name == "nt" else 4096
    try:
        component_bytes = os.pathconf(target, "PC_NAME_MAX")
        relative_path_bytes = os.pathconf(target, "PC_PATH_MAX")
    except (AttributeError, OSError, ValueError):
        component_bytes = 255
        relative_path_bytes = fallback_path_bytes
    if component_bytes < 1:
        component_bytes = 255
    if relative_path_bytes < 1:
        relative_path_bytes = fallback_path_bytes
    # Paths are created through this absolute collection directory. PATH_MAX
    # includes its prefix, the joining separator and the terminating NUL.
    relative_path_bytes -= len(os.fsencode(target.absolute())) + 2
    if relative_path_bytes < 1:
        raise InvalidState("local destination cannot represent a materialization path")
    return DestinationRules(
        windows_names=os.name == "nt",
        case_sensitive=case_sensitive,
        unicode_equivalence=cast(Any, unicode_equivalence),
        component_bytes=component_bytes,
        relative_path_bytes=relative_path_bytes,
    )


def _inventory(
    api: ApiClient, collection_id: int, summary: Mapping[str, Any]
) -> tuple[str, str, list[PortableCollectionArtifact]]:
    cursor: str | None = None
    identity: str | None = None
    artifacts: list[PortableCollectionArtifact] = []
    while True:
        page = api.get_portable_collection_inventory(
            collection_id,
            cursor=cursor,
            limit=1000,
            inventory_identity=identity,
        )
        authority = page.authority
        if authority.header.collection != collection_id:
            raise InvalidState("portable inventory names another collection")
        if authority.header.artifact_set_identity != summary["artifact_set_identity"]:
            raise InvalidState("portable inventory artifact set differs from collection")
        if identity is None:
            identity = authority.inventory_identity
        elif authority.inventory_identity != identity:
            raise InvalidState("portable inventory changed during traversal")
        artifacts.extend(
            PortableCollectionArtifact.from_mapping(item.model_dump(mode="json"))
            for item in page.artifacts
        )
        if page.complete:
            if len(artifacts) != int(authority.artifact_count):
                raise InvalidState("portable inventory count differs from complete traversal")
            builder = PortableCollectionIdentityBuilder(authority.header)
            for item in artifacts:
                builder.add(item)
            if builder.identity != identity:
                raise InvalidState("portable inventory identity differs from its artifacts")
            return identity, authority.header.provenance_identity, artifacts
        cursor = page.next_cursor
        if cursor is None:
            raise InvalidState("portable inventory continuation is missing")


def _journals(
    api: ApiClient,
    target: Path,
    collection_id: int,
    archive_root: str,
    *,
    repair: bool = False,
) -> dict[str, tuple[Path, int, str]]:
    result: dict[str, tuple[Path, int, str]] = {}
    after: str | None = None
    while True:
        page = api.list_collection_provenance_journals(
            collection_id,
            page_size=200,
            after_journal_id=after,
            archive_root_sha256=archive_root if after is not None else None,
        )
        if page.get("archive_root_sha256") != archive_root:
            raise InvalidState("canonical journal corpus names another archive root")
        journals = page.get("journals")
        if not isinstance(journals, list):
            raise InvalidState("canonical journal page is malformed")
        for row in journals:
            journal_id = str(row["journal_id"])
            byte_count = int(row["bytes"])
            sha256 = str(row["sha256"])
            if journal_id in result:
                raise InvalidState("canonical journal corpus repeats an identity")
            output = _output(target, collection_id, shared_journal_components(sha256))
            if not _matches(output, byte_count, sha256):
                if output.exists() or output.is_symlink():
                    if not repair:
                        raise InvalidState("local canonical journal differs from archive")
                    _quarantine(target, output, collection_id=collection_id)
                with tempfile.TemporaryDirectory(prefix=".journal-", dir=output.parent) as name:
                    staging = Path(name) / "journal.jsonseq"
                    downloaded = api.download_collection_provenance_journal(
                        collection_id, journal_id, output=staging
                    )
                    if downloaded != (byte_count, sha256):
                        raise InvalidState("downloaded journal differs from frozen corpus")
                    _publish_file(staging, output, byte_count=byte_count, sha256=sha256)
            result[journal_id] = (output, byte_count, sha256)
        after = page.get("next_journal_id")
        if after is None:
            break
        if not isinstance(after, str) or not journals:
            raise InvalidState("canonical journal traversal did not advance")
    return result


def _selected_hint(
    api: ApiClient,
    target: Path,
    collection_id: int,
    archive_root: str,
    artifact: PortableCollectionArtifact,
    journals: Mapping[str, tuple[Path, int, str]],
    *,
    db: sqlite3.Connection,
    closure: MemberHistoryClosure,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    detail = api.get_collection_artifact_provenance(collection_id, artifact.artifact_id)
    if detail.get("archive_root_sha256") != archive_root or detail.get(
        "artifact"
    ) != ArtifactMemberIdentityDocument.model_validate(
        {
            "artifact_id": artifact.artifact_id,
            "bytes": str(artifact.bytes),
            "sha256": artifact.sha256,
        }
    ).model_dump(mode="json"):
        raise InvalidState("primary provenance detail differs from selected artifact")
    binding = CollectionArtifactProvenanceBindingDocument.model_validate(detail["binding"])
    if binding.artifact_id != artifact.artifact_id:
        raise InvalidState("primary provenance binding names another artifact")
    _freeze_member_history(
        db, api, target, collection_id, archive_root, artifact, binding, detail, closure
    )
    journal = journals.get(binding.journal.journal_id)
    if journal is None:
        raise InvalidState("primary provenance journal is absent from corpus")
    journal_path, journal_bytes, _ = journal
    prefix_bytes = int(binding.journal.prefix_bytes)
    if prefix_bytes > journal_bytes:
        raise InvalidState("primary provenance prefix exceeds its journal")
    summary = validate_journal_chunks(
        _prefix_chunks(journal_path, prefix_bytes),
        expected_anchor=binding.journal.model_dump(mode="json"),
        require_exact_tail=True,
        require_profiles=False,
    )
    _, occurrence = selected_delivery_occurrence(
        summary,
        binding=binding.model_dump(mode="json"),
        artifact_id=artifact.artifact_id,
        byte_count=artifact.bytes,
        sha256=artifact.sha256,
        member_role=COLLECTION_MEMBER_ROLE,
    )
    hint = occurrence.get("materialization_hint")
    if hint is not None:
        hint = MaterializationHintDocument.model_validate(hint).model_dump(mode="json")
    sidecar = _output(target, collection_id, primary_sidecar_components(artifact.artifact_id))
    _publish_prefix(
        journal_path,
        sidecar,
        byte_count=prefix_bytes,
        sha256=binding.journal.prefix_sha256,
    )
    return hint, binding.model_dump(mode="json")


def _publish_provenance_blob(
    target: Path, collection_id: int, components: Sequence[str], content: bytes, *, repair: bool
) -> None:
    output = _output(target, collection_id, components)
    digest = hashlib.sha256(content).hexdigest()
    if _matches(output, len(content), digest):
        return
    if output.exists() or output.is_symlink():
        if not repair:
            raise InvalidState("local member history was modified")
        _quarantine(target, output, collection_id=collection_id)
    staged = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix=".history-", dir=output.parent, delete=False
        ) as stream:
            staged = Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        _publish_file(staged, output, byte_count=len(content), sha256=digest)
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)


def _structure_components(object_id: str) -> tuple[str, ...]:
    return (
        "structure",
        *provenance_structure_object_path(object_id).removesuffix(".age").split("/"),
    )


def _history_file_components(artifact_id: str, suffix: str) -> tuple[str, ...]:
    return ("provenance", "history", artifact_id[:2], artifact_id + suffix)


def _history_control_files(
    binding: MemberHistoryBinding, proof: SourceMemberHistoryBindingProof
) -> Iterator[tuple[tuple[str, ...], bytes]]:
    yield (
        _history_file_components(binding.artifact_id, ".binding-proof.json"),
        proof.to_json_bytes(),
    )
    yield (
        _history_file_components(binding.artifact_id, ".selection.json"),
        canonical_json_bytes(
            {
                "history_binding": binding.to_mapping(),
                "extent": RETAINED_HISTORY_EXTENT,
                "binding_proof_sha256": proof.identity,
            }
        ),
    )


def _blob_chunks(path: Path) -> Iterator[bytes]:
    with path.open("rb") as source:
        while chunk := source.read(64 * 1024):
            yield chunk


@contextmanager
def _history_closure(
    db: sqlite3.Connection,
    api: ApiClient | None,
    target: Path,
    collection_id: int,
    archive_root: str,
    journals: Mapping[str, tuple[Path, int, str]] | None = None,
    *,
    repair: bool = False,
    freezing: bool = False,
) -> Iterator[MemberHistoryClosure]:
    def read_structure(path: str) -> Iterator[bytes]:
        object_id = provenance_structure_object_id(path)
        output = _output(target, collection_id, _structure_components(object_id))
        frozen = (
            None
            if freezing
            else db.execute(
                "SELECT bytes, sha256 FROM desired_history_objects "
                "WHERE collection_id = ? AND object_id = ?",
                (collection_id, object_id),
            ).fetchone()
        )
        if not freezing and frozen is None:
            raise InvalidState("member history needs structure outside its frozen closure")
        if (
            output.is_file()
            and not output.is_symlink()
            and (frozen is None or _matches(output, int(frozen["bytes"]), str(frozen["sha256"])))
        ):
            raw = read_bounded_history_object(_blob_chunks(output), PAGE_BYTES_MAX)
            if provenance_structure_identity(raw).relative_path != path:
                raise InvalidState("local history structure differs from its identity")
        else:
            if api is None:
                raise InvalidState("local member history structure is missing or modified")
            if (output.exists() or output.is_symlink()) and not repair:
                raise InvalidState("local member history structure was modified")
            raw = api.get_collection_provenance_structure(
                collection_id, object_id, archive_root_sha256=archive_root
            )
            if provenance_structure_identity(raw).relative_path != path or (
                frozen is not None
                and (len(raw), hashlib.sha256(raw).hexdigest())
                != (int(frozen["bytes"]), str(frozen["sha256"]))
            ):
                raise InvalidState("remote history structure differs from the frozen selection")
            _publish_provenance_blob(
                target, collection_id, _structure_components(object_id), raw, repair=repair
            )
        yield raw

    def read_journal(journal_id: str, end: int | None) -> Iterator[bytes]:
        if journals is not None:
            selected = journals.get(journal_id)
            if selected is None:
                raise InvalidState("member history needs a missing journal")
            path, count, _digest = selected
        else:
            selected_row = db.execute(
                "SELECT bytes, sha256 FROM desired_journals "
                "WHERE collection_id = ? AND journal_id = ?",
                (collection_id, journal_id),
            ).fetchone()
            if selected_row is None:
                raise InvalidState("member history needs a missing journal")
            path = _output(
                target, collection_id, shared_journal_components(str(selected_row["sha256"]))
            )
            count = int(selected_row["bytes"])
        yield from _prefix_chunks(path, count if end is None else end)

    with MemberHistoryClosure(
        MemberHistoryStore(read_structure), read_journal, member_role=COLLECTION_MEMBER_ROLE
    ) as closure:
        yield closure


def _freeze_member_history(
    db: sqlite3.Connection,
    api: ApiClient,
    target: Path,
    collection_id: int,
    archive_root: str,
    artifact: PortableCollectionArtifact,
    primary: CollectionArtifactProvenanceBindingDocument,
    detail: Mapping[str, Any],
    closure: MemberHistoryClosure,
) -> None:
    try:
        binding = MemberHistoryBinding.from_mapping(detail.get("history_binding"))
        history = binding.verify_descriptor(canonical_json_bytes(detail.get("member_history")))
        if (
            (binding.artifact_id, binding.bytes, binding.sha256)
            != (artifact.artifact_id, artifact.bytes, artifact.sha256)
            or history.primary.journal.to_mapping() != primary.journal.model_dump(mode="json")
            or history.primary.delivery_association_id != primary.delivery_association_id
        ):
            raise ValueError("member history differs from the selected member and primary")
        proof = api.get_collection_artifact_history_binding_proof(
            collection_id, artifact.artifact_id, archive_root_sha256=archive_root
        )
        if (proof.binding, proof.collection_id, hashlib.sha256(proof.archive_root).hexdigest()) != (
            binding,
            collection_id,
            archive_root,
        ):
            raise ValueError("history proof differs from the selected root/member")
        closure.resolve(binding, extent=RETAINED_HISTORY_EXTENT)
    except (TypeError, ValueError) as exc:
        raise InvalidState(f"final member history is missing or invalid: {exc}") from exc
    _publish_provenance_blob(
        target,
        collection_id,
        _history_file_components(binding.artifact_id, ".json"),
        history.to_json_bytes(),
        repair=False,
    )
    for components, raw in _history_control_files(binding, proof):
        _publish_provenance_blob(target, collection_id, components, raw, repair=False)
    db.execute(
        "INSERT INTO pending_member_history VALUES (?, ?, ?, ?)",
        (
            binding.artifact_id,
            canonical_json_bytes(binding.to_mapping()).decode(),
            RETAINED_HISTORY_EXTENT,
            proof.to_json_bytes().decode(),
        ),
    )


def _pending_history(db: sqlite3.Connection, artifact_id: str) -> tuple[str, str, str]:
    row = db.execute(
        "SELECT binding_json, extent, proof_json FROM pending_member_history WHERE artifact_id = ?",
        (artifact_id,),
    ).fetchone()
    if row is None:
        raise InvalidState("local plan has no final member history")
    return str(row[0]), str(row[1]), str(row[2])


def _frozen_member_history(
    row: Any, archive_root: str
) -> tuple[MemberHistoryBinding, SourceMemberHistoryBindingProof]:
    if row["history_extent"] != RETAINED_HISTORY_EXTENT:
        raise InvalidState("local history selection is not complete retained history")
    binding = MemberHistoryBinding.from_mapping(
        require_canonical_json(str(row["history_binding_json"]).encode())
    )
    proof = SourceMemberHistoryBindingProof.from_json_bytes(str(row["history_proof_json"]).encode())
    if (
        (binding.artifact_id, binding.bytes, binding.sha256)
        != (row["artifact_id"], int(row["bytes"]), row["sha256"])
        or proof.binding != binding
        or proof.collection_id != int(row["collection_id"])
        or hashlib.sha256(proof.archive_root).hexdigest() != archive_root
    ):
        raise InvalidState("frozen member history differs from its exact root/member")
    return binding, proof


def _audit_member_histories(db: sqlite3.Connection, target: Path) -> Iterator[str]:
    for collection in db.execute(
        "SELECT collection_id, archive_root_sha256 FROM desired_collections ORDER BY collection_id"
    ):
        collection_id = int(collection["collection_id"])
        root = str(collection["archive_root_sha256"])
        with _history_closure(db, None, target, collection_id, root) as closure:
            for row in db.execute(
                "SELECT * FROM desired_artifacts WHERE collection_id = ? ORDER BY artifact_id",
                (collection_id,),
            ):
                try:
                    binding, proof = _frozen_member_history(row, root)
                    closure.resolve(binding, extent=RETAINED_HISTORY_EXTENT)
                    history = closure.store.descriptor(binding)
                    files = [
                        (
                            _history_file_components(binding.artifact_id, ".json"),
                            history.to_json_bytes(),
                        ),
                        *_history_control_files(binding, proof),
                    ]
                    for components, raw in files:
                        if not _matches(
                            _output(target, collection_id, components),
                            len(raw),
                            hashlib.sha256(raw).hexdigest(),
                        ):
                            yield (
                                f"member history mismatch: {collection_id}/"
                                f"{binding.artifact_id}/{components[-1]}"
                            )
                except (OSError, TypeError, ValueError, InvalidState) as exc:
                    yield f"member history invalid: {collection_id}/{row['artifact_id']}: {exc}"
        for row in db.execute(
            "SELECT object_id, bytes, sha256 FROM desired_history_objects "
            "WHERE collection_id = ? ORDER BY object_id",
            (collection_id,),
        ):
            if not _matches(
                _output(target, collection_id, _structure_components(str(row["object_id"]))),
                int(row["bytes"]),
                str(row["sha256"]),
            ):
                yield f"history structure mismatch: {collection_id}/{row['object_id']}"


def _tags(api: ApiClient, collection_id: int, summary: Mapping[str, Any]) -> list[str]:
    tags: list[str] = []
    token: str | None = None
    while True:
        page = api.list_collection_tags(
            collection_id,
            revision=int(summary["tag_revision"]),
            tag_set_identity=str(summary["tag_set_identity"]),
            page_size=100,
            page_token=token,
        )
        if (
            page.get("collection_id") != str(collection_id)
            or page.get("revision") != summary["tag_revision"]
            or page.get("tag_set_identity") != summary["tag_set_identity"]
        ):
            raise InvalidState("collection tag authority changed during traversal")
        values = page.get("tags")
        if not isinstance(values, list) or any(
            not isinstance(value, str) or validate_collection_tag(value) != value
            for value in values
        ):
            raise InvalidState("collection tag page is invalid")
        tags.extend(values)
        next_token = page.get("next_page_token")
        if next_token is None:
            return tags
        if not isinstance(next_token, str) or not next_token or not values:
            raise InvalidState("collection tag traversal did not advance")
        token = next_token


def _local_collection(db: sqlite3.Connection, collection_id: int) -> dict[str, object]:
    row = db.execute(
        """
        SELECT c.collection_id, c.created_at, c.archive_root_sha256,
               c.layout_mode, c.remote_unavailable,
               COUNT(a.artifact_id) AS artifacts,
               COALESCE(SUM(a.bytes), 0) AS bytes
        FROM desired_collections AS c
        LEFT JOIN desired_artifacts AS a USING (collection_id)
        WHERE c.collection_id = ?
        GROUP BY c.collection_id
        """,
        (collection_id,),
    ).fetchone()
    if row is None:
        raise NotFound(f"local collection not found: {collection_id}")
    return {
        "collection_id": int(row["collection_id"]),
        "created_at": str(row["created_at"]),
        "archive_root_sha256": str(row["archive_root_sha256"]),
        "layout_mode": str(row["layout_mode"]),
        "status": "remote-unavailable" if row["remote_unavailable"] else "desired",
        "artifacts": int(row["artifacts"]),
        "bytes": int(row["bytes"]),
        "tag_count": int(
            db.execute(
                "SELECT COUNT(*) FROM desired_collection_tags WHERE collection_id = ?",
                (collection_id,),
            ).fetchone()[0]
        ),
    }


@local_app.command("add")
def add_collection(
    collection_id: Annotated[int, typer.Argument(help="Collection ID")],
    mode: Annotated[
        str,
        typer.Option("--mode", help="declared-hints or id-layout"),
    ] = "declared-hints",
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    if mode not in {"declared-hints", "id-layout"}:
        raise typer.BadParameter("--mode must be declared-hints or id-layout")
    target = _target()
    normalized = normalize_collection_id(collection_id)
    with closing(_connect(target)) as db, ApiClient() as api:
        summary = api.get_collection(normalized)
        if normalize_collection_id(summary["id"]) != normalized:
            raise InvalidState("Riverhog returned another collection")
        archive_root = str(summary["archive_root_sha256"])
        existing = db.execute(
            "SELECT archive_root_sha256, layout_mode FROM desired_collections "
            "WHERE collection_id = ?",
            (normalized,),
        ).fetchone()
        if existing is not None:
            if (existing["archive_root_sha256"], existing["layout_mode"]) != (
                archive_root,
                mode,
            ):
                raise InvalidState("local collection has a different frozen root or layout")
            payload = {"status": "already-added", "collection": _local_collection(db, normalized)}
            emit(
                payload if json_mode else f"desired collection already added: {normalized}",
                json_mode=json_mode,
            )
            return
        inventory_identity, provenance_identity, artifacts = _inventory(api, normalized, summary)
        journals = _journals(api, target, normalized, archive_root)
        advice: list[MemberAdvice] = []
        bindings: dict[str, dict[str, Any]] = {}
        db.executescript(
            "CREATE TEMP TABLE pending_member_history (artifact_id TEXT PRIMARY KEY, "
            "binding_json TEXT NOT NULL, extent TEXT NOT NULL, proof_json TEXT NOT NULL);"
            "CREATE TEMP TABLE pending_history_objects (object_id TEXT PRIMARY KEY, "
            "bytes INTEGER NOT NULL, sha256 TEXT NOT NULL);"
        )
        with _history_closure(
            db, api, target, normalized, archive_root, journals, freezing=True
        ) as closure:
            for artifact in artifacts:
                hint, binding = _selected_hint(
                    api,
                    target,
                    normalized,
                    archive_root,
                    artifact,
                    journals,
                    db=db,
                    closure=closure,
                )
                advice.append(MemberAdvice(str(artifact.artifact_id), hint))
                bindings[str(artifact.artifact_id)] = binding
            for raw in closure.structure_objects():
                identity = provenance_structure_identity(raw)
                db.execute(
                    "INSERT INTO pending_history_objects VALUES (?, ?, ?)",
                    (
                        identity.object_id,
                        len(raw),
                        hashlib.sha256(raw).hexdigest(),
                    ),
                )
        rules = _destination_rules(_collection_directory(target, normalized))
        plan = plan_materialization(advice, rules=rules, mode=cast(Any, mode))
        plan_by_id = {row.artifact_id: row for row in plan}
        tags = _tags(api, normalized, summary)
        if api.get_collection(normalized)["archive_root_sha256"] != archive_root:
            raise InvalidState("collection archive root changed before local plan freeze")
        db.execute(
            """
            INSERT INTO desired_collections (
                collection_id, archive_root_sha256, inventory_identity,
                artifact_set_identity, provenance_identity, created_at,
                layout_mode, rules_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized,
                archive_root,
                inventory_identity,
                str(summary["artifact_set_identity"]),
                provenance_identity,
                str(summary["created_at"]),
                mode,
                json.dumps(dataclasses.asdict(rules), sort_keys=True),
            ),
        )
        db.executemany(
            "INSERT INTO desired_collection_tags (collection_id, tag) VALUES (?, ?)",
            ((normalized, tag) for tag in tags),
        )
        db.executemany(
            """
            INSERT INTO desired_journals (collection_id, journal_id, bytes, sha256)
            VALUES (?, ?, ?, ?)
            """,
            (
                (normalized, journal_id, value[1], value[2])
                for journal_id, value in journals.items()
            ),
        )
        db.executemany(
            """
            INSERT INTO desired_artifacts (
                collection_id, artifact_id, bytes, sha256, destination_json, reason,
                hint_json, binding_json, primary_bytes, primary_sha256,
                history_binding_json, history_extent, history_proof_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    normalized,
                    str(artifact.artifact_id),
                    artifact.bytes,
                    artifact.sha256,
                    json.dumps(
                        list(plan_by_id[str(artifact.artifact_id)].components), ensure_ascii=False
                    ),
                    plan_by_id[str(artifact.artifact_id)].reason,
                    (
                        json.dumps(
                            list(
                                cast(
                                    tuple[str, ...],
                                    plan_by_id[str(artifact.artifact_id)].materialization_hint,
                                )
                            ),
                            ensure_ascii=False,
                        )
                        if plan_by_id[str(artifact.artifact_id)].materialization_hint is not None
                        else None
                    ),
                    json.dumps(bindings[str(artifact.artifact_id)], sort_keys=True),
                    int(bindings[str(artifact.artifact_id)]["journal"]["prefix_bytes"]),
                    bindings[str(artifact.artifact_id)]["journal"]["prefix_sha256"],
                    *_pending_history(db, str(artifact.artifact_id)),
                )
                for artifact in artifacts
            ),
        )
        db.execute(
            "INSERT INTO desired_history_objects (collection_id, object_id, bytes, sha256) "
            "SELECT ?, object_id, bytes, sha256 FROM pending_history_objects",
            (normalized,),
        )
        db.commit()
        payload = {"status": "added", "collection": _local_collection(db, normalized)}
    emit(payload if json_mode else f"desired collection added: {normalized}", json_mode=json_mode)


def _destination(target: Path, collection_id: int, serialized: str) -> Path:
    parts = json.loads(serialized)
    if not isinstance(parts, list) or any(not isinstance(part, str) for part in parts):
        raise InvalidState("frozen local destination is malformed")
    return _output(target, collection_id, parts)


def _ensure_provenance(
    db: sqlite3.Connection,
    api: ApiClient,
    target: Path,
    *,
    repair: bool,
) -> None:
    for collection in db.execute(
        "SELECT collection_id, archive_root_sha256 FROM desired_collections "
        "WHERE remote_unavailable = 0 ORDER BY collection_id"
    ):
        collection_id = int(collection["collection_id"])
        archive_root = str(collection["archive_root_sha256"])
        frozen = {
            str(row["journal_id"]): (int(row["bytes"]), str(row["sha256"]))
            for row in db.execute(
                "SELECT journal_id, bytes, sha256 FROM desired_journals WHERE collection_id = ?",
                (collection_id,),
            )
        }
        current = _journals(api, target, collection_id, archive_root, repair=repair)
        if {key: (value[1], value[2]) for key, value in current.items()} != frozen:
            raise InvalidState("canonical journal corpus differs from frozen local state")
        with _history_closure(
            db, api, target, collection_id, archive_root, current, repair=repair
        ) as closure:
            for row in db.execute(
                "SELECT * FROM desired_artifacts WHERE collection_id = ? ORDER BY artifact_id",
                (collection_id,),
            ):
                selected, proof = _frozen_member_history(row, archive_root)
                detail = api.get_collection_artifact_provenance(
                    collection_id, ArtifactId(selected.artifact_id)
                )
                try:
                    if (
                        detail.get("archive_root_sha256") != archive_root
                        or MemberHistoryBinding.from_mapping(detail.get("history_binding"))
                        != selected
                    ):
                        raise ValueError("remote member history differs from its frozen selection")
                    history = selected.verify_descriptor(
                        canonical_json_bytes(detail.get("member_history"))
                    )
                except (TypeError, ValueError) as exc:
                    raise InvalidState(
                        f"final member history is missing or invalid: {exc}"
                    ) from exc
                closure.resolve(selected, extent=RETAINED_HISTORY_EXTENT)
                primary = json.loads(str(row["binding_json"]))
                if history.primary.to_mapping() != {
                    key: primary[key] for key in ("journal", "delivery_association_id")
                }:
                    raise InvalidState("frozen primary differs from final member history")
                _publish_provenance_blob(
                    target,
                    collection_id,
                    _history_file_components(selected.artifact_id, ".json"),
                    history.to_json_bytes(),
                    repair=repair,
                )
                for components, raw in _history_control_files(selected, proof):
                    _publish_provenance_blob(target, collection_id, components, raw, repair=repair)
                source = current[primary["journal"]["journal_id"]][0]
                output = _output(
                    target, collection_id, primary_sidecar_components(selected.artifact_id)
                )
                byte_count = int(row["primary_bytes"])
                sha256 = str(row["primary_sha256"])
                if _matches(output, byte_count, sha256):
                    continue
                if output.exists() or output.is_symlink():
                    if not repair:
                        raise InvalidState("local primary provenance was modified")
                    _quarantine(
                        target,
                        output,
                        collection_id=collection_id,
                        artifact_id=selected.artifact_id,
                    )
                _publish_prefix(source, output, byte_count=byte_count, sha256=sha256)
        if api.get_collection(collection_id)["archive_root_sha256"] != archive_root:
            raise InvalidState(
                "collection archive root changed during local history synchronization"
            )


def _quarantine(
    target: Path,
    output: Path,
    *,
    collection_id: int,
    artifact_id: str | None = None,
) -> Path:
    directory = target / ".a-riverhog-cli-quarantine"
    if directory.is_symlink():
        raise InvalidState("local quarantine is a symbolic link")
    directory.mkdir(mode=0o700, exist_ok=True)
    if output.is_symlink() or not output.is_file():
        raise InvalidState("local non-regular materialization requires manual repair")
    identity = uuid.uuid4().hex
    destination = directory / (identity + ".data")
    association = directory / (identity + ".json")
    record = {
        "collection_id": str(collection_id),
        "artifact_id": artifact_id,
        "original_destination": list(output.relative_to(target).parts),
    }
    created = False
    try:
        with association.open("xb") as stream:
            created = True
            stream.write(canonical_json_bytes(record))
            stream.flush()
            os.fsync(stream.fileno())
        os.link(output, destination)
        with destination.open("rb") as stream:
            os.fsync(stream.fileno())
    except BaseException:
        if created and not destination.exists():
            association.unlink(missing_ok=True)
        raise
    output.unlink()
    return destination


def _missing_artifacts(
    db: sqlite3.Connection, target: Path, *, repair: bool
) -> list[tuple[int, str]]:
    missing: list[tuple[int, str]] = []
    for row in db.execute(
        """
        SELECT a.collection_id, a.artifact_id, a.destination_json, a.bytes, a.sha256
        FROM desired_artifacts AS a
        JOIN desired_collections AS c USING (collection_id)
        WHERE c.remote_unavailable = 0
        ORDER BY a.collection_id, a.artifact_id
        """
    ):
        output = _destination(target, int(row["collection_id"]), str(row["destination_json"]))
        byte_count = int(row["bytes"])
        sha256 = str(row["sha256"])
        if _matches(output, byte_count, sha256):
            continue
        if output.exists() or output.is_symlink():
            if not repair:
                typer.echo(
                    f"mismatch retained: {row['collection_id']}/{row['artifact_id']}",
                    err=True,
                )
                continue
            _quarantine(
                target,
                output,
                collection_id=int(row["collection_id"]),
                artifact_id=str(row["artifact_id"]),
            )
        missing.append((int(row["collection_id"]), str(row["artifact_id"])))
    return missing


def _retrieval_plan_artifacts(
    api: ApiClient, plan: Mapping[str, Any]
) -> tuple[dict[str, Any], ...]:
    plan_id = str(plan["id"])
    plan_etag = str(plan["etag"])
    artifact_count = int(plan["artifact_count"])
    artifacts: list[dict[str, Any]] = []
    start_ordinal = 0
    while True:
        page = api.list_retrieval_plan_artifacts(
            plan_id, plan_etag=plan_etag, start_ordinal=start_ordinal, page_size=100
        )
        current = page.get("artifacts")
        if (
            page.get("plan_id") != plan_id
            or page.get("etag") != plan_etag
            or page.get("start_ordinal") != start_ordinal
            or not isinstance(current, list)
            or any(not isinstance(item, dict) for item in current)
        ):
            raise InvalidState("retrieval plan artifact page changed its authority")
        artifacts.extend(current)
        if len(artifacts) > artifact_count:
            raise InvalidState("retrieval plan exceeded its declared artifact count")
        if page.get("complete") is True:
            if page.get("next_ordinal") is not None or len(artifacts) != artifact_count:
                raise InvalidState("retrieval plan ended inconsistently")
            return tuple(artifacts)
        next_ordinal = page.get("next_ordinal")
        expected_next = start_ordinal + len(current)
        if not current or isinstance(next_ordinal, bool) or next_ordinal != expected_next:
            raise InvalidState("retrieval plan did not advance exactly")
        start_ordinal = expected_next


def _download_job(
    db: sqlite3.Connection,
    target: Path,
    api: ApiClient,
    job: Mapping[str, Any],
) -> int:
    job_id = str(job["id"])
    lease_seconds = int(job["lease_seconds"])
    api.renew_retrieval_job(job_id, lease_seconds=lease_seconds)
    rows = list(
        db.execute(
            """
            SELECT r.collection_id, r.artifact_id, r.bytes, r.sha256,
                   a.destination_json
            FROM retrieval_job_artifacts AS r
            JOIN desired_artifacts AS a
              ON a.collection_id = r.collection_id AND a.artifact_id = r.artifact_id
            WHERE r.retrieval_job_id = ? ORDER BY r.ordinal
            """,
            (job_id,),
        )
    )
    count = db.execute(
        "SELECT COUNT(*) FROM retrieval_job_artifacts WHERE retrieval_job_id = ?",
        (job_id,),
    ).fetchone()[0]
    if len(rows) != count:
        raise InvalidState("local retrieval job has an unavailable frozen artifact")
    downloads: list[RetrievalDownload] = []
    transfer_root = target / ".a-riverhog-cli-transfers" / job_id
    if transfer_root.is_symlink():
        raise InvalidState("local transfer root is a symbolic link")
    transfer_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    for row in rows:
        output = _destination(target, int(row["collection_id"]), str(row["destination_json"]))
        if _matches(output, int(row["bytes"]), str(row["sha256"])):
            continue
        if output.exists() or output.is_symlink():
            raise InvalidState(f"local artifact was modified: {output}")
        staging = transfer_root / f"{row['collection_id']}-{row['artifact_id']}"
        downloads.append(
            RetrievalDownload(
                collection_id=int(row["collection_id"]),
                artifact_id=ArtifactId(str(row["artifact_id"])),
                output=staging,
                expected_bytes=int(row["bytes"]),
                expected_sha256=str(row["sha256"]),
            )
        )

    def maintain_lease() -> None:
        api.renew_retrieval_job(job_id, lease_seconds=lease_seconds)

    concurrency = configured_download_concurrency()
    try:
        download_retrieval_files(
            api,
            job_id,
            downloads,
            concurrency=concurrency,
            window=configured_download_window(concurrency=concurrency),
            heartbeat=maintain_lease,
            heartbeat_interval_seconds=max(0.1, min(3600, lease_seconds / 3)),
        )
        for download in downloads:
            row = next(
                current
                for current in rows
                if int(current["collection_id"]) == download.collection_id
                and str(current["artifact_id"]) == download.artifact_id
            )
            output = _destination(target, int(row["collection_id"]), str(row["destination_json"]))
            _publish_file(
                download.output,
                output,
                byte_count=download.expected_bytes,
                sha256=download.expected_sha256,
            )
        api.acknowledge_retrieval_job(job_id)
        db.execute("DELETE FROM retrieval_jobs WHERE id = ?", (job_id,))
        db.commit()
        return len(downloads)
    finally:
        shutil.rmtree(transfer_root, ignore_errors=True)


def _cancel_active_retrievals(db: sqlite3.Connection, api: ApiClient) -> list[str]:
    canceled: list[str] = []
    for row in db.execute("SELECT id FROM retrieval_jobs ORDER BY updated_at"):
        job_id = str(row["id"])
        job = api.get_retrieval_job(job_id)
        if job["state"] in {"requested", "ready", "failed"}:
            api.cancel_retrieval_job(job_id)
            canceled.append(job_id)
    db.execute("DELETE FROM retrieval_jobs")
    db.commit()
    return canceled


def _sync(*, wait: bool, repair: bool, restore_policy: str) -> dict[str, object]:
    if restore_policy not in {"allow", "never"}:
        raise typer.BadParameter("--restore-policy must be allow or never")
    target = _target()
    with closing(_connect(target)) as db, ApiClient() as api:
        for row in list(
            db.execute("SELECT collection_id, archive_root_sha256 FROM desired_collections")
        ):
            collection_id = int(row["collection_id"])
            try:
                remote = api.get_collection(collection_id)
            except NotFound:
                db.execute(
                    "UPDATE desired_collections SET remote_unavailable = 1 WHERE collection_id = ?",
                    (collection_id,),
                )
                continue
            if remote.get("archive_root_sha256") != row["archive_root_sha256"]:
                raise InvalidState("remote collection differs from frozen local archive root")
            db.execute(
                "UPDATE desired_collections SET remote_unavailable = 0 WHERE collection_id = ?",
                (collection_id,),
            )
        db.commit()
        _ensure_provenance(db, api, target, repair=repair)
        materialized = 0
        unavailable: set[tuple[int, str]] = set()
        while True:
            active = db.execute(
                "SELECT id FROM retrieval_jobs ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
            job: dict[str, Any] | None = None
            if active is not None:
                job = api.get_retrieval_job(str(active["id"]))
                if job["state"] in {"expired", "failed", "canceled", "completed"}:
                    db.execute("DELETE FROM retrieval_jobs WHERE id = ?", (job["id"],))
                    db.commit()
                    job = None
            if job is None:
                missing = [
                    item
                    for item in _missing_artifacts(db, target, repair=repair)
                    if item not in unavailable
                ]
                if not missing:
                    if unavailable:
                        return {
                            "status": "cache-miss",
                            "materialized_artifacts": materialized,
                            "unavailable_artifacts": len(unavailable),
                        }
                    return {
                        "status": "materialized" if materialized else "current",
                        "materialized_artifacts": materialized,
                    }
                batch = missing[:RETRIEVAL_ARTIFACT_BATCH_MAX]
                plan = api.plan_retrieval(batch, restore_policy=cast(RestorePolicy, restore_policy))
                selected = _retrieval_plan_artifacts(api, plan)
                actual = tuple(
                    (normalize_collection_id(item["collection_id"]), str(item["artifact_id"]))
                    for item in selected
                )
                if actual != tuple(sorted(batch)):
                    raise InvalidState("retrieval plan changed its requested artifact selection")
                if restore_policy == "never" and plan["requires_restore"]:
                    blocked = {
                        (normalize_collection_id(item["collection_id"]), str(item["artifact_id"]))
                        for item in selected
                        if item["requires_restore"] is True
                    }
                    unavailable.update(blocked)
                    batch = [item for item in batch if item not in blocked]
                    if not batch:
                        continue
                    plan = api.plan_retrieval(
                        batch, restore_policy=cast(RestorePolicy, restore_policy)
                    )
                    selected = _retrieval_plan_artifacts(api, plan)
                job = api.create_retrieval_job(str(plan["id"]), plan_etag=str(plan["etag"]))
                db.execute(
                    "INSERT INTO retrieval_jobs (id, state) VALUES (?, ?)",
                    (job["id"], job["state"]),
                )
                db.executemany(
                    """
                    INSERT INTO retrieval_job_artifacts (
                        retrieval_job_id, ordinal, collection_id, artifact_id, bytes, sha256
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        (
                            job["id"],
                            index,
                            normalize_collection_id(item["collection_id"]),
                            str(item["artifact_id"]),
                            int(item["bytes"]),
                            str(item["sha256"]),
                        )
                        for index, item in enumerate(selected)
                    ),
                )
                db.commit()
            while job["state"] == "requested" and wait:
                time.sleep(10)
                job = api.get_retrieval_job(str(job["id"]))
            if job["state"] != "ready":
                return {
                    "status": str(job["state"]),
                    "retrieval": job,
                    "materialized_artifacts": materialized,
                }
            materialized += _download_job(db, target, api, job)


@local_app.command("sync")
def sync(
    wait: Annotated[bool, typer.Option(help="Wait while archive retrieval is pending")] = False,
    restore_policy: Annotated[str, typer.Option("--restore-policy")] = "allow",
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    payload = _sync(wait=wait, repair=False, restore_policy=restore_policy)
    emit(
        payload
        if json_mode
        else f"{payload['status']}: {payload.get('materialized_artifacts', 0)} artifacts",
        json_mode=json_mode,
    )


@local_app.command("repair")
def repair(
    wait: Annotated[bool, typer.Option(help="Wait while archive retrieval is pending")] = False,
    restore_policy: Annotated[str, typer.Option("--restore-policy")] = "allow",
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    payload = _sync(wait=wait, repair=True, restore_policy=restore_policy)
    emit(
        payload
        if json_mode
        else f"{payload['status']}: {payload.get('materialized_artifacts', 0)} artifacts",
        json_mode=json_mode,
    )


@local_app.command("remove")
def remove_collection(
    collection_id: Annotated[int, typer.Argument(help="Collection ID")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    target = _target()
    normalized = normalize_collection_id(collection_id)
    with closing(_connect(target)) as db, ApiClient() as api:
        canceled = _cancel_active_retrievals(db, api)
        db.execute("DELETE FROM desired_collections WHERE collection_id = ?", (normalized,))
        db.commit()
    payload = {
        "status": "removed",
        "collection_id": normalized,
        "local_artifacts": "retained",
        "retrievals_canceled": canceled,
    }
    emit(
        payload
        if json_mode
        else f"desired collection removed; local artifacts retained: {normalized}",
        json_mode=json_mode,
    )


@local_app.command("show")
def show_collection(
    collection_id: Annotated[int, typer.Argument(help="Collection ID")],
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    target = _target()
    with closing(_connect(target)) as db:
        payload = _local_collection(db, normalize_collection_id(collection_id))
    emit(payload if json_mode else format_local_collection(payload), json_mode=json_mode)


LOCAL_LIST_SORT_FIELDS = {
    "collection_id": "collection_id",
    "created_at": "created_at",
    "tag_count": "tag_count",
    "status": "status",
    "artifacts": "artifacts",
    "bytes": "bytes",
}


@local_app.command("list")
def list_collections(
    page_size: Annotated[
        int,
        typer.Option("--page-size", min=1, max=LOCAL_LIST_PAGE_SIZE_MAX),
    ] = 25,
    page_token: Annotated[str | None, typer.Option("--page-token")] = None,
    sort: Annotated[str, typer.Option("--sort", help="Sort field")] = "collection_id",
    order: Annotated[str, typer.Option("--order", help="Sort order")] = "asc",
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Search collection id, tag, or status"),
    ] = None,
    ids: Annotated[
        bool,
        typer.Option("--ids", help="Emit one collection id per line"),
    ] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    if ids and json_mode:
        raise typer.BadParameter("--ids and --json cannot be used together")
    if sort not in LOCAL_LIST_SORT_FIELDS:
        allowed = ", ".join(sorted(LOCAL_LIST_SORT_FIELDS))
        raise typer.BadParameter(f"--sort must be one of: {allowed}")
    normalized_order = order.strip().lower()
    if normalized_order not in {"asc", "desc"}:
        raise typer.BadParameter("--order must be asc or desc")

    target = _target()
    normalized_query = (query or "").strip() or None
    selectors = {
        "order": normalized_order,
        "query": normalized_query,
        "sort": sort,
    }
    database = _database(target)
    token_codec = BrowseTokenCodec(
        hashlib.sha256(
            b"a-riverhog-cli-local-list-token/v1\x00" + str(database).encode("utf-8")
        ).digest(),
        lifetime_seconds=LOCAL_LIST_TOKEN_LIFETIME_SECONDS,
    )
    try:
        position = token_codec.verify(
            page_token,
            operation="local.list_collections",
            principal=str(database),
            selectors=selectors,
        )
    except BrowseTokenError as exc:
        raise typer.BadParameter(str(exc), param_hint="--page-token") from exc
    if position is not None and len(position) != 2:
        raise typer.BadParameter("page token position is invalid", param_hint="--page-token")
    with closing(_connect(target)) as db:
        filters = ""
        params: list[object] = []
        if normalized_query:
            filters = (
                "WHERE CAST(collection_id AS TEXT) LIKE ? "
                "OR EXISTS (SELECT 1 FROM desired_collection_tags AS t "
                "           WHERE t.collection_id = local_collections.collection_id "
                "             AND t.tag LIKE ?) "
                "OR status LIKE lower(?)"
            )
            pattern = f"%{normalized_query}%"
            params.extend((pattern, pattern, pattern))
        base_query = f"""
                WITH local_collections AS (
                SELECT c.collection_id, c.created_at, c.remote_unavailable,
                       (SELECT COUNT(*) FROM desired_collection_tags AS t
                        WHERE t.collection_id = c.collection_id) AS tag_count,
                       CASE
                           WHEN c.remote_unavailable = 1 THEN 'remote-unavailable'
                           ELSE 'desired'
                       END AS status,
                       COUNT(a.artifact_id) AS artifacts,
                       COALESCE(SUM(a.bytes), 0) AS bytes
                FROM desired_collections AS c
                LEFT JOIN desired_artifacts AS a USING (collection_id)
                GROUP BY c.collection_id, c.created_at, c.remote_unavailable
                )
                SELECT * FROM local_collections
                {filters}
                """
        order_column = LOCAL_LIST_SORT_FIELDS[sort]
        continuation = ""
        if position is not None:
            sort_value, collection_id = position
            if not isinstance(collection_id, int) or isinstance(collection_id, bool):
                raise typer.BadParameter(
                    "page token position is invalid", param_hint="--page-token"
                )
            comparison = ">" if normalized_order == "asc" else "<"
            continuation = (
                f"WHERE ({order_column} {comparison} ? "
                f"OR ({order_column} = ? AND collection_id > ?))"
            )
            params.extend((sort_value, sort_value, collection_id))
        rows = db.execute(
            f"""
            SELECT * FROM ({base_query})
            {continuation}
            ORDER BY {order_column} {normalized_order.upper()}, collection_id ASC
            LIMIT ?
            """,
            (*params, page_size + 1),
        ).fetchall()
        has_more = len(rows) > page_size
        page_rows = rows[:page_size]
        collections = [_local_collection(db, int(row["collection_id"])) for row in page_rows]
    next_page_token = None
    if has_more and page_rows:
        last = page_rows[-1]
        next_page_token = token_codec.issue(
            operation="local.list_collections",
            principal=str(database),
            selectors=selectors,
            position=(last[order_column], int(last["collection_id"])),
        )
    payload = {
        "page_size": page_size,
        "next_page_token": next_page_token,
        "sort": sort,
        "order": normalized_order,
        "query": normalized_query,
        "collections": collections,
    }
    if ids:
        emit(
            format_list_ids(payload, "collections", id_key="collection_id"),
            json_mode=False,
        )
        return
    emit(payload if json_mode else format_local_collections(payload), json_mode=json_mode)


@local_app.command("audit")
def audit(
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    target = _target()
    problems = 0
    samples: list[str] = []

    def record(label: str) -> None:
        nonlocal problems
        problems += 1
        if len(samples) < LOCAL_AUDIT_SAMPLE_LIMIT:
            samples.append(label)

    with closing(_connect(target)) as db:
        for problem in _audit_member_histories(db, target):
            record(problem)
        for row in db.execute(
            "SELECT collection_id, artifact_id, destination_json, bytes, sha256, "
            "primary_bytes, primary_sha256 FROM desired_artifacts "
            "ORDER BY collection_id, artifact_id"
        ):
            collection_id = int(row["collection_id"])
            artifact_id = str(row["artifact_id"])
            output = _destination(target, collection_id, str(row["destination_json"]))
            if not _matches(output, int(row["bytes"]), str(row["sha256"])):
                record(f"artifact mismatch: {collection_id}/{artifact_id}")
            sidecar = _output(target, collection_id, primary_sidecar_components(artifact_id))
            if not _matches(sidecar, int(row["primary_bytes"]), str(row["primary_sha256"])):
                record(f"primary provenance mismatch: {collection_id}/{artifact_id}")
        for row in db.execute(
            "SELECT collection_id, journal_id, bytes, sha256 FROM desired_journals "
            "ORDER BY collection_id, journal_id"
        ):
            output = _output(
                target,
                int(row["collection_id"]),
                shared_journal_components(str(row["sha256"])),
            )
            if not _matches(output, int(row["bytes"]), str(row["sha256"])):
                record(f"journal mismatch: {row['collection_id']}/{row['journal_id']}")
    payload = {
        "status": "ok" if not problems else "issues",
        "problems": problems,
        "samples": samples,
        "samples_truncated": problems > len(samples),
    }
    if problems:
        emit(payload if json_mode else "\n".join(samples), json_mode=json_mode)
        raise typer.Exit(1)
    emit(payload if json_mode else "local artifacts and provenance match", json_mode=json_mode)


@local_app.command("evict")
def evict(
    collection_id: Annotated[int, typer.Argument(help="Collection ID")],
    confirm: Annotated[bool, typer.Option("--confirm")] = False,
    json_mode: Annotated[bool, typer.Option("--json", help="Emit JSON.")] = False,
) -> None:
    if not confirm:
        raise typer.BadParameter("--confirm is required")
    target = _target()
    normalized = normalize_collection_id(collection_id)
    with closing(_connect(target)) as db, ApiClient() as api:
        rows = list(
            db.execute(
                "SELECT * FROM desired_artifacts WHERE collection_id = ?",
                (normalized,),
            )
        )
        journals = list(
            db.execute(
                "SELECT sha256, bytes FROM desired_journals WHERE collection_id = ?",
                (normalized,),
            )
        )
        outputs: list[tuple[Path, int, str]] = []
        collection = db.execute(
            "SELECT archive_root_sha256 FROM desired_collections WHERE collection_id = ?",
            (normalized,),
        ).fetchone()
        if collection is None:
            raise NotFound(f"local collection not found: {normalized}")
        for row in rows:
            history_binding, proof = _frozen_member_history(row, str(collection[0]))
            outputs.append(
                (
                    _output(
                        target,
                        normalized,
                        _history_file_components(history_binding.artifact_id, ".json"),
                    ),
                    history_binding.history_bytes,
                    history_binding.history_sha256,
                )
            )
            for components, raw in _history_control_files(history_binding, proof):
                outputs.append(
                    (
                        _output(target, normalized, components),
                        len(raw),
                        hashlib.sha256(raw).hexdigest(),
                    )
                )
            outputs.append(
                (
                    _destination(target, normalized, str(row["destination_json"])),
                    int(row["bytes"]),
                    str(row["sha256"]),
                )
            )
            outputs.append(
                (
                    _output(
                        target,
                        normalized,
                        primary_sidecar_components(str(row["artifact_id"])),
                    ),
                    int(row["primary_bytes"]),
                    str(row["primary_sha256"]),
                )
            )
        for row in db.execute(
            "SELECT object_id, bytes, sha256 FROM desired_history_objects WHERE collection_id = ?",
            (normalized,),
        ):
            outputs.append(
                (
                    _output(target, normalized, _structure_components(str(row["object_id"]))),
                    int(row["bytes"]),
                    str(row["sha256"]),
                )
            )
        for row in journals:
            outputs.append(
                (
                    _output(target, normalized, shared_journal_components(str(row["sha256"]))),
                    int(row["bytes"]),
                    str(row["sha256"]),
                )
            )
        for output, byte_count, sha256 in outputs:
            if output.exists() and not _matches(output, byte_count, sha256):
                raise InvalidState(f"local modification blocks eviction: {output}")
        canceled = _cancel_active_retrievals(db, api)
        for output, _, _ in outputs:
            output.unlink(missing_ok=True)
        db.execute("DELETE FROM desired_collections WHERE collection_id = ?", (normalized,))
        db.commit()
    payload = {
        "status": "evicted",
        "collection_id": normalized,
        "artifacts": len(rows),
        "retrievals_canceled": canceled,
    }
    emit(payload if json_mode else f"evicted local collection {normalized}", json_mode=json_mode)
