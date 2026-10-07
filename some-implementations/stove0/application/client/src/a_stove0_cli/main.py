"""Rich human and stable JSON commands for every public stove0 operation."""

from __future__ import annotations

import importlib.metadata
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, NoReturn, cast

import typer
from http_api_contracts import closed_literal_values
from pydantic import BaseModel
from rich.console import Console
from rich.pretty import Pretty
from rich.table import Table
from stove0_api_client import Stove0ApiClient, Stove0ApiError
from stove0_operator_contracts import PlanningOwnerKind, WorkInitiationStatus
from stove0_protocol import CollectionRootIdentityRef, canonical_json_bytes
from stove0_protocol.planning_jobs import PlanningJobStatus
from stove0_recipe_config.catalog import (
    CompiledRecipeCatalog,
    RecipeCatalogExplanation,
    RecipeCatalogValidation,
    load_recipe_catalog,
)
from stove0_recipe_config.source_map import recipe_source_map

app = typer.Typer(
    help="Operate stove0 collection workflows.",
    add_completion=False,
)

_CLI_RESULT_CONTRACT = {
    "format": "riverhog-cli-result-contract/v1",
    "identity_prefix": "stove0-cli-result",
    "default_profile": "human-json",
    "profiles": {
        "human-json": {
            "id": "stove0-cli-human-json/v1",
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
                    "stdout": {"all": "empty"},
                    "stderr": {"all": "noncontractual-diagnostic"},
                },
            ],
        }
    },
    "command_profiles": {},
    "command_overrides": {},
    "executable_groups": [],
    "outcome_selectors": {
        "completed": {"kind": "command-completed"},
        "operational": {"kind": "application-error"},
        "usage": {"kind": "parser-rejected-invocation"},
    },
    "output_authorities": {
        "work create": {
            "kind": "openapi-schema",
            "schema": "WorkInitiationStatus",
        },
        "preview": {
            "kind": "openapi-schema",
            "schema": "PlanningJobStatus",
        },
        "health": {
            "kind": "openapi-schema",
            "schema": "HealthOut",
        },
        "recipe validate": {
            "kind": "cli-local-json-schema",
            "identity": "stove0-recipe-catalog-validation/v1",
            "schema": RecipeCatalogValidation.model_json_schema(),
        },
        "recipe compile": {
            "kind": "cli-local-json-schema",
            "identity": "stove0-compiled-recipe-catalog/v1",
            "schema": CompiledRecipeCatalog.model_json_schema(),
        },
        "recipe explain": {
            "kind": "cli-local-json-schema",
            "identity": "stove0-recipe-catalog-explanation/v1",
            "schema": RecipeCatalogExplanation.model_json_schema(),
        },
    },
    "version_distribution": "a-stove0-cli",
}
work_app = typer.Typer(help="Transformation work.")
recipe_app = typer.Typer(help="Configured recipes.")
evaluation_app = typer.Typer(help="Materialized trials and evaluations.")
event_app = typer.Typer(help="Lifecycle events.")
scheduler_app = typer.Typer(help="Scheduler status and execution.")
selection_app = typer.Typer(help="Exact content-addressed artifact selections.")
observation_app = typer.Typer(help="Named tasks, accepted evidence, and typed observation views.")
admission_app = typer.Typer(help="Classification admission decisions.")
admission_policy_app = typer.Typer(help="Configured classification admission policies.")
departure_app = typer.Typer(help="Catalog-departure external effects.")
departure_policy_app = typer.Typer(help="Configured departure-effect policies.")
app.add_typer(work_app, name="work")
app.add_typer(recipe_app, name="recipe")
app.add_typer(evaluation_app, name="evaluation")
app.add_typer(event_app, name="event")
app.add_typer(scheduler_app, name="scheduler")
app.add_typer(selection_app, name="selection")
app.add_typer(observation_app, name="observation")
app.add_typer(admission_app, name="admission")
admission_app.add_typer(admission_policy_app, name="policy")
app.add_typer(departure_app, name="departure")
departure_app.add_typer(departure_policy_app, name="policy")
console = Console()


@dataclass(frozen=True)
class Context:
    client_factory: Callable[[], Stove0ApiClient]
    json_output: bool

    @cached_property
    def client(self) -> Stove0ApiClient:
        return self.client_factory()


@app.callback()
def configure(
    context: typer.Context,
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            callback=lambda value: _version_callback(value),
            is_eager=True,
            help="Show the installed version and exit.",
        ),
    ] = None,
    base_url: str | None = typer.Option(None, "--base-url"),
    token: str | None = typer.Option(None, "--token"),
    json_output: bool = typer.Option(False, "--json", help="Emit stable JSON."),
    allow_insecure_http: bool | None = typer.Option(
        None,
        "--allow-insecure-http/--no-allow-insecure-http",
        help="Explicitly allow remote cleartext HTTP.",
    ),
) -> None:
    del version
    context.obj = Context(
        client_factory=lambda: Stove0ApiClient(
            base_url=base_url,
            token=token,
            allow_insecure_http=allow_insecure_http,
        ),
        json_output=json_output,
    )


def _version_callback(value: bool | None) -> None:
    if not value:
        return
    typer.echo(importlib.metadata.version("a-stove0-cli"))
    raise typer.Exit()


@app.command("health")
def health(context: typer.Context, ready: bool = typer.Option(False, "--ready")) -> None:
    state = _context(context)
    _call(state, state.client.health_ready if ready else state.client.health_live)


@recipe_app.command("list")
def list_recipes(context: typer.Context) -> None:
    state = _context(context)
    _call(state, state.client.list_recipes, table=("recipes", ("id", "revision", "sha256")))


@recipe_app.command("show")
def show_recipe(
    context: typer.Context,
    recipe_id: str,
    revision: int | None = typer.Option(None),
) -> None:
    state = _context(context)
    _call(state, lambda: state.client.get_recipe(recipe_id, revision=revision))


@admission_policy_app.command("list")
def list_admission_policies(context: typer.Context) -> None:
    state = _context(context)
    _call(
        state,
        state.client.list_admission_policies,
        table=("policies", ("id", "revision", "phase", "selector", "recipe_id")),
    )


@admission_policy_app.command("rebaseline")
def rebaseline_admission_policy(context: typer.Context, policy_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.rebaseline_admission_policy(policy_id))


@admission_policy_app.command("backfill")
def backfill_admission_policy(context: typer.Context, policy_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.backfill_admission_policy(policy_id))


@admission_app.command("list")
def list_admissions(
    context: typer.Context,
    page_size: int = typer.Option(25, min=1, max=100),
    page_token: str | None = typer.Option(None),
    policy_id: str | None = typer.Option(None),
    state_filter: str | None = typer.Option(None, "--state"),
    query: str | None = typer.Option(None, "--query", "-q"),
    sort: str = typer.Option("created_at"),
    order: str = typer.Option("desc"),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.list_admissions(
            page_size=page_size,
            page_token=page_token,
            policy_id=policy_id,
            state=cast(Any, state_filter),
            query=query,
            sort=cast(Any, sort),
            order=cast(Any, order),
        ),
        table=(
            "admissions",
            (
                "admission_id",
                "policy_id",
                "collection_id",
                "state",
                "attempt_count",
                "next_attempt_at",
                "failure",
                "work_id",
            ),
        ),
    )


@admission_app.command("show")
def show_admission(context: typer.Context, admission_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.get_admission(admission_id))


@departure_policy_app.command("list")
def list_departure_policies(context: typer.Context) -> None:
    state = _context(context)
    _call(
        state,
        state.client.list_departure_policies,
        table=("policies", ("id", "revision", "phase", "selector", "target_registration_id")),
    )


@departure_policy_app.command("rebaseline")
def rebaseline_departure_policy(context: typer.Context, policy_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.rebaseline_departure_policy(policy_id))


@departure_app.command("list")
def list_departure_effects(
    context: typer.Context,
    page_size: int = typer.Option(25, min=1, max=100),
    page_token: str | None = typer.Option(None),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.list_departure_effects(page_size=page_size, page_token=page_token),
        table=("effects", ("departure_id", "policy_id", "state", "attempt_count", "failure")),
    )


@departure_app.command("show")
def show_departure_effect(context: typer.Context, departure_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.get_departure_effect(departure_id))


@recipe_app.command("validate")
def validate_recipe_catalog(
    context: typer.Context,
    path: Annotated[Path, typer.Argument(exists=True, dir_okay=False, readable=True)],
) -> None:
    """Validate source or compiled catalog documents and their exact offline closure."""

    state = _context(context)
    _call(
        state,
        lambda: load_recipe_catalog(path).validation_document(),
        table=("recipes", ("id", "revision", "sha256")),
    )


@recipe_app.command("compile")
def compile_recipe_catalog(
    context: typer.Context,
    path: Annotated[Path, typer.Argument(exists=True, dir_okay=False, readable=True)],
    output: Annotated[
        Path | None, typer.Option(help="Write the canonical compiled installation document.")
    ] = None,
    source_map: Annotated[
        Path | None, typer.Option(help="Write auxiliary source locations.")
    ] = None,
) -> None:
    """Compile closed source against its exact local documents; make no network calls."""

    def compile_document() -> CompiledRecipeCatalog:
        destinations = [target.resolve() for target in (output, source_map) if target is not None]
        if len(set(destinations)) != len(destinations) or path.resolve() in destinations:
            raise ValueError("compiled output, source map and input paths must be distinct")
        catalog = load_recipe_catalog(path)
        locations = recipe_source_map(path) if source_map is not None else None
        if output is not None:
            output.write_bytes(
                canonical_json_bytes(catalog.model_dump(mode="json", by_alias=True)) + b"\n"
            )
        if source_map is not None and locations is not None:
            source_map.write_bytes(canonical_json_bytes(locations.model_dump(mode="json")) + b"\n")
        return catalog

    _call(_context(context), compile_document, table=("recipes", ("id", "revision", "sha256")))


@recipe_app.command("explain")
def explain_recipe_catalog(
    context: typer.Context,
    path: Annotated[Path, typer.Argument(exists=True, dir_okay=False, readable=True)],
) -> None:
    """Show normalized semantics, derived boundaries and the required task order."""
    _call(_context(context), lambda: load_recipe_catalog(path).explanation_document())


@work_app.command("list")
def list_work(
    context: typer.Context,
    page_size: int = typer.Option(25, min=1, max=100),
    page_token: str | None = typer.Option(None),
    phase: str | None = typer.Option(None),
    query: str | None = typer.Option(None, "--query", "-q"),
    sort: str = typer.Option("updated_at"),
    order: str = typer.Option("desc"),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.list_work(
            page_size=page_size,
            page_token=page_token,
            phase=cast(Any, phase),
            query=query,
            sort=cast(Any, sort),
            order=cast(Any, order),
        ),
        table=(
            "work",
            ("work_id", "phase", "result_kind", "target_state", "result_identity", "revision"),
        ),
    )


@work_app.command("create")
def create_work(
    context: typer.Context,
    recipe_id: str,
    inputs: Annotated[
        list[str],
        typer.Argument(help="Collection receipt as ID:ARCHIVE_ROOT_SHA256:CONTENT_IDENTITY"),
    ],
    preview_sha256: str = typer.Option(..., "--preview-sha256"),
    wait: bool = typer.Option(False, help="Poll the accepted initiation until it resolves."),
    wait_seconds: float = typer.Option(3600.0, min=0.1),
    revision: int | None = typer.Option(None),
    intent: Annotated[Path | None, typer.Option(exists=True, dir_okay=False)] = None,
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: _wait_initiation(
            state,
            state.client.create_work(
                recipe_id,
                _collection_roots(inputs),
                preview_sha256=preview_sha256,
                recipe_revision=revision,
                effective_intent=_document(intent),
            ),
            wait=wait,
            seconds=wait_seconds,
        ),
    )


@work_app.command("show")
def show_work(context: typer.Context, work_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.get_work(work_id))


@work_app.command("coordination")
def inspect_work_coordination(context: typer.Context, work_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.inspect_work_coordination(work_id))


@selection_app.command("show")
def get_artifact_selection(
    context: typer.Context,
    selection_sha256: str,
    continuation: str | None = typer.Option(None),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.get_artifact_selection(
            selection_sha256,
            continuation=continuation,
        ),
        table=("artifacts", ("id", "role", "path", "bytes")),
    )


@observation_app.command("list")
def list_observation_tasks(
    context: typer.Context,
    owner_id: str,
    owner_kind: str = typer.Option("work"),
    page_size: int = typer.Option(25, min=1, max=100),
    page_token: str | None = typer.Option(None),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.list_observation_tasks(
            owner_kind=_observation_owner(owner_kind),
            owner_id=owner_id,
            page_size=page_size,
            page_token=page_token,
        ),
    )


@observation_app.command("show")
def show_observation_task(
    context: typer.Context,
    owner_id: str,
    task_work_id: str,
    task_id: str,
    owner_kind: str = typer.Option("work"),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.get_observation_task(
            task_work_id, task_id, owner_kind=_observation_owner(owner_kind), owner_id=owner_id
        ),
    )


@observation_app.command("results")
def list_observation_results(
    context: typer.Context,
    owner_id: str,
    task_work_id: str,
    task_id: str,
    evidence_set_sha256: str,
    owner_kind: str = typer.Option("work"),
    start_ordinal: int = typer.Option(0, min=0),
    limit: int = typer.Option(100, min=1, max=100),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.get_observation_results(
            task_work_id,
            task_id,
            owner_kind=_observation_owner(owner_kind),
            owner_id=owner_id,
            evidence_set_sha256=evidence_set_sha256,
            start_ordinal=start_ordinal,
            limit=limit,
        ),
    )


@observation_app.command("result")
def show_observation_result(
    context: typer.Context, owner_id: str, request_id: str, owner_kind: str = typer.Option("work")
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.get_observation_result(
            request_id, owner_kind=_observation_owner(owner_kind), owner_id=owner_id
        ),
    )


@observation_app.command("view")
def show_observation_view(
    context: typer.Context,
    owner_id: str,
    task_work_id: str,
    task_id: str,
    view_id: str,
    view_sha256: str,
    owner_kind: str = typer.Option("work"),
    start_ordinal: int = typer.Option(0, min=0),
    limit: int = typer.Option(100, min=1, max=100),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.get_observation_view(
            task_work_id,
            task_id,
            view_id,
            owner_kind=_observation_owner(owner_kind),
            owner_id=owner_id,
            view_sha256=view_sha256,
            start_ordinal=start_ordinal,
            limit=limit,
        ),
    )


@work_app.command("step")
def step_work(context: typer.Context, work_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.step_work(work_id))


@work_app.command("retry")
def retry_work(context: typer.Context, work_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.retry_work(work_id))


@work_app.command("cancel")
def cancel_work(
    context: typer.Context,
    work_id: str,
) -> None:
    state = _context(context)
    _call(state, lambda: state.client.cancel_work(work_id))


@app.command("preview")
def preview(
    context: typer.Context,
    recipe_id: str,
    inputs: Annotated[
        list[str],
        typer.Argument(help="Collection receipt as ID:ARCHIVE_ROOT_SHA256:CONTENT_IDENTITY"),
    ],
    wait: bool = typer.Option(False, help="Poll this planning invocation until it resolves."),
    wait_seconds: float = typer.Option(3600.0, min=0.1),
    revision: int | None = typer.Option(None),
    intent: Annotated[Path | None, typer.Option(exists=True, dir_okay=False)] = None,
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: _wait_preview(
            state,
            state.client.preview_workflow(
                recipe_id,
                _collection_roots(inputs),
                recipe_revision=revision,
                effective_intent=_document(intent),
            ),
            wait=wait,
            seconds=wait_seconds,
        ),
    )


@app.command("preview-show")
def show_preview(context: typer.Context, job_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.get_workflow_preview(job_id))


@app.command("preview-cancel")
def cancel_preview(context: typer.Context, job_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.cancel_workflow_preview(job_id))


@work_app.command("initiation")
def show_work_initiation(context: typer.Context, job_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.get_work_initiation(job_id))


def _observation_owner(value: str) -> PlanningOwnerKind:
    if value not in closed_literal_values(PlanningOwnerKind):
        raise typer.BadParameter("observation owner must be work or preview")
    return cast(PlanningOwnerKind, value)


def _wait_preview(
    state: Context, status: PlanningJobStatus, *, wait: bool, seconds: float
) -> PlanningJobStatus:
    deadline = time.monotonic() + seconds
    while wait and status.state != "completed":
        if time.monotonic() >= deadline:
            return status
        time.sleep(max(0.0, min(1.0, deadline - time.monotonic())))
        status = state.client.get_workflow_preview(status.job_id)
    return status


def _wait_initiation(
    state: Context, status: WorkInitiationStatus, *, wait: bool, seconds: float
) -> WorkInitiationStatus:
    deadline = time.monotonic() + seconds
    while wait and status.state == "pending":
        if time.monotonic() >= deadline:
            return status
        time.sleep(max(0.0, min(1.0, deadline - time.monotonic())))
        status = state.client.get_work_initiation(status.job.job_id)
    return status


@evaluation_app.command("list")
def list_evaluations(
    context: typer.Context,
    page_size: int = typer.Option(25, min=1, max=100),
    page_token: str | None = typer.Option(None),
    phase: str | None = typer.Option(None),
    query: str | None = typer.Option(None, "--query", "-q"),
    sort: str = typer.Option("updated_at"),
    order: str = typer.Option("desc"),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.list_evaluations(
            page_size=page_size,
            page_token=page_token,
            phase=cast(Any, phase),
            query=query,
            sort=cast(Any, sort),
            order=cast(Any, order),
        ),
        table=("evaluations", ("evaluation_id", "phase", "revision")),
    )


@evaluation_app.command("create")
def create_evaluation(context: typer.Context, definition: Path) -> None:
    state = _context(context)
    _call(state, lambda: state.client.create_evaluation(_document(definition)))


@evaluation_app.command("show")
def show_evaluation(context: typer.Context, evaluation_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.get_evaluation(evaluation_id))


@evaluation_app.command("step")
def step_evaluation(context: typer.Context, evaluation_id: str) -> None:
    state = _context(context)
    _call(state, lambda: state.client.step_evaluation(evaluation_id))


@evaluation_app.command("cancel")
def cancel_evaluation(
    context: typer.Context,
    evaluation_id: str,
) -> None:
    state = _context(context)
    _call(state, lambda: state.client.cancel_evaluation(evaluation_id))


@evaluation_app.command("retry")
def retry_evaluation(
    context: typer.Context,
    evaluation_id: str,
    variant_id: str,
) -> None:
    state = _context(context)
    _call(state, lambda: state.client.retry_evaluation_variant(evaluation_id, variant_id))


@evaluation_app.command("review")
def review_evaluation(
    context: typer.Context,
    evaluation_id: str,
    variant_id: str,
    rating: int | None = typer.Option(None, min=1, max=5),
    note: str | None = typer.Option(None),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.review_evaluation_variant(
            evaluation_id,
            variant_id,
            rating=rating,
            note=note,
        ),
    )


@event_app.command("list")
def list_events(
    context: typer.Context,
    after: str | None = typer.Option(None),
    limit: int = typer.Option(100, min=1, max=100),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.list_events(after=after, limit=limit).model_dump(mode="json"),
        table=("events", ("id", "type", "subject", "occurred_at")),
    )


@scheduler_app.command("status")
def scheduler_status(context: typer.Context) -> None:
    state = _context(context)
    _call(state, state.client.scheduler_status)


@scheduler_app.command("run")
def scheduler_run(
    context: typer.Context,
    role: str = typer.Option("combined"),
    work_limit: int = typer.Option(25, min=1, max=100),
) -> None:
    state = _context(context)
    _call(
        state,
        lambda: state.client.run_scheduler(
            role=cast(Any, role),
            work_limit=work_limit,
        ),
    )


def _context(context: typer.Context) -> Context:
    if not isinstance(context.obj, Context):
        raise RuntimeError("stove0 CLI context was not initialized")
    return context.obj


def _document(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise typer.BadParameter("JSON document must contain an object")
    return value


def _collection_roots(values: list[str]) -> tuple[CollectionRootIdentityRef, ...]:
    roots: list[CollectionRootIdentityRef] = []
    for value in values:
        fields = value.split(":")
        if len(fields) != 3:
            raise typer.BadParameter(
                "collection receipt must be ID:ARCHIVE_ROOT_SHA256:CONTENT_IDENTITY"
            )
        collection_id, archive_root_sha256, artifact_set_identity = fields
        try:
            roots.append(
                CollectionRootIdentityRef.model_validate(
                    {
                        "collection_id": collection_id,
                        "archive_root_sha256": archive_root_sha256,
                        "artifact_set_identity": artifact_set_identity,
                    }
                )
            )
        except ValueError as exc:
            raise typer.BadParameter(f"invalid collection receipt: {value}") from exc
    return tuple(sorted(roots, key=lambda item: (item.collection_id, item.archive_root_sha256)))


def _call(
    state: Context,
    operation: Any,
    *,
    table: tuple[str, tuple[str, ...]] | None = None,
) -> None:
    try:
        payload = operation()
    except (Stove0ApiError, ValueError, OSError) as exc:
        _fail(str(exc))
    if isinstance(payload, BaseModel):
        payload = payload.model_dump(mode="json", by_alias=True)
    _render(payload, json_output=state.json_output, table=table)


def _render(
    payload: dict[str, Any],
    *,
    json_output: bool,
    table: tuple[str, tuple[str, ...]] | None,
) -> None:
    if json_output:
        typer.echo(canonical_json_bytes(payload).decode("utf-8"))
        return
    if table is not None and isinstance(payload.get(table[0]), list):
        if table[0] == "recipes" and payload.get("catalog_sha256") is not None:
            console.print(f"Catalog: {payload['catalog_sha256']}")
        rendered = Table(show_header=True)
        for column in table[1]:
            rendered.add_column(column.replace("_", " ").title())
        for item in payload[table[0]]:
            if not isinstance(item, dict):
                continue
            rendered.add_row(*(_table_value(item, column) for column in table[1]))
        console.print(rendered)
        if "next_page_token" in payload:
            console.print(f"Next page token: {payload.get('next_page_token') or '-'}")
        return
    console.print(Pretty(payload, expand_all=False))


def _table_value(item: dict[str, Any], column: str) -> str:
    if column in item:
        return str(item[column])
    recipe = item.get("recipe")
    if isinstance(recipe, dict) and column in {"id", "revision", "sha256"}:
        return str(recipe.get(column, ""))
    definition = item.get("definition")
    if isinstance(definition, dict) and column in {"id", "revision"}:
        return str(definition.get(column, ""))
    policy = item.get("policy")
    if isinstance(policy, dict) and column in {
        "id",
        "revision",
        "selector",
        "recipe_id",
    }:
        return str(policy.get(column, ""))
    intent = item.get("intent")
    if isinstance(intent, dict) and column in {"admission_id", "departure_id", "policy_id"}:
        return str(intent.get(column, ""))
    if isinstance(intent, dict) and column == "collection_id":
        collection = intent.get("collection")
        if isinstance(collection, dict):
            return str(collection.get("collection_id", ""))
    status = item.get("target_status")
    workflow = item.get("workflow_plan")
    branch_set = item.get("branch_set_plan")
    coordination = item.get("coordination_settlement")
    if column == "result_kind":
        if isinstance(branch_set, dict):
            return "coordination"
        if isinstance(workflow, dict) and workflow.get("result_kind") is not None:
            return str(workflow["result_kind"])
        if isinstance(status, dict):
            return (
                "external-effect"
                if status.get("protocol") == "stove0-effect-target/v1"
                else "collection"
            )
    if column == "target_state" and isinstance(status, dict):
        return str(status.get("state", ""))
    if column == "result_identity":
        if isinstance(coordination, dict):
            return str(coordination.get("settlement_sha256", ""))
        if isinstance(status, dict):
            receipt = status.get("effect_receipt")
            if isinstance(receipt, dict):
                return str(receipt.get("receipt_sha256", ""))
            output = status.get("output_collection")
            if isinstance(output, dict):
                return str(output.get("artifact_set_identity", ""))
    return ""


def _fail(message: str) -> NoReturn:
    typer.echo(message, err=True)
    raise typer.Exit(1)


def main() -> None:
    app()


__all__ = ["app", "main"]
