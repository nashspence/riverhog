"""Growth scopes for scheduling, original execution and separate receiving.

The callback graph is specialized to the Native journal owner. A submission is
not a delegate invocation of the work it schedules: worker roots are correlated
by their original context and physical slot in execution evidence.
"""
from __future__ import annotations

from riverhog_growth_contracts import (
    COUNT, OCTETS, Bound, Declaration, Dimension, Expression, declare_growth, growth_binding,
)
from riverhog_core.growth_bounds import bound_envelope
from .archive_construction_retention import ArchiveConstructionRetention as Retention
from .journal_retirement_registry import (
    JournalRetirementRegistry as Registry, JournalRetirementTurn as Turn,
    format_retirement_blocker,
)
from .physical_execution import PhysicalExecutionOwner as Execution, _PhysicalExecutionResult, release_exception_graph


def c(value, unit=COUNT):
    return Expression("constant", value=value, unit=unit)


def d(name):
    return Expression("dimension", name=name)


def scale(value, term):
    return Expression("product", terms=(c(value, ()), term))


def add(*terms):
    return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)


def _bind(operation, name, dimensions, bounds, *, children=(), limits=None, scope):
    """Owner-local numeric envelope using the existing heterogeneous-call pattern."""
    child_dimensions, child_bounds = [], []
    for tag, child in children:
        declaration = growth_binding(child).declaration
        child_dimensions.append(Dimension(tag + "_calls", COUNT, "Actual callback invocations."))
        child_bounds.append(Bound(tag + "_calls", COUNT, "equal", d(tag + "_calls"),
                                  "Actual samples must contain every callback, including failures."))
        if limits and tag in limits:
            child_bounds.append(Bound(tag + "_calls_limit", COUNT, "upper", limits[tag],
                                      "Independently derived source branch/loop bound."))
        child_dimensions.extend(Dimension(tag + "_" + item.name, item.unit,
            "Componentwise dominating actual child input: " + item.meaning)
            for item in declaration.dimensions)
        arguments = {item.name: d(tag + "_" + item.name) for item in declaration.dimensions}
        for bound in declaration.bounds:
            child_bounds.append(Bound(tag + "_" + bound.metric, bound.unit, "upper",
                Expression("product", terms=(c(1, (("count", -1),)), d(tag + "_calls"),
                           bound_envelope(child, bound.metric, arguments))),
                "Sum of actual child metrics; validate each child's inputs and numeric envelope."))
    declare_growth(Declaration(
        id="riverhog-server.journal-retirement-" + name + ".growth/v1",
        authority="riverhog-server",
        subjects=(operation.__module__ + "." + operation.__qualname__,),
        scope=scope,
        starts_at="receive admitted original identities and this phase's independent inputs",
        ends_at="return or raise from this phase, retaining partial and exceptional work",
        dimensions=tuple(dimensions) + tuple(child_dimensions),
        bounds=tuple(bounds) + tuple(child_bounds),
        conditions=(
            "The maintained journal callback family is used; worker and coordinator scopes never overlap in attribution.",
            "Child samples independently validate actual inputs; componentwise envelopes dominate every invocation.",
            "Production logging sinks remain independently owned; no arbitrary slow sink is licensed by an entry count.",
        ),
        invariants=(
            "Ordinary original retention, terminal physical capacity, resource receiving and worker join are distinct.",
            "A return, exception, Future completion or join alone does not confirm resource receiving.",
            "An original invocation has one evidence identity; no attribution by wall-clock overlap or replacement actor.",
        ),
        limitations=(
            "Primitive counts exclude interpreter hashing, OS latency, storage allocation and aggregate memory.",
            "The joined transcript composes separate roots; a scheduling root does not contain future worker execution.",
        ),
    ), delegates=tuple(children))(operation)


def dim(name, unit=COUNT, meaning="Independent admitted input for this phase."):
    return Dimension(name, unit, meaning)


def upper(name, expression, unit=COUNT, meaning="Source-derived primitive envelope."):
    return Bound(name, unit, "upper", expression, meaning)


def declare_journal_retirement_growth(cleanup):
    _bind(Registry._scope, "scope", (dim("control_octets", OCTETS),), (
        upper("canonical_calls", c(2)), upper("hash_calls", c(1)),
        upper("encoded_octets", scale(2, d("control_octets")), OCTETS),
        upper("hashed_octets", d("control_octets"), OCTETS),
    ), scope="complete original scope serialization, metadata admission and exact hashing")
    _bind(Registry._accept_progress, "accept", (dim("outcomes"),), (
        upper("type_checks", c(1)), upper("payload_octets", c(0, OCTETS), OCTETS),
    ), scope="one exact bool progress validation, not resource receiving")
    _bind(Registry._create, "create", (dim("originals"),), (
        upper("actors_created", c(1)), upper("resource_opens", c(0)),
    ), scope="one passive original journal actor construction; failure is retained")
    _bind(Registry.retained, "retained", (dim("retained_contexts"),), (
        upper("lookups", c(1)), upper("enumerations", c(0)),
    ), scope="one nonblocking original-context membership lookup")

    _bind(Registry.collect, "collect", (dim("limit"), dim("retained_contexts")), (
        upper("completion_visits", d("limit")), upper("withdrawal_visits", d("limit")),
        upper("bookkeeping_events", scale(2, d("limit"))),
        upper("error_log_calls", d("limit")), upper("error_frame_release_calls", c(0)),
        upper("actor_close_calls", c(0)),
    ), children=(("physical", Execution.collect_workspace), ("retire", Retention.retire),
                 ("accept", Registry._accept_progress)),
       limits={"physical": c(1), "retire": d("limit"), "accept": d("limit")},
       scope="coordinator: bounded physical outcome receiving and ordinary reservation withdrawal")
    _bind(Registry.resume_ready, "resume", (dim("limit"), dim("retained_contexts")), (
        upper("context_visits", d("limit")), upper("bookkeeping_events", d("limit")),
        upper("actor_close_calls", c(0)),
    ), children=(("physical", Execution.try_submit_workspace),),
       limits={"physical": d("limit")}, scope="coordinator: submit original close-only retries, never execute them")
    _bind(Registry.submit, "submit", (dim("retained_contexts"),), (
        upper("bookkeeping_events", c(1)), upper("context_creations", c(1)),
        upper("actor_creations", c(0)), upper("status_calls", c(1)),
    ), children=(("scope", Registry._scope), ("retain", Retention.try_retain_workspace),
                 ("physical", Execution.try_submit_workspace)),
       limits={"scope": c(1), "retain": c(1), "physical": c(1)},
       scope="coordinator: exact metadata and ordinary reservation before terminal worker submission")

    # Namespace/file callbacks are the concrete journal retirement family. The
    # full FileReclamationOwner receiver retains its existing primitive contract;
    # these entry counters cover additional final receiving outside _advance.
    _bind(Turn._receive, "receive", (dim("callbacks"), dim("resources")), (
        upper("file_close_entries", add(c(1), d("callbacks"))), upper("namespace_close_entries", c(1)),
        upper("callback_invocations", d("callbacks")),
        upper("native_close_calls", d("resources")), upper("resource_opens", c(0)),
    ), scope="worker: original file and reverse-dependent namespace receiving, including refusal")
    _bind(format_retirement_blocker, "format", (dim("error_octets", OCTETS),), (
        upper("format_calls", c(1)), upper("formatted_octets", d("error_octets"), OCTETS),
        upper("retained_failure_octets", c(4000, OCTETS), OCTETS),
    ), scope="worker: complete error formatting before 1000-character blocker slice")
    _bind(Turn.advance, "worker", (dim("octet_budget", OCTETS),), (
        upper("work_entries", c(1)), upper("receive_entries", c(1)),
        upper("blocker_entries", c(1)),
    ), children=(("unit", cleanup._advance), ("receive", Turn._receive),
                 ("format", format_retirement_blocker), ("blocked", cleanup._blocked)),
       limits={tag: c(1) for tag in ("unit", "receive", "format", "blocked")},
       scope="original worker quantum through real unit, receiving and real blocker handling")
    _bind(Turn.close, "close", (dim("originals"),), (
        upper("receive_entries", c(1)), upper("work_entries", c(0)),
    ), children=(("receive", Turn._receive),), limits={"receive": c(1)},
       scope="original close-only worker; no repeated file mutation or reconstructed actor")

    _bind(release_exception_graph, "exception-graph", (
        dim("exception_nodes"), dim("exception_edges"), dim("traceback_frames"),
    ), (
        upper("exception_visits", add(c(1), d("exception_edges"))),
        upper("frame_clear_calls", d("exception_nodes")),
        upper("traceback_frames", d("traceback_frames")),
    ), scope="execution-only joined exception graph/frame cleanup; finalizers retain their owning costs")
    _bind(_PhysicalExecutionResult.release_exception_frames, "exception-frames", (dim("outcomes"),),
        (upper("graph_entries", c(1)),), children=(("graph", release_exception_graph),),
        limits={"graph": c(1)}, scope="worker: receive the retained original outcome's exception frames")
    _bind(Registry._run, "dispatch", (dim("originals"),), (
        upper("work_or_close_entries", c(1)), upper("create_entries", c(1)),
    ), children=(("create", Registry._create), ("work", Turn.advance),
                 ("close", Turn.close), ("frames", _PhysicalExecutionResult.release_exception_frames),
                 ("diagnostic", release_exception_graph)),
       limits={**{tag: c(1) for tag in ("create", "work", "close", "frames")}, "diagnostic": c(2)},
       scope="one original physical callback, forward quantum or close-only receiving")

    _bind(cleanup.advance, "schedule", (dim("unit_limit"), dim("octet_budget", OCTETS)), (
        upper("bookkeeping_events", scale(4, d("unit_limit")), meaning=
              "At most L received outcomes + L reservation withdrawals + L close submissions + L new submissions."),
        upper("outer_iterations", d("unit_limit")),
        upper("candidate_iterations", scale(3, d("unit_limit"))),
        upper("excluded_jobs_per_call", c(2)),
        upper("exclusion_copy_references", scale(6, d("unit_limit"))),
        upper("inline_worker_entries", c(0)), upper("inline_close_entries", c(0)),
    ), children=(("select", cleanup._select), ("collect", Registry.collect),
                 ("resume", Registry.resume_ready), ("retained", Registry.retained),
                 ("submit", Registry.submit)),
       limits={"select": scale(3, d("unit_limit")), "collect": c(1), "resume": c(1),
               "retained": scale(3, d("unit_limit")), "submit": scale(3, d("unit_limit"))},
       scope="one finite asynchronous scheduling call, ending without a worker wait")
