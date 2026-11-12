"""Native retirement's separate coordination, worker and receiving contracts.

Submission is an asynchronous handoff, not an inline execution delegate. A
worker sample belongs to the original context and exact physical invocation;
collection is a later call and does not repeat the worker's file observations.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    Unit,
    declare_growth,
    growth_binding,
)

from riverhog_core.growth_bounds import bound_envelope

if TYPE_CHECKING:
    from .upload_journal_validation_retirement import UploadJournalValidationRetirement

_Limit = tuple[str, Unit, Literal["upper", "equal"], Expression, str]
_Child = tuple[str, Callable[..., Any], Expression]


def _count(value: int, unit: Unit = COUNT) -> Expression:
    return Expression("constant", value=value, unit=unit)


def _dimension(name: str) -> Expression:
    return Expression("dimension", name=name)


def _scale(value: int, term: Expression) -> Expression:
    return Expression("product", terms=(_count(value, ()), term))


def _add(*terms: Expression) -> Expression:
    return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)


def _product(calls: Expression, term: Expression) -> Expression:
    return Expression("product", terms=(_count(1, (("count", -1),)), calls, term))


def _binding(
    operation: Callable[..., Any],
    name: str,
    scope: str,
    dimensions: tuple[tuple[str, Unit, str], ...],
    limits: tuple[_Limit, ...],
    children: tuple[_Child, ...] = (),
) -> None:
    """Compose only invocations within this exact synchronous scope.

    Heterogeneous actual child samples retain their independent inputs. Their
    componentwise maxima dominate numeric child envelopes, as in catalog/unit
    composition. Call caps are independently derived from the owning loops.
    """
    bindings = {tag: growth_binding(child) for tag, child, _ in children}
    child_bounds = []
    for tag, child, cap in children:
        binding = bindings[tag]
        arguments = {
            item.name: _dimension(tag + "_" + item.name) for item in binding.declaration.dimensions
        }
        child_bounds.extend(
            (
                Bound(
                    tag + "_calls",
                    COUNT,
                    "equal",
                    _dimension(tag + "_calls"),
                    "All actual invocation prefixes, including exceptions.",
                ),
                Bound(
                    tag + "_calls_limit",
                    COUNT,
                    "upper",
                    cap,
                    "Source control-flow cap, not a fitted observed maximum.",
                ),
            )
        )
        child_bounds.extend(
            Bound(
                tag + "_" + bound.metric,
                bound.unit,
                "upper",
                _product(
                    _dimension(tag + "_calls"), bound_envelope(child, bound.metric, arguments)
                ),
                "Sum of this child's actual metrics, counted once in this scope.",
            )
            for bound in binding.declaration.bounds
        )
    declaration = Declaration(
        id="riverhog-server.journal-retirement-" + name + ".growth/v1",
        authority="riverhog-server",
        subjects=(operation.__module__ + "." + operation.__qualname__,),
        scope=scope,
        starts_at="entry with the original identities and independently admitted inputs",
        ends_at="return or exceptional exit of this invocation, not another worker's completion",
        dimensions=(
            Dimension(
                "other_workspaces",
                COUNT,
                "Retained workspace records at entry; observed without enumeration.",
            ),
            *(Dimension(key, unit, meaning) for key, unit, meaning in dimensions),
            *(
                Dimension(tag + "_calls", COUNT, "Actual calls including exceptional prefixes.")
                for tag in bindings
            ),
            *(
                Dimension(
                    tag + "_" + item.name,
                    item.unit,
                    "Componentwise upper envelope of actual child inputs. " + str(item.meaning),
                )
                for tag, binding in bindings.items()
                for item in binding.declaration.dimensions
            ),
        ),
        bounds=(
            *(
                Bound(metric, unit, relation, bound, meaning)
                for metric, unit, relation, bound, meaning in limits
            ),
            *child_bounds,
        ),
        conditions=(
            "Original resource, namespace and ordinary retention ownership is unchanged.",
            "Actual worker identity/join and positive resource receiving are distinct predicates.",
            "Numeric child envelopes are monotone; absent child envelopes are zero.",
            "The observer records exceptions as executed prefixes, never as successful work.",
        ),
        invariants=(
            "No asynchronous worker effect is charged as inline scheduling work.",
            "A submitted, joined, received or withdrawn item is not necessarily file progress.",
            "No failed original is reconstructed; original close-only retries remain explicit.",
        ),
        limitations=(
            "Primitive counts exclude interpreter/hash/index internals and backend latency.",
            "Logical scope evidence is not a whole-service memory, latency or archive proof.",
            "Logging and finalizer latency need a separately responsive backend profile.",
        ),
    )
    declare_growth(declaration, delegates=tuple((tag, child) for tag, child, _ in children))(
        operation
    )


def _upper(metric: str, value: Expression, meaning: str, unit: Unit = COUNT) -> _Limit:
    return metric, unit, "upper", value, meaning


def _zero(metric: str, meaning: str, unit: Unit = COUNT) -> _Limit:
    return metric, unit, "equal", _count(0, unit), meaning


def declare_async_retirement_growth(owner: type[UploadJournalValidationRetirement]) -> None:
    """Bind the exact Native owner after its catalog/unit delegates are declared."""
    from .archive_construction_retention import ArchiveConstructionRetention as Retention
    from .journal_retirement_registry import JournalRetirementRegistry as Registry
    from .journal_retirement_registry import JournalRetirementTurn as Turn
    from .physical_execution import PhysicalExecutionOwner as Physical
    from .physical_execution import _PhysicalExecutionResult as Result

    _binding(
        Registry._scope,
        "scope",
        "one exact Native scope/control preparation",
        (
            (
                "control_octets",
                OCTETS,
                "Full canonical normalized job/root/target control width, before admission.",
            ),
        ),
        (
            _upper("encode_calls", _count(2), "Admission encoding plus exact scope encoding."),
            _upper(
                "encoded_octets",
                _scale(2, _dimension("control_octets")),
                "Both complete encodings; rejection may stop after the first.",
                OCTETS,
            ),
            _upper(
                "hashed_octets",
                _dimension("control_octets"),
                "Exactly scoped digest input; no source payload.",
                OCTETS,
            ),
            _zero("payload_octets", "No source content in scope controls.", OCTETS),
        ),
    )
    _binding(
        Registry._accept_progress,
        "accept",
        "one typed original progress result check",
        (),
        (
            _upper(
                "result_checks", _count(1), "Exactly one boolean type check; no receipt inferred."
            ),
            _zero("payload_octets", "No resource content is read.", OCTETS),
        ),
    )
    _binding(
        Registry._create,
        "create",
        "passive original Native turn allocation",
        (),
        (
            _upper(
                "actors_constructed",
                _count(1),
                "One passive turn, never a retry of a failed constructor.",
            ),
            _zero("file_opens", "Resource records precede every open."),
        ),
    )
    _binding(
        Turn._receive,
        "receive",
        "one original file/namespace receiving attempt",
        (("callbacks", COUNT, "Callbacks retained on the original handle owner at entry."),),
        (
            _upper(
                "callbacks_limit",
                _count(5),
                "Four namespace resources and the one extent receiver.",
            ),
            _upper(
                "callback_invocations",
                _dimension("callbacks"),
                "Reverse dependent suffix stops on first failure.",
            ),
            _upper(
                "native_close_calls",
                _add(_count(2), _dimension("callbacks")),
                "Two extent resources plus at most one per namespace callback.",
            ),
            _zero("native_open_calls", "Receiving never reconstructs acquisition."),
            _zero("native_stat_calls", "A pathname/inode lookup is not a receiving receipt."),
            _zero("native_dup_calls", "Receiving never adopts a recycled descriptor."),
            _zero("mutating_file_calls", "Close-only receiving never repeats truncation/unlink."),
            _upper(
                "positive_receive",
                _count(1),
                "Positive only after every actual dependent receiver succeeds.",
            ),
        ),
    )
    _binding(
        Turn.advance,
        "turn",
        "one original Native worker quantum including error/receiving work",
        (
            (
                "error_octets",
                OCTETS,
                "Full formatting before slicing, not merely the stored 1000-character suffix.",
            ),
        ),
        (
            _upper(
                "error_format_calls",
                _count(1),
                "The handled storage/custody exception is formatted once.",
            ),
            _upper(
                "error_formatted_octets",
                _dimension("error_octets"),
                "Full error representation is charged.",
                OCTETS,
            ),
            _upper(
                "progress_returns",
                _count(1),
                "One actual boolean progress result, not the scheduler return count.",
            ),
        ),
        (
            ("unit", owner._advance, _count(1)),
            ("receive", Turn._receive, _count(1)),
            ("blocked", owner._blocked, _count(1)),
        ),
    )
    _binding(
        Turn.close,
        "close",
        "one original close-only worker retry",
        (),
        (_zero("work_reconstructions", "No call to original advance or acquisition."),),
        (("receive", Turn._receive, _count(1)),),
    )
    _binding(
        Registry._run,
        "worker",
        "one original registry work callback on its physical worker",
        (),
        (_upper("context_entries", _count(1), "One original context; no inline scheduling work."),),
        (
            ("create", Registry._create, _count(1)),
            ("turn", Turn.advance, _count(1)),
            ("close", Turn.close, _count(1)),
        ),
    )

    # These generic helpers have one owner; do not bind a Native callback graph
    # to a shared registry method used by a different retirement actor.
    _binding(
        Physical.status,
        "physical-status",
        "one exact physical slot point read",
        (),
        (
            _upper(
                "status_calls",
                _count(1),
                "One nonblocking observation; no global closure inference.",
            ),
            _upper("worker_liveness_calls", _count(1), "At most one exact worker is_alive probe."),
        ),
    )
    _binding(
        Result.release_exception_frames,
        "exception-receive",
        "joined original exception graph receiving",
        (
            (
                "exception_nodes",
                COUNT,
                "Distinct nodes in the original cause/context/group graph at entry.",
            ),
            (
                "exception_edges",
                COUNT,
                "All original graph references, including repeated/cyclic edges.",
            ),
            (
                "traceback_frames",
                COUNT,
                "Sum of original traceback chain lengths; shared tails are charged per node.",
            ),
        ),
        (
            _upper(
                "clear_frames_calls",
                _dimension("exception_nodes"),
                "One clear_frames per distinct exception.",
            ),
            _upper(
                "traceback_frame_visits",
                _dimension("traceback_frames"),
                "Actual traceback traversal; no constant-per-outcome claim.",
            ),
            _upper(
                "graph_edges",
                _dimension("exception_edges"),
                "Repeated edges remain counted even with deduplication.",
            ),
            _zero("original_work_calls", "Frame receiving never executes original Native work."),
        ),
    )

    L = _dimension("unit_limit")
    children = (
        ("select", owner._select, _scale(3, L)),
        ("physical_collect", Physical.collect_workspace, _count(1)),
        ("physical_submit", Physical.try_submit_workspace, _scale(4, L)),
        ("retain", Retention.try_retain_workspace, _scale(3, L)),
        ("retire", Retention.retire, L),
        ("scope", Registry._scope, _scale(3, L)),
        ("accept", Registry._accept_progress, L),
        ("exception_receive", Result.release_exception_frames, _scale(2, L)),
        ("status", Physical.status, _scale(4, L)),
    )
    _binding(
        owner.advance,
        "schedule",
        "one non-waiting Native retirement coordination invocation",
        (
            ("unit_limit", COUNT, "max(0, supplied integer limit); independent of work observed."),
            (
                "octet_budget",
                OCTETS,
                "Positive per-original file admission, not used to infer completed bytes.",
            ),
            (
                "log_control_octets",
                OCTETS,
                "Largest full log type/identity controls, excluding exception-body formatting.",
            ),
        ),
        (
            _upper("outer_iterations", L, "At most one new submission per outer iteration."),
            _upper(
                "candidate_iterations",
                _scale(3, L),
                "At most three candidates per outer iteration.",
            ),
            _upper(
                "exclusion_references",
                _scale(3, L),
                "The exact tuple widths are 0+1+2 per complete candidate group.",
            ),
            _upper(
                "context_mapping_steps",
                _add(_scale(24, L), _count(12)),
                "Registry control primitives; physical and ledger mappings are separate delegates.",
            ),
            _upper(
                "context_rotations", _scale(2, L), "At most L in collection and L in resumption."
            ),
            _zero("context_enumerations", "No backlog census in coordination."),
            _upper(
                "collected_outcomes",
                L,
                "Actual joined completion delivery, even if result/receiving fails.",
            ),
            _upper(
                "withdrawn_reservations",
                L,
                "Exact previously received originals, possibly from an earlier poll.",
            ),
            _upper(
                "resumed_closes", L, "Actual close-only worker submissions, not completed closes."
            ),
            _upper(
                "new_submissions", L, "Accepted new physical submissions, not completed quanta."
            ),
            _upper(
                "scheduling_events",
                _scale(4, L),
                "collect<=2L + resume<=L + new<=L, with separately checked phase equality.",
            ),
            _upper("log_calls", L, "At most one diagnostic per collected exception/mismatch."),
            _upper(
                "log_control_octets_total",
                _product(L, _dimension("log_control_octets")),
                "Actual control/log formatting inputs; sink has its own latency.",
                OCTETS,
            ),
            _zero(
                "inline_worker_calls",
                "No Native unit, blocker formatting or close-only worker is invoked inline.",
            ),
            _zero(
                "inline_file_calls",
                "Coordination does not perform workspace file I/O; log sink excluded explicitly.",
            ),
            _zero(
                "waiting_joins",
                "Only the physical collector's zero-wait exact worker join is allowed.",
            ),
        ),
        children,
    )
