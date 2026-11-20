"""Source-owned coordination costs for independent workspace execution."""

from collections.abc import Callable
from typing import Any

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)


def declare_workspace_execution_growth(operation: Callable[..., Any], *, collect: bool) -> None:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    quantum = Expression("dimension", name="quantum") if collect else count(1)
    steps = Expression("sum", terms=(scale(24, quantum), count(4)))
    limits = {
        "mapping_steps": steps,
        "mapping_enumerations": count(0),
        "workers_constructed": count(0) if collect else count(1),
        "workers_started": count(0) if collect else count(1),
        "worker_liveness_calls": scale(2, quantum) if collect else count(0),
        "worker_join_calls": quantum if collect else count(0),
        "results_received": quantum if collect else count(0),
    }
    name = "collect_workspace" if collect else "try_submit_workspace"
    declaration = Declaration(
        id="riverhog-server.physical-workspace-"
        + ("collect" if collect else "submit")
        + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.services.physical_execution.PhysicalExecutionOwner." + name,),
        scope="one original workspace " + ("receiving poll" if collect else "submission"),
        starts_at="receive exact original identities and configured shared capacity",
        ends_at="return original joined outcomes, a retained submission or explicit deferral",
        dimensions=(
            Dimension("retained_slots", COUNT, "Independent retained original physical slots."),
            Dimension("quantum", COUNT, "Positive receiving limit; one for a submission."),
            Dimension(
                "control_octets_per_step",
                OCTETS_PER_COUNT,
                "Complete widest mapping key/control width per primitive.",
            ),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "equal" if metric == "mapping_enumerations" else "upper",
                    expression,
                    "Actual mapping, thread and receiving primitives.",
                )
                for metric, expression in limits.items()
            ),
            Bound(
                "mapping_key_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        steps,
                        Expression("dimension", name="control_octets_per_step"),
                    ),
                ),
                "Complete actual keys of every observed mapping primitive.",
            ),
            Bound(
                "payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Coordination never inspects resource or outcome bodies.",
            ),
        ),
        conditions=(
            "Configured capacity bounds retained slots and queued submission.",
            "Exact workspace controls have a separate local receiver.",
            "Work callbacks and thread latency retain execution ownership.",
        ),
        invariants=(
            "Workspace work shares global and per-target terminal allowances.",
            "Only the selected receiver's rotating bucket is inspected.",
            "Published Future results stay charged until the actual worker joins.",
            "Retries preserve the original receiver, target and identity.",
            "Execution join grants no resource or terminal disposal authority.",
        ),
        limitations=(
            "Mapping/hash internals and thread latency remain backend work.",
            "Callbacks, handle receiving and resource ledgers have separate scopes.",
        ),
    )
    declare_growth(declaration)(operation)
