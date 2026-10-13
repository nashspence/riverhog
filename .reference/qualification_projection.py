"""Synthetic examples of sufficiency and two-way accounting, NOT Riverhog authority.

Reuses the earlier attribution example. Inputs stand for independently reviewed
owner requirements and already authenticated runner records. This module cannot
validate oracle truth, discover tests/contracts, inspect assertions, or grant
release permission. Its records and state names are illustrative, not a schema.
"""

from dataclasses import dataclass
from collections.abc import Mapping

from claim_projection import Binding, Claim, Identity, Result, support, required_checks_pass


@dataclass(frozen=True)
class Requirement:
    claim: Claim
    assertions: frozenset[tuple[str, str]]
    agreement_is_promised: bool = False

    def __post_init__(self) -> None:
        if not self.assertions or any(not node or not group for node, group in self.assertions):
            raise ValueError("a fixture proof requirement cannot be vacuously empty")


@dataclass(frozen=True)
class Oracle:
    claim: Claim
    authority: str
    kind: str = "owned-expectation"

    def __post_init__(self) -> None:
        if self.kind not in {"owned-expectation", "agreement", "self-derived", "unresolved"}:
            raise ValueError("unknown expectation provenance")
        if self.kind in {"owned-expectation", "agreement"} and not self.authority:
            raise ValueError("expected behavior needs its independently owned authority")


def qualify(
    requirement: Requirement,
    bindings: tuple[Binding, ...],
    results: tuple[Result, ...],
    selected: Identity,
    *,
    oracles: Mapping[tuple[str, str], Oracle],
    required_nodes: frozenset[str],
    run_exit_status: int = 0,
) -> dict[str, object]:
    """Show why an attributable assertion is not automatically sufficient proof.

    The fixture uses one conjunctive requirement. Production alternatives and
    composed witnesses must come from their owner, not from this demonstration.
    """
    if type(run_exit_status) is not int or run_exit_status < 0:
        raise ValueError("invalid enclosing run outcome")
    candidates = support(requirement.claim, bindings, results, selected)
    supported = frozenset(
        key for key in candidates
        if (oracle := oracles.get(key)) is not None
        and oracle.claim == requirement.claim
        and (oracle.kind == "owned-expectation"
             or (oracle.kind == "agreement" and requirement.agreement_is_promised))
    )
    execution_passed = run_exit_status == 0 and required_checks_pass(
        required_nodes, results, selected
    )
    missing = requirement.assertions - supported
    return {
        "execution_passed": execution_passed,
        "observed_assertions": sorted(candidates),
        "supported_assertions": sorted(supported),
        "missing_required_assertions": sorted(missing),
        "claim_status": "qualified" if execution_passed and not missing else "not_established",
    }


def reconcile(
    discovered_nodes: tuple[str, ...],
    purposes: Mapping[str, frozenset[str]],
    bindings: tuple[Binding, ...],
    claims: frozenset[Claim],
) -> dict[str, object]:
    """Two-way diagnostic over supplied discoveries, never a discovery algorithm.

    An unbound product-intent check needs owner review. A known promise without
    witnesses can remain an honestly reported proof gap after accounting closes.
    """
    nodes = set(discovered_nodes)
    if len(nodes) != len(discovered_nodes) or not nodes:
        raise ValueError("fixture discovery must be nonempty and unique")
    if set(purposes) - nodes or any(binding.node not in nodes for binding in bindings):
        raise ValueError("stale test identity; do not silently drop it")
    allowed = {"product", "implementation", "policy", "tooling", "unresolved"}
    if any(not roles or roles - allowed for roles in purposes.values()):
        raise ValueError("invalid explicit check purposes")
    if any(b.node in purposes and "unresolved" not in purposes[b.node]
           and b.purpose not in purposes[b.node] for b in bindings):
        raise ValueError("binding contradicts the reviewed check purpose")
    unclassified = sorted(node for node in nodes
                          if node not in purposes or "unresolved" in purposes[node])
    mapped_product_nodes = {b.node for b in bindings if b.purpose == "product" and b.claim in claims}
    unbound_product = sorted(node for node, roles in purposes.items()
                            if "product" in roles and node not in mapped_product_nodes)
    bound_claims = {b.claim for b in bindings if b.claim is not None}
    dangling = bound_claims - claims
    no_witness = claims - bound_claims
    return {
        "classification_complete": not unclassified,
        "relationship_review_needed": bool(unbound_product or dangling),
        "unclassified_nodes": unclassified,
        "unbound_product_nodes": unbound_product,
        "claims_without_candidate_witness": sorted(map(repr, no_witness)),
        "dangling_claim_bindings": sorted(map(repr, dangling)),
        "counts": {
            "nodes": len(nodes),
            "assertion_groups": len({(b.node, b.assertion) for b in bindings}),
            "discovered_claims": len(claims),
            "bound_claims": len(bound_claims & claims),
        },
    }
