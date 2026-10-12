"""Synthetic decision-boundary example, not a production schema or proof engine.

Inputs represent already resolved owner declarations and already authenticated
runner evidence. This module does not discover contracts, collect pytest, verify
artifact custody, infer purpose, or authorize any Riverhog qualification.
"""

from dataclasses import dataclass

PURPOSES = frozenset({"product", "implementation", "policy", "tooling", "unresolved"})


@dataclass(frozen=True)
class Identity:
    source: str
    run: str
    work: str
    conditions: str


@dataclass(frozen=True)
class Claim:
    owner: str
    subject: str
    predicate: str
    scope: str


@dataclass(frozen=True)
class Binding:
    node: str
    assertion: str
    purpose: str
    claim: Claim | None = None

    def __post_init__(self) -> None:
        if not self.node or not self.assertion or self.purpose not in PURPOSES:
            raise ValueError("binding requires explicit identity and known purpose")
        if (self.purpose == "product") != (self.claim is not None):
            raise ValueError("only resolved product bindings carry product claims")
        if self.claim and not all(vars(self.claim).values()):
            raise ValueError("product claim has an unresolved authority or scope")


@dataclass(frozen=True)
class Result:
    identity: Identity
    node: str
    assertions: frozenset[str]
    phases: tuple[str, str, str] = ("passed", "passed", "passed")
    complete: bool = True
    was_xfail: bool = False
    valid: bool = True


def _results(results: tuple[Result, ...], expected: Identity) -> dict[str, Result]:
    """One explicitly selected attempt; never union results across runs/retries."""
    indexed: dict[str, Result] = {}
    if not all(vars(expected).values()):
        raise ValueError("expected execution identity is incomplete")
    for result in results:
        if result.node in indexed or not result.node:
            raise ValueError("duplicate or empty execution node")
        if result.identity != expected or not result.valid:
            raise ValueError("invalid or mismatched execution evidence")
        if len(result.phases) != 3 or any(
            phase not in {"passed", "failed", "skipped", "missing"} for phase in result.phases
        ):
            raise ValueError("invalid phase evidence")
        indexed[result.node] = result
    return indexed


def _passed(result: Result) -> bool:
    return result.complete and not result.was_xfail and result.phases == ("passed",) * 3


def support(
    claim: Claim,
    bindings: tuple[Binding, ...],
    results: tuple[Result, ...],
    expected: Identity,
) -> frozenset[tuple[str, str]]:
    """Return distinct successful assertion witnesses for this exact scoped claim."""
    if len(bindings) != len(set(bindings)):
        raise ValueError("duplicate binding")
    indexed = _results(results, expected)
    known: dict[str, set[str]] = {}
    for binding in bindings:
        known.setdefault(binding.node, set()).add(binding.assertion)
    for node, result in indexed.items():
        if node not in known or not result.assertions <= known[node]:
            raise ValueError("result has an unbound node or assertion")
    return frozenset(
        (binding.node, binding.assertion)
        for binding in bindings
        if binding.purpose == "product"
        and binding.claim == claim
        and (result := indexed.get(binding.node)) is not None
        and _passed(result)
        and binding.assertion in result.assertions
    )


def required_checks_pass(
    required_nodes: frozenset[str], results: tuple[Result, ...], expected: Identity
) -> bool:
    """Purpose does not waive required execution; this is NOT product qualification."""
    indexed = _results(results, expected)
    return all(node in indexed and _passed(indexed[node]) for node in required_nodes)
