# G02/G40 and G63/G64: approved policy to owner-level integration

**Authority:** [#964 maintainer decision 6118314165](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118314165).
**Status:** non-authoritative #903 integration reference, not a second contract,
not executable source and not evidence of product conformance. Earlier conversation
and family JSON are research. Inspected source for the underlying inventory:
main at 152d095038df0c0d0ed642cf265d47b11c65a754. Reconcile exact
post-#963 implementation source before accepting claims. Do not duplicate
the policy or the owner declarations in AGENTS.md.

## G02 — preserved structural and expressive validation

Approved: maintain existing schema/configuration functionality and named
profile-specific cost declarations. Do not silently narrow existing JSON Schema,
regular-expression, aliases/references, recursion or validation semantics to buy
strict linearity. New semantic exclusions require separate maintainer consent.

Bind exact public operations in the config-validation, riverhog-protocol,
riverhog-provenance-contracts and stove0-protocol owners. For each operation,
partition the genuinely reachable supported profiles, e.g.:

* Fixed maintained typed/structural schema: input bytes/nodes, nesting, field
  validation, canonicalization, and uniqueness/order work. Linear structural
  traversal only when demonstrated for the fixed schema and real validator.
* User-controlled schema or reference/alias expansion: measure program bytes
  and reference graph, input bytes/nodes, recursion/branching, matching behavior
  and any materialized state. A validated bound may legitimately exceed linear
  or depend on schema and input independently.
* Regex and nonstructural predicates: declare actual engine/profile and
  match/search/compile dimensions; never claim worst-case linearity of
  unqualified arbitrary patterns or assume a fast matching fixture proves it.
* Streaming parse/validation where possible: make the true byte/window behavior
  and any whole-instance global validation state explicit. Bounded steps do not
  mean bounded total state for inherently global uniqueness constraints.

Every profile states allowed semantic input, complete success/error boundary,
worst/amortized/expected/conditional status, RAM/scratch/state/retry and
capacity policy; omitted or incompatible reachable delegates block composite
qualification. Fail exact invalid material rather than return a partial document.
No arbitrary member count ceiling may be introduced by a parser budget.

Required integration witnesses: real schemas including nested/aliased/recursive
structures, deeply nested valid/invalid input, uniqueness, actual regex backtracking
or an explicitly qualified safe pattern profile, both empty/large inputs and
condition-dependent branches. Count useful program/node/byte work, not only
one happy-path elapsed time. Preserve current schema failures and exact identities.

## G40 — maintained fact and classification predicate language

Approved: keep the existing compiled recipe's logical expression power, including
same-record array quantifiers, nested Boolean rules and supported joins. Missing,
partial, ambiguous or unsupported accepted testimony must remain indeterminate or
error, not a complete negative enabling a weaker relationship tier.

Distinguish a fixed simple per-record predicate, indexed candidate selection, full
scan, nested record/array quantification, variable program size and complex joins.
Use separate dimensions for predicate AST P, selected facts/records and their bytes
F, subject scope S, indexed domain Z, necessary join/result cardinality J and
actual emitted output O. If fixed P allows one useful pass, declare it; if P varies
or nested loops multiply work, charge the real product and explicit index/sort.
A predicate-work dimension cannot simply equal an implementation's actual time
or self-created intermediate explosion.

Bind at stove0-protocol and stove0-core, not at a new policy engine. Qualify native
whole-scope/accepted-view readers from G38/G39 and predicate/decision callers from
the compiled #953 language. Test no-match scans, variable P independently from F,
nested quantification, same-record vs cross-record negative controls, ambiguous
evidence, cold/disk-backed state, replay, and necessary dense lawful results.

## G63 — internal source/contract generation and rendering budgets

No v1 end-user promise for total build/CI duration. Retain source-discovered
public method/route coverage and derived Contract Closure and Audit Records;
do not hand maintain a second registry of interfaces or approved bounds.
Internal budgets separately charge parsed source/schema/dependency bytes,
contract elements and references, actual HTML/output bytes, sort/index work,
build delegates, memory and temporary/retained artifacts. Do not traverse every
ancestor/interface descendant afresh per rendered relationship.

A public API shipped by tooling still gets its real public owner (such as G61);
only the nonproduct generator/qualification orchestration receives internal G63
budgets. Include negative tests for missing/disconnected/duplicate/invalid
mappings, source drift, report omissions, reference fan-out and repeated
crosslink expansion. Render scopes, exceptions and evidence status legibly.

## G64 — one growth model and accountable evidence

Approved: one typed, closed, dimensionally checked expression schema with fixed
arithmetic/operators and named index/order/delegate costs. Normative declarations
live beside each owning operation, with stable ID, scope, units, assumptions,
completion and compositional delegation. Existing Contract Closure projects
the normative source; separately generated Audit records hold source trace,
actual evidence, applicability, unresolved cases and results. #963 is the
automatic measurement/target authority; #469 consumes owner-approved
growth declarations and executed witnesses. No duplicate work/target registry.

Per public operation, derive exactly one of: owns declaration; inherits exact
declaration plus wrapper cost; delegates a clearly bounded portion to a qualified
component; or is demonstrably nonexecutable with reason. Discover inherited
methods, dynamic routes and generated operations at integration time. The
64-family inventory is only scope research, not complete dynamic coverage.

Preserve four **separate** statuses: functional, growth conformance, evidence
integrity, and elapsed time. Required missing, stale, truncated, noncomparable or
wrong-source evidence is never a pass; a valid elapsed-time target miss is
report-only and needs an explicit maintainer disposition. Growth violations block
qualification. Instrument without performing workloads twice only for timing.
Measure the real operation/call multiplicity, SQL/byte/counter events and child/
container peaks under named conditions, including failures and cleanup.
Adversarial tests must vary dimensions separately, exercise cached/cold/evicted
working sets, exact completion, retry/cancel/recovery and actual supported
backend/provider regimes.

## Decisions and end-user meaning

Approved: preserved complex configurations/workflows; truthful, profile-scoped
resource expectations; explicit capacity outcomes; inspectable tested contracts
and stricter regression accountability. End users do not lose expressive power
or silently receive false negative classifications. There is no new numerical
latency service guarantee.

No further *general policy/architecture vote* is needed for G02/G40/G63/G64.
An integrator must still establish owner-specific formulas, exact expression
codec, executable Closure bindings, runtime profile witnesses, numerical
conditions, and post-#963 exact-SHA qualification. If implementation reveals
a materially different semantic/external-authority trade-off, submit that
specific decision to the maintainer instead of silently restricting behavior.

G38/G39's separately approved current-only v2 contract is in
[../g38_g39/DESIGN.md](../g38_g39/DESIGN.md); its exact source and
qualification owners remain distinct. G26 and G27/G28 were subsequently approved in #964 comment 6118482544;
see [the current decision directory](../integration/DECISIONS.md). G22 design
is supplied; exact implementation/proof remains integration work.
