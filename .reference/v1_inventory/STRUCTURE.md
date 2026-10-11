# Proposed v1 growth contract structure

**Status:** owner-level growth declaration proposal, not a completed public contract or current runtime conformance. The separate **G26 derived-index/extraction identity revision and pre-v1 no-old-schema-migration hard cut are approved, with Alembic intentionally retained** by [#964 decision 6112668478](https://github.com/nashspence/riverhog/issues/964#issuecomment-6112668478). Other individual formulas still need exact owner proof.

**Audited source:** `nashspence/riverhog@152d095038df0c0d0ed642cf265d47b11c65a754`, tree `991e994457b487a7dca016b7fc0409485cef25c6`.

## Recommendation

Adopt owner-scoped growth declarations for the public operations in `INVENTORY.md`. Keep internal engineering budgets separate. Do not place a blanket “the whole repository is O(number of files)” sentence in the Closure.

This proposal identifies 62 public declaration families and two internal budget families, mapping all 86 workspace distributions. “Family” is a review grouping, not a new source of runtime authority. One family may resolve into multiple exact operation elements, and wrappers may inherit a declaration rather than duplicate it. Every proposed bound is unestablished until its owner supplies the algorithm argument and required real-implementation evidence.

The static inventory is not a replacement for running the existing discovery machinery. It is grounded in the checked source files and public/entrypoint declarations; it does not certify that every dynamically assembled route, inherited method, or contract element has already been bound.

## 1. Use a multidimensional cost model

Use independent dimensions for member count, selected payload bytes, unique required provenance bytes/records/edges, requested output, archive objects, applicable policy/program size, active windows, index-domain size and admitted retries. Freeze their meanings, not test sizes or measured CPU seconds.

An ordinary pipeline should make a bounded number of useful passes over each member and required byte/record, with bounded working windows. Explicit ordered-index comparisons and sorting are accounted separately:

- `linear(X)` means O(X) logical visits or byte processing.
- `indexed(k,Z)` permits O(k log(2+Z)) ordered-index work, with variable key bytes accounted.
- `sort(k)` permits one O(k log(2+k)) comparison sort when required. Already ordered traversal has no sorting allowance.
- Canonical JSON ordering has its actual per-object property/key-byte cost.
- Cryptography charges processed bytes and independently encrypted objects at fixed security parameters.
- Component decoding/encoding/provider work has its own named profile; it is not smuggled into a generic O(N) constant.

**This is not a strict-linear-total-cost claim when a logarithmic term is present.** Publishing strict O(N) total machine work instead requires eliminating or explicitly rejecting every logarithmic exception before freeze. My recommendation is linear streaming/traversal with narrow, reviewable indexed/ordered profiles, rather than inaccurate labels.

The `E` or `Q` dimension is not whatever large intermediate relation the current implementation happens to construct. A dense result counts only when the public semantics actually request it. Repeated expansion of shared history is work amplification, not new independent evidence.

Define whether a bound applies to one request, a fixed snapshot, a full lifecycle, or an amortized operation sequence. A linear method called once per member can produce a quadratic lifecycle. No concurrency improvement changes that total-work fact.

## 2. Normative declaration: what belongs in the Closure

Each owning declaration should contain:

1. A stable ID/version, owning existing authority and exact subject bindings.
2. Operation scope/start/end, including whether the bound is per call, batch, complete lifecycle or amortized sequence.
3. Dimension definitions, units and exact counting semantics.
4. A cost-model ID/version and structured expressions for work, bytes moved/hashed, requests, working memory, temporary/durable state, and replay where applicable.
5. Whether each bound is worst-case, amortized or explicitly expected; expected hashing behavior must not be called a worst-case guarantee.
6. Fixed conditions and dependence on configurable capacities, security parameters, input ordering, index preparation and selected extension.
7. Cold, warm, cache-bypass and failure/retry scopes. No implicit permanent warm-cache assumption.
8. Explicit delegates and composite-cost contributions.
9. Preserved functional invariants and the completion boundary.

Do not put approvals, issue references, test-node IDs, source line numbers, run times, benchmark sample sizes or “currently passing” into the normative declaration. These belong in GitHub and the separately bound Audit Record.

An illustrative structured declaration is provided in `declaration_example.json`. Its metric names and schema are proposals, not implemented vocabulary. The expression language should remain a closed schema (dimensions, integer constants, sum, product, maximum, logarithmic and named ordering/dependency terms). Do not introduce eval, an unrestricted formula language, or an alternate semantic interpreter.

Dimensions with byte units must not silently be added to record counts as if they were one measurement. Each metric defines its own consistent expression and primitive work unit. The human shorthand in the inventory abbreviates a vector, not a dimensionally valid CPU-second formula.

## 3. Keep ownership at existing contract boundaries

Declare growth beside the source that owns the existing operation: public protocol/native lifecycle contract, package public function or component contract. The generator collects it. A common validated declaration schema can be shared without copying the promises into a central hand-maintained registry.

Examples:
- Archive construction and upload lifecycle: Riverhog protocol/server plus generic producer implementation.
- Provenance semantics and validation: provenance contract/library, with endpoint/claimed-reader bindings.
- Extension dispatch: protocol/support wrapper plus the separately selected component.
- A CLI that merely streams a server result inherits the server operation and adds its own local rendering/copying bound.
- A constant/error enumeration/schema-only export receives an explicit non-executable disposition. Its validation or serialization operation may inherit a real bound.
- Arbitrary codec/provider/application behavior is not made linear by a generic adapter declaration.

New packages and unrelated protected identities still require owning review.
The **G26 derived-index identity** change has the specific maintainer approval
above; implementation must be a pre-v1 hard cut updating the owning Alembic
`v1_0001` baseline in place with fail-closed obsolete-state validation and no
old-to-new pre-v1 compatibility, **retaining** Alembic initialization and future
forward-only revision/upgrade test coverage.
Adding growth declarations does not relax any other protected boundary.

**Composition is an acceptance obligation.** A public linear lifecycle cannot be approved while a reachable delegate has an unresolved or incompatible growth bound. Either establish a compatible algorithm, narrow and explicitly approve the lifecycle's operation model, or publish the actual weaker bound. Merely labeling the expensive work a separate component, cache, validation step or 'required snapshot' does not remove it from end-to-end cost.

For each composite claim, record the call multiplicities symbolically. The claim fails if a per-call linear cost is multiplied by an independently growing loop when that product is not necessary public work. Include implicit status polling, authorization, metadata reads, finalization, indexing, cleanup and replay; no stage may be moved outside the accounting boundary just to satisfy the claim.


## 4. Integrate with existing Closure / Audit separation

The source already separates declared `external_contract` values from Audit Record discovery, trace, witness associations and source identities. Preserve that split.

Proposed projection:

```
Closure.external_contract.growth
  format / cost-model definitions
  declarations keyed by stable owning declaration ID
  exact operation/element applicability and delegates

Audit.growth_analysis
  closure/declaration digest
  source trace / discovered subject coverage
  implementation and candidate witness associations
  complexity argument references
  executed evidence references and applicability
  unestablished obligations / omissions / mismatches
```

This is an extension of the existing records, not a second competing Closure. Exact key placement and schema revision are implementation decisions to review with the owning tooling; do not hard-code a new pointer in multiple tools before that decision.

Changing a declared bound, its input dimensions, assumptions, scope, worst-case/expected mode or completion obligation changes contractual meaning and the relevant digest. Changing source location, test implementation, sample sizes, SQL indexes or measured times affects audit/build evidence, not that normative promise.

The human Audit and Documentation renderers must show scope, dimensions, bound, assumptions and limitations in the normal reading path. Do not hide a material exception only behind raw JSON or a distant footnote.

## 5. Derived coverage, not a parallel inventory

Extend existing discovery to require a disposition for every relevant public operation:
- owns a growth declaration;
- inherits an exact declaration with wrapper overhead;
- delegates a defined portion to a named independently selected component;
- non-executable/no growth-sensitive work, with a precise reason.

Internal generator, tests and CI scheduling receive internal budgets instead of archive-product guarantees. A declaration with unresolved subject/delegate, omitted runtime operation or missing applicability fails coverage. A discovered component name alone is not proof of contract coverage.

The 86-distribution map and 203 literal route declarations in this package are *scope aids*. Dynamic registration, inherited public methods and generated/native protocol operations must be reconciled using the maintained discovery at integration time. This work did not run the full dynamic Closure generator and must not be described as having achieved method-level completeness.

## 6. Evidence required to accept a declaration

Require an algorithm argument and source-integrated witnesses:
- count bytes and records actually traversed/decoded/hashed, not merely wrapper calls;
- verify SQL plans or primitive row/edge visits where one query can hide a scan;
- assert complete output and negative cases alongside the cost bound;
- measure full append/page/checkpoint/poll lifecycles, not an isolated method only;
- vary independent axes and sparse/adversarial/dense lawful shapes separately;
- exercise cold/evicted/bypassed caches and restart at actual boundaries;
- run supported database/platform/final-image paths where lower layers affect the claim.

A witness budget `a*N+b` or a corresponding declared expression uses reviewed implementation constants. Do not fit a permissive envelope to a regressed implementation and call that proof. A few favorable wall-time ratios are not asymptotic proof; an inefficient constant is still visible in profiling.

A test-node name or source link is candidate evidence. Only a retained, complete result on the right source and workload establishes executed evidence. Separate `functional`, `growth_conformance`, `evidence_integrity`, and `elapsed_time` results; a valid timing miss cannot turn a growth violation into a pass.

## 7. Cheap qualification instead of indiscriminate giant tests

Choose size ladders that cross every distinct implementation regime, not a mandatory universal maximum count. Small real records can expose scans, nesting, shared history and repeated prefix work. Use supported small cache/page/segment settings to cross several boundaries while preserving production semantics.

Test a generated large-file stream to establish bounded reading and replay, but label simulated bytes versus bytes actually encrypted/stored. Include a modest real final-image witness through several segments and pack closures. Retain a larger database/OS/provider test when real cardinality changes its plan or resource behavior; a source-only mock is not a substitute for that regime.

Do not remove the already required 128-file functional proof merely because this proposal prefers smaller growth witnesses. Cadence/coverage substitution remains an explicit decision. No obligation to encode 100,000 files before every commit is proposed.

## 8. Decisions that cannot be papered over

### Online graph updates and corrections

A static graph has a linear traversal/cycle-validation target. An arbitrary stream of accepted edge insertions/corrections with validity required after each prefix is a different problem. It is not justified to claim linear aggregate work merely because the final static graph can be checked linearly. G22 must name the accepted operation model and actual algorithmic bound. Narrowing a rejection boundary is a semantic change, not a performance trick.

### General predicates and schemas

A fixed structural schema and simple record predicate can have linear traversal. Arbitrary JSON Schema, regular expressions, nested quantifiers, graph joins or user-defined schemas do not automatically have that bound. G02/G28/G40 require concrete profiles and declared exceptions. A generic `predicate_work` variable that means “whatever the implementation did” is not an acceptable contract.

### Shared history and discovery

A compact representation must preserve exact query scopes and output semantics. G26 is a design obligation, not approval of a particular database schema. Internal expanded member-by-history rows cannot justify their own quadratic growth.

### Tools and storage providers

Declare wrapper/orchestration/transfer costs and qualify supported tool profiles separately. Fixed codec settings, resolution, duration and media/container structure matter. Remote calendars, cloud response time and GPU availability are not asymptotic file-count guarantees.

### Materialized APIs

Returning a complete Python list or byte string necessarily materializes that output. Declare O(output) memory for that API, and independently bind the bounded-stream alternative; do not falsely label all public APIs constant-space.

## 9. Proposed v1 decision

Approve the inventory as a scope proposal, then establish the exact cost model and narrow profile exceptions. Adopt the straightforward byte/traversal/control bounds, fix the measured violations, and refuse to mark unresolved graph/query/representation cases conforming before their owner decides and qualifies them.

#961 owns the short developer rule. #963 owns evidence execution/reporting and internal timing applicability. #964 owns these operation-specific growth declarations, implementation corrections and witnesses. #469 consumes the resulting normative declarations and exact executed evidence for v1; #960's timing-shortfall disposition is unchanged.

This remains a proposal for actual owner-level growth declarations. The #964
maintainer explicitly approved G26's derived-index identity revision and the
pre-v1 current-baseline hard cut. This reference distinguishes that forbidden
legacy conversion from the **retained v1 Alembic migration infrastructure**; it
is guidance, not an old-schema compatibility path or runtime change to main.
