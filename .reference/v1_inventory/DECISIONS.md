# Approved growth-contract directions; G22/G26 design briefs

**Latest owner decision (superseding earlier open-policy wording for these subjects):**
[#964 comment 6118314165](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118314165) settles G02/G40 expressive-profile
policy, G63/G64 Closure/Audit/evidence ownership and internal budgets, and
approves the scoped G38/G39 current-only contract change. See
[G02_G40_G63_G64_INTEGRATION.md](G02_G40_G63_G64_INTEGRATION.md) and
[../g38_g39/DESIGN.md](../g38_g39/DESIGN.md). Subsequent statements in this
historical document about pending designs are superseded by their published
designs and issue-owned decisions; proposed **exact formulas, final identities,
runtime evidence and qualifications are not automatically approved**.


**Authority:** [#964 cost/profile decisions 6111028968](https://github.com/nashspence/riverhog/issues/964#issuecomment-6111028968) and [explicit G26 identity/hard-cut approval 6112668478](https://github.com/nashspence/riverhog/issues/964#issuecomment-6112668478).
**Status:** authoritative decisions reside in that comment. This is the non-authoritative #903 integration handoff, not frozen v1 product declarations or evidence that existing code conforms.
**Audits:** historical superlinear probes at `8425e672c60285a549ca4d5aaba15ecbd99e5bf3`; 64-family inventory at `152d095038df0c0d0ed642cf265d47b11c65a754`. The post-#963 implementation baseline remains to be selected and reconciled.

## Approved decisions: can guide ordinary integration now

**D1 — Real growth bounds and necessary indexed work.** Default to linear traversal of payload bytes, independently necessary members, records, history and graph edges at the declared operation boundary. Allow narrowly named and justified index maintenance/lookup, canonical ordering and sorting costs. Account for the *whole* cost, such as `O(N log N)`, without describing it as strict linear machine work; maintain strict linear bounds for operations where they are feasible and declared. Differentiate expected hashing from worst-case, account for variable key lengths, and name worst-case/amortized/conditional behavior explicitly. No general "near linear" waiver and no arbitrary time-based guarantees.

**D2 — Online graph semantics.** The **direction** is approved: preserve required per-acceptance graph/correction validity and negative-case rejection. Treat one static graph versus an online update sequence as distinct operations, with distinct honest total-work bounds. Do not move rejection to a later phase just to pass a performance check. **The specific algorithm and bound remain unapproved pending G22 design submission.** Atomic staged changes would change acceptance semantics and need a separate explicit maintainer decision.

**D3 — Named evaluator profiles.** Preserve maintained schema/query/predicate expressiveness by defining owner-specific, exact supported profiles: e.g. bounded structural schema, simple fixed predicate, indexed lookup, full scan, nested/complex query. Count relevant program/schema sizes, input sizes, candidates actually demanded by public semantics and emitted results; disclose actual cost class and conditions. Do not use "work done by current implementation" as a vacuous cost dimension. A narrower, deliberately tractable expression subset is a serious alternative **only if later separately approved**, not silently authorized by this policy.

**D4 — Compact G26 discovery and approved identity HARD CUT.** Share immutable history/assertions in a derived index, with exact member/scope associations and query-time expansion when required. The maintainer has **explicitly approved** G26's new compact extraction/dataset commitment and derived generation/read identity ([decision 6112668478](https://github.com/nashspence/riverhog/issues/964#issuecomment-6112668478)). Implement a **current-schema-only pre-v1 hard cut** by editing the single
Alembic `v1_0001` bootstrap directly. Preserve the forward-only Alembic framework
and its future-revision/interrupted-upgrade tests. Do not migrate from old pre-v1
layouts, retain old cursor adapters or dual readers/writers, or roll a transition. The G26 design submission is supplied in [../g26/DESIGN.md](../g26/DESIGN.md). Exact query scopes, archive identities and non-rebuildable state remain protected; its production realization and bounds still need integration proof. The factored design is preferred to on-demand adjacency for the stated profiles; this does not approve unrelated schema/public-authority changes.

**D5 — Qualified component and provider profiles.** Generic Riverhog/Stove0 transport, custody, planning, status and preservation work gets its own bounds. Chosen codec, observer, tool, GPU and provider implementation work gets separately scoped profiles; a composite must account for every reachable delegate, including invocation multiplicity. A narrower set of *qualified* supported profiles is valid when applicability is declared, but missing or incompatible delegate bounds prohibit a composite conformance claim. Count actual calls and bytes; external network/calendar wait is not a file-count work promise. APIs returning entire arrays/bytes incur output-sized memory, not stream-sized memory.

**D6 — Evidence and cadence.** The maintainer approved smaller adversarial real-implementation work-count tests, complete-outcome/negative proof and modest real platform/boundary witnesses, with larger tests only for distinct query-plan, cache, disk, provider and resource regimes. Existing required tests are not replaced without reviewed equivalent coverage. Performance-time misses remain report-only; declared growth failures/missing evidence block conformance. `#960`'s 600-second maintained profile and its explicitly approved shortfall disposition are unchanged.

These approve **policy and direction**, not all 62 proposed public family's exact expressions, public operation applicability, source-code conformance, or protected-boundary changes. Each owner must still justify the actual selected profile, bound, resource assumptions and witness. A composite lifecycle may not pass if the graph or index owner remains unresolved.

## G22 design: online journal/graph acceptance and corrections

**To supply next, before broad online-graph redesign:** a decision-ready comparison of a semantics-preserving incremental algorithm versus an explicitly staged/atomic transaction alternative. Recommend the semantics-preserving algorithm unless evidence demonstrates a reason to request a changed acceptance contract.

Provide:
- Exact externally observable acceptance/rejection points, snapshot and prefix identity, correction/retraction and late-invalidations, graph edge/type rules and current error semantics.
- Formal operation dimensions and accounting for *each accepted prefix* **and the entire stream** of updates, including cold working sets, memory/disk/index work, durability, cancellation and replay. A static `O(V+E)` pass cannot substitute for a bound across repeated online calls.
- Algorithm/data-structure choices and why they remain correct for forward/reverse edges, deep chains, stars, disjoint graphs, cycles, dense lawful graphs, cross-journal imports and corrections.
- Executable adversarial real-implementation work counts plus complete positive/negative examples, DB query-plan and restart/fencing tests.
- Any owner-specific public contract or stored-authority consequences; identify semantic changes separately for **new explicit approval**.

**Pending:** algorithm, aggregate bound and any new transaction/acceptance model. No numeric budget or universal online linearity was approved.

## G26 design: exact shared-history discovery without Cartesian amplification

**To supply next, before broad derived-index redesign:** compare a factored index of shared exact immutable assertions/roots plus member/scope links against compact graph adjacency with authorized on-demand relevance. Prefer factorization provisionally but select based on exact query semantics and measured work/state profiles.

Provide:
- The exact logical relevance relation across `member`, `input-history`, `collection`, `recorded-history` scopes; clarify **recorded does not imply caused/attributed**. Full query/predicate/count semantics, completeness, ordering, stable pagination and ambiguity.
- Sparse, fully shared, partially shared and genuinely dense lawful cases; distinguish unique stored evidence/relations from requested output rows and avoidable member-by-shared-assertion staging.
- Factorization/index schema and query expansion; compare with adjacency/on-demand alternative on build CPU/work, database rows/bytes, status-read costs, one-result queries, full exports, cache-eviction and reconstruction.
- Byte-/row-exact parity across old/new builds and retries, generation/dataset commitment identity, stale-worker fencing, live permissions, archive-only recovery and rebuild. Explain any digest change and protected/frozen boundary effects rather than regenerating an expectation to conceal them.
- **Approved G26 hard cut only:** replace old derived index schema/extraction with
the new v1 baseline representation. **Keep Alembic and its intentional forward-only
upgrade APIs/tests**; edit the application's single `v1_0001` revision and DDL
rather than appending a schema migration from discarded pre-v1 state. No rolling/
dual format paths or compatibility readers. Fail closed on incompatible old state;
preserve archived authority and non-rebuildable operational evidence, and rebuild
only derived indexes from authenticated source.
- Source-executed adversarial work budgets and a compact real PostgreSQL/integration witness. A 128-file proof need not encode 100,000 media files to expose quadratic staging.

**Approved identity direction:** compact extraction/dataset/read-generation revision and pre-v1 **no-old-schema-migration** hard cut with Alembic retained (6112668478, corrected by the subsequent maintainer clarification). **Pending:** final implemented schema, derived-index query parity, work bounds, state-safety and integrated qualification, not a separate approval of this specific identity change.

## Parallel work authorized without these designs

The integration agent may now reconcile the real post-#963 SHA, extend existing Closure/Audit projection and public-operation coverage, author straightforward owner-scoped declarations, integrate the shared evidence machinery, and fix confirmed narrow per-item/index/lookup amplification where semantics are preserved. Require faithful operation/lifecycle composition, source-integrated work counts and negative/error/restart parity.

Neither G22 nor G26 may be labeled conforming while its requisite definition/delegates remain unresolved, and no acceptance-boundary or broad data-representation change follows automatically from this reference.

**Authority/order:** #961 onboarding prerequisite → #963 shared validation/evidence foundation → #964 declaration and conformance integration (coordinated implementation allowed) → #469 reviewed v1 closure evidence. #903 reference history is non-authoritative. The maintained report-only #960 target, functional proof and explicit timing-shortfall disposition remain intact. No changes to `main`, release, provider authorization or required qualification are supplied.
