# G26 exact source relation and scope algorithms

Read [DESIGN.md](DESIGN.md) for current execution, staging, commitment, lifecycle
and ownership. This file retains the source semantics, not the superseded query
workspace or flat-hash restart policy. Relation names/keys describe meaning;
DESIGN.md supplies segmented physical carriers and qualified source identities.
G27/G28 owns query admission, retained views, progress and response transport.

## 2. Exact relation to preserve

Let `R(member, snapshot, assertion, scope)` be the SET relation returned by current
`member_relevance`. A snapshot means the complete exact anchor, including journal,
through-entry/sequence/hash, prefix hash and byte count. It is never a latest head.
An assertion has its exact original entry and identity, with effective/retracted
state evaluated at that snapshot. Multiple paths to the same tuple add no copies.

The compiler/evaluator must satisfy the following rules on every admitted source:

| Current operation | Exact meaning retained |
| --- | --- |
| Member primary | Verify slot, payload identity, delivery context and association against the selected primary. Seed its State and association as `member`. |
| Collection | Mark the effective delivery context plus documentary extensions targeting that context in the primary snapshot as `collection`; do not propagate to all co-resident metadata. |
| State/occurrence/artifact | Mark documentary own-id/subject/target assertions and relevant journal-subject assertions, plus observations/descriptions of the selected State. Context object lookups use that snapshot's effective view. Preserve current treatment of retracted documentary assertions. |
| State lifecycle | Traverse effective generation, invalidation and derivation-generated endpoints, related activity/context and usage/generation references. Follow a derivation's used State into `input-history` at the exact local or foreign anchor. |
| Same-artifact continuity | Other effective States for the SAME continuity Artifact in the primary snapshot start as `recorded-history`, not `member`. Their actual causal input traversal retains existing `input-history` behavior. |
| Own late bound claims | Own history's bound roots can contribute EFFECTIVE extensions targeting the exact primary State, occurrence, Artifact or generating activity. These are `member`. Retained-only roots are not bound roots. |
| Imported late claims | Import reachability alone is INSUFFICIENT. The imported history's exact primary State must also lie in this member's causal State set. Both conditions must hold for the SAME member; then bound-root endpoint extensions are `input-history`. |
| Documentary history | Every documentary assertion, effective or retracted, in every exact snapshot selected by retained history/import extents, foreign documentary references or fork dependencies is `recorded-history`. |
| Opaque metadata | Lexical posting extraction is unchanged. Opaque profile values are not traversed as graph edges; only the existing declared canonical reference grammar creates dependencies. |

Use the existing scalar extraction and exact `assertion_clause_matches` as the
semantic oracle; production uses the bounded equivalents specified by G27/G28.
All tests in one clause must still match ONE assertion and, when pinned, ONE profile section.
Default scopes still exclude `recorded-history`. An imported record is not causal
attribution merely because the source binding proof is valid.

## 3. Source-normal storage contract

All production rows are namespaced by `build_id`; source coordinates additionally
carry the exact archived journal identity. Application owner remains Riverhog.
The following is the new **current-only** logical schema; map names through
the owner's current schema definitions, not migration conventions. Strings/integers
use the existing protocol domains and key collations.
No new semantic count/depth bound is introduced by a physical SQL column.

| Relation | Primary key after build_id; essential columns and access indexes |
| --- | --- |
| journal_source | `journal_id`; admitted complete byte identity, length and terminal coordinates, source-capture identity, verified/complete marker. No latest external provider lookup. |
| entry_fact | `(journal_id, sequence)`; exact entry ID/hash, predecessor/checkpoint identity, prefix SHA/bytes, entry kind. Unique exact entry-id lookup. |
| assertion_fact | `(journal_id, assertion_id)`; origin entry/sequence, referent ID/type, exact canonical byte-stream reference/hash, `born_sequence`, nullable `retired_sequence`. Index `(journal_id,referent_id,born_sequence)` and `(journal_id,born_sequence,assertion_id)`. |
| scalar/profile/chunk/edge facts | Re-key existing postings by `(journal_id,assertion_id,ordinal/path-node identity)` instead of duplicated snapshot row_key. Lexical/profile meaning remains with the current extractor; DESIGN.md replaces whole-assertion posting tuples and expanded pointer carriers with bounded source-normal streams. Inverse indexes for declared local owners/endpoints and complete foreign-reference coordinates. |
| snapshot | `(journal_id,prefix_sha256)`; full exact anchor, required extent, source-complete link. Header only; index `(journal_id,through_sequence,prefix_sha256)`. |
| history | `history_sha256`; complete verified descriptor, primary exact anchor/association, expected root/import set commitments. |
| history_root | `(root_set_sha256, record_key)`; exact snapshot anchor, bound/retained inclusion and original page/ordinal. `history` references this set authority. Store each authored root-set record once; index snapshot/inclusion back to set. |
| history_import | `(import_set_sha256, record_key)`; target H, extent, exact input State and source-proof identity, original page/ordinal. `history` references the set authority. Inverse indexes by target H/input State. Preserve source-root membership and import-cycle rejection. |
| documentary_dependency | `(journal_id,entry_sequence,reference identity)`; exact target snapshot, including documentary retracted assertions and fork anchors. A prefix selects entries up to its through-sequence; do not copy all prior dependencies into each snapshot. |
| member | Existing member identity plus exact history binding, selected primary and delivery association. Root binding is verified from the archive member-binding tree, not an inferred filename. |
| source_work | `(build_id,work_kind,exact_source_key,ordinal)`; pending/complete, verified terminal/expected count, checkpoint and fenced epoch. Index PENDING work, not scans through completed rows. Operational only, excluded from dataset digest. |

**Prefix intervals:** an assertion is documentary at prefix `p` when `born <= p`;
it is effective iff also `retired IS NULL OR p < retired`. Corrections do not erase
the raw row and an assertion identity is not reused. Keep correction/frame records
for validation and exact snapshot provenance. A current complete transcript can
supply these interval facts only after the neutral journal validator has validated
all relevant prefixes. G22 is the validation owner; this is not a second validator.

**Do not instantiate** `member x shared snapshot`, `snapshot x assertion`, or
`State x every possible prefix` rows as an optimization. State, activity, owner and
bound-endpoint selectors are parameterized indexed joins over source facts. An
exact encountered prefix is a parameter, not permission to clone the entire view.
The model adapter expands small views solely for differential testing.

### Fixed selector identities and incidence

A selector is identified by its typed exact coordinates, encoded using existing
canonical JSON, with a domain-separated digest. Compare original coordinates on
an existing key; never trust a hash collision as equality. Separate graph kinds,
scopes, history extents, snapshot anchors and source identities.

Implement only these private selectors (or equivalent named SQL accessors):
`member`, `history-documentary(H,extent)`, `snapshot-documentary(anchor)`,
`prefix-dependencies(journal,through)`, `State(anchor,id,scope)`,
`own(anchor,id,scope)`, `observed(anchor,state,scope)`,
`activity(anchor,id,scope)`, `collection(anchor,context)`,
`continuity(anchor,artifact)`, `import-reachable(H)`,
`causal-State(exact external reference)`, and `late-bound(H,scope)`.

Unary incidences propagate reachability. Imported late-bound applicability uses
ONE fixed binary condition:

`import-reachable(member,H) AND causal-State(member,primary-State(H))`.

It is not valid to OR the inputs, combine reachability of two different members,
or keep only a union of every member's causal State set. Prefer a direct indexed
correlated semijoin for this rule; a named private AND gate in the executable model
makes the same requirement testable. No general configurable rule engine is needed.

For dependency prefixes use per-entry edges plus an indexed predecessor chain or
range accessor. A traversal marks exact `(kind,coordinates)` once. Do not carry
path/depth in the deduplication identity: a shared diamond must not enumerate paths.


## 6. Query implementation without reconstructing R

Select content from G27/G28's exact admission-pinned directory and immutable
generations. Preserve native tag/description/predicate meanings over those retained
versions, not current mutable rows. Current catalog/provenance and artifact-scope
authority is checked before every page/support chunk. The index never grants
permission. A current generation replacement does not invalidate that retained
view; public read/expiry/progress semantics belong only to G27/G28.

### 6a. Exact assertion predicates and interval selection

Evaluate native posting predicates using qualified bounded candidate access and
G27/G28's exact segmented matcher once per distinct source assertion. The native
`assertion_clause_matches` remains the parity oracle, not permission to deserialize
an unbounded total or hold a database resource throughout evaluation.
Never combine different assertions/profile subsections to satisfy one clause.
Resolve effective/retracted state for an exact prefix with born/retired intervals.

For several requested prefixes of one journal, sweep sorted matching assertion
birth/retraction events and sorted selected prefix endpoints. Retain an ordered
index/heap of currently eligible support. Emit the best support per needed
snapshot/selector, not all matching assertion-by-prefix pairs. For recorded-history
with assertion_state=any, all born records remain eligible. Retraction queries
activate at retirement; effective queries deactivate there. Same-page clauses are
separate exact predicates. This is source/range work plus ordered maintenance, not
quadratic prefix copying.

The current first-support priority is `(journal_id, sequence, assertion_id, scope)`.
Retain it; propose `prefix_sha256` as a FINAL deterministic tie-breaker only where
the current ordering leaves multiple otherwise equal snapshot witnesses. Identify
this explicitly in the new extraction policy, verify exact API expectations, and
obtain owner approval if any supported contract pins another tie behavior. Do not
change which member or clause matches to make support selection cheaper.

### 6b. Common unary/shared-root profile

For exact unguarded scope paths, seed matching snapshot/row selectors with their
best support keys. Propagate in reverse, using a disk-backed priority worklist.
Pop the smallest witness key; settle a selector once; propagate the same key to
its parents. The first settled member root is its globally smallest support on
those paths. Zero-cost cycles/diamonds terminate by exact selector deduplication,
not path enumeration. Batch indexed adjacency reads and writes; don't issue one
network round trip for every fact.

Why this is correct: every unary edge preserves a valid support witness. Processing
keys in order ensures no larger witness can settle a parent before an available
smaller one. Later witnesses for a settled selector cannot improve any ancestor.
For N members sharing A matching facts under one snapshot, query work is O(N+A)
visits plus sorting/index costs, not N independent traversals of A facts. The
included model tests this and forbid calling its flat expansion helper.

### 6c. Correlated imported-late-claim profile

An AND guard cannot be traversed as OR and accepted. Reverse OR relaxation supplies
only a sound candidate superset. For exact guard evaluation intersect **same-member**
import reachability and causal-State reachability. Reuse exact per-query operand
projections keyed by history/state, not one projection per raw matching assertion.

Implement exact checking on the query-relevant backward slice using a disk-backed
per-candidate least-fixed-point frontier: insert each `(kind,coordinates)` once,
process each incident unary edge and each AND input once; activate a guard only
when both inputs have been reached for that member. Apply source interval joins
lazily. For the exact candidate, choose support using the same ordering and exact
predicate checks. Combine with unguarded best support, selecting the smaller
verified witness. Do not accept the unguarded witness immediately when an earlier
possible guarded witness remains unverified.

This is a **separately declared G28 correlated-reachability profile**. A safe bound
is the sum of relevant slice visits over candidate members, bounded by C times the
selected source graph/selector domain, plus index/predicate/result costs. It is
not a universal O(output) or file-linear query guarantee. Profile C from public
selectors and necessary matching-history/state candidates, not an arbitrary
unrestricted Cartesian prefilter. Zero-result, ambiguous and broad-match cases
must be measured. Do not mark a composite strictly linear if it needs this general
profile; stronger specialized query algorithms can be added with actual evidence.
