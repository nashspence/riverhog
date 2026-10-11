# G26: source-factored exact-scope discovery

Recommended implementation design for #964 under #903. Audited source:
`main@152d095038df0c0d0ed642cf265d47b11c65a754`, tree
`991e994457b487a7dca016b7fc0409485cef25c6`.

This is a precise design and executable semantic/cost model, NOT a fully wired
production patch or completed growth qualification. It preserves the supplied G22
reference unchanged. The specific derived discovery-index extraction/dataset and
read-generation identity revision is **explicitly approved by the maintainer**:
[#964 comment 6112668478](https://github.com/nashspence/riverhog/issues/964#issuecomment-6112668478).
The approval is a pre-v1 hard cut with no migration or backward compatibility.
It does not certify the implementation or waive owner verification of exact
current-state and protected-identity evidence.

## 1. Selected architecture

Replace expanded per-member assertion memberships with **source-normal facts and
fixed, typed scope-selection rules**. Store an assertion and its lexical postings
once per admitted journal chain, with birth/retraction coordinates. Represent an
exact snapshot by its authenticated anchor, not by a copy of all preceding rows.
Represent shared history/import/dependency selections once, preserving the precise
member root and all four scope rules. Resolve only the memberships a query needs.

The design has three inseparable parts:

1. One source-bound build, rather than a complete closure rebuilt per member.
2. A compact deterministic dataset commitment, rather than hashing the expanded
   Cartesian relation after compressing it on disk.
3. Query execution that retains sharing for common scope/match paths and accounts
   honestly for correlated/general queries instead of expanding every pair first.

Do not implement a general expression VM, inferred relationship taxonomy, opaque
query language, alternate provenance interpreter, or new archive format. The only
rules are the current `member_relevance` semantics enumerated below, implemented
at their existing owners. Private selectors are indexes/access paths for those
rules, not independently authored user programs or authority.

### Approved derived-index identity hard cut

**Implement the expressly approved pre-v1 derived-index identity revision.** Keep archive
roots, journal bytes/anchors, member/history identities, source proofs, request
schemas and the logical discovery result relation unchanged. Change the extraction
policy/dataset commitment to identify the new deterministic representation.
Consequently a newly built `index_generation` and the associated `read_identity`
will differ from the old expanded generation. Existing cursors must conflict, not
continue silently. Rebuilds under the SAME new policy/source remain identical.

This is not a claim of byte-identical old generation IDs. The old SHA-256 digest
feeds every expanded membership row; retaining that exact computation retains
work proportional to the expanded relation. Do not replace it with an unreviewed
commutative checksum or quietly hash a different preimage under the old identity.
The specific derived index/read-generation identity change is now approved in
#964; update the owner-reviewed exact freeze expectations from the new source and
record their evidence. No approval was given for altering archive authority or
query semantics. **No old-to-new schema migration or identity-translation path is
required or permitted**; this is a clean current-schema replacement.

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

Use the existing scalar extraction and exact `assertion_clause_matches`. All tests
in one clause must still match ONE assertion and, when pinned, ONE profile section.
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
| assertion_fact | `(journal_id, assertion_id)`; origin entry/sequence, referent ID/type, exact canonical bytes/hash, `born_sequence`, nullable `retired_sequence`. Index `(journal_id,referent_id,born_sequence)` and `(journal_id,born_sequence,assertion_id)`. |
| scalar/profile/chunk/edge facts | Re-key existing postings by `(journal_id,assertion_id,ordinal/pointer)` instead of duplicated snapshot row_key. Existing bounded lexical/profile extraction remains the owner. Inverse indexes for declared local owners/endpoints and complete foreign-reference coordinates. |
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

## 4. Source build and staging sequence

Replace the per-member complete-relevance loop with one resumable generation build:

1. Take existing collection/root/epoch/pending fences and record exact archive,
   provenance, member-binding and extraction identities. Authenticate the selected
   immutable source before any stage becomes admissible. Never use provider listing
   or an old search index to discover authority.
2. Scan the member binding tree in bounded ordered pages once. Stage exact member
   bindings and enqueue their H descriptors. Verify tree totals and terminals.
3. Resolve the union of needed descriptor/root/import/source-proof structures with
   a disk-backed deduplicated worklist. Every imported source binding and exact
   input State is validated by the existing neutral owner. Share work by exact
   source identity, NOT by permission or merely a journal name. Detect cyclic
   histories independently; fixedpoint termination is not semantic cycle acceptance.
4. Read and capture each reached admitted journal stream once for this build's
   source-view, verifying current access/custody/root fences and all bytes through
   the provider's final fence. Build G22-validated entry/assertion/interval/reference
   facts and lexical postings. A short or revoked provider aborts the stage; no
   partially read journal can mark its task complete.
5. Link required exact prefixes using already verified entry/prefix coordinates.
   Foreign reference equality includes entry, assertion, referent type and object,
   not just journal/prefix hash. No repeated full-prefix parse/hash per member.
6. Publish source stages in bounded transactions. A task's terminal/count/digest
   attestation and its last rows commit atomically under the current fence. Retry
   compares exact existing values; INSERT OR IGNORE alone is not sufficient.
7. Check all source terminals, references, member totals and coverage against the
   independently authenticated archive manifests, not just the number of rows the
   builder itself happened to insert. Unfinished/failed work prevents completion.
8. Commit the compact canonical dataset in a quiescent sealed pending build,
   produce the generation identity and atomically switch active_build_id under
   `_require_pending`'s current root/deletion/epoch conditions. Preserve an old
   applicable active generation on ordinary rebuild failure.

### Capture lifetime, caches and re-authentication

Use a build-scoped verified source projection, not a process-wide authorization
cache. Successful immutable structural/semantic work may be shared inside one
exact admitted build while current ownership/root/fence checks continue at each
transaction and public request. Public provenance API fresh-read obligations remain
unchanged. This design does not remove `JournalReferenceCache`'s public current-read
checks or accept a cached history under a new claim.

Perform archive I/O and heavy extraction outside short catalog write transactions;
then reacquire the fence to publish results. Keep source capture under existing
scratch ownership/leases and bounded prefetch capacities. No eager copy of unrelated
whole corpora per member. Durable completed derived facts can resume a fenced build;
missing disposable capture is reconstructed for the uncommitted source unit rather
than treated as authority or silently truncating work. Changed roots supersede the
build, not reuse its validation. Record discarded/repeated work on interruption.

### Private implementation interfaces

Add `canonical_discovery_source.py` for the Riverhog-owned source adapter and
`canonical_discovery_factors.py` for normalized staging/access. Both upload and
archive-only rebuild consume these same operations, with no public route changes:

- `begin_source_build(source_identity, expected_member_manifest) -> build_id`:
  use existing pending generation/fence owner; do not create a competing lifecycle.
- `advance_source_build(build_id, row_budget, byte_budget) -> progress`:
  process one bounded source-work batch, outside-transaction reads followed by a
  fenced stage. Return only stable phase/key/count summaries, not full source rows.
- `capture_journal(exact_source, required_anchors) -> verified_fact_stream`:
  neutral G22 validation, one raw assertion ledger, exact reference/anchor directory,
  terminal provider fence. A reference is resolved against that checked directory.
- `stage_source_page(build_id, source_key, ordinal, exact_records, terminal_proof)`:
  compare every duplicate's exact data; atomically advance page and source progress.
- `complete_source_build(build_id) -> generation_id`: source completeness checks,
  compact commitment and existing publish fence. No expanded R count or hash.
- `matching_assertion_facts(build_id, clause) -> bounded ordered iterator`:
  current SQL predicate plus exact native clause check, interval/profile-aware.
- `resolve_clause_members(build_id, clause, read_identity) -> query_projection`:
  unary minimum-support propagation, then exact guarded correlation as needed.
- `read_projection_page(query_projection, cursor, principal) -> original page`:
  current scope/read-identity checks, keyset paging, no held client-lifetime snapshot.

Normalize each local/external attachment endpoint once from its immutable origin
entry/assertion coordinates. An effective-at-snapshot query is an interval filter,
not a rewritten external-reference identity. A late-bound lookup joins history's
bound root set to that exact endpoint index; it must not scan the shared snapshot's
entire extension set for every member. Raw `own`, observation, State/lifecycle and
activity accessors similarly use indexed owner/endpoint plus prefix interval tests.

These are private integration names/roles, not additional frozen public exports.
The integration agent should implement them within the existing owner, replacing
retired runtime paths in one explicit pre-v1 hard cut; the derived-index identity
change is already approved and must not be implemented as a schema upgrade.

## 5. Compact commitment and identity transition

Replace expanded `_dataset_sha256` input with a **fixed source-normal table stream**.
Use domain `riverhog-canonical-discovery-factor-dataset/v1\n`, canonical records with
length framing, deterministic table order and existing canonical scalar encoding.
Exclude build UUID, epoch, queue state, timestamps, query caches and random SQL IDs.
Include exact source/member/history/snapshot coordinates, raw assertion identities
and extraction products, and the versioned fixed selection policy. Necessary
ordered-index work is accounted separately from linear bytes hashed.

Freeze the extraction normal form: identical admitted source and policy produce
identical records regardless of concurrency, input-page order, cache state, retries
or cold rebuild. Do not attempt to make arbitrary semantically equivalent graphs
share one digest by Boolean minimization. Stored facts are the deterministic output
of the named extractor, not whatever compressed graph an optimizer happens to pick.

Change existing `_EXTRACTION_POLICY` to a new explicitly versioned policy (recommended
`riverhog-canonical-discovery-extraction/v2`) identifying the source-normal storage,
interval state, exact scope rules and support tie order. Its one shared digest
continues to qualify generation/read applicability. Do not create a second registry.
The outer API response format can remain unchanged while its opaque generation
values change. Update owning declared derivation/freeze surfaces where required.

Do not keep an expanded logical membership count as a seal prerequisite. Replace
`expected_memberships` and the upload cursor's membership total with verified
physical/source-stage counts and complete manifests. A diagnostic explicit flat
export may enumerate/count R on request; it is not part of ordinary index building.

A final compact digest pass visits each canonical record once. Store checkpoints
at deterministic table/key boundaries with a verifiable hasher state or reuse the
existing neutral incremental commitment abstraction; neither stage nor status may
restart the whole digest per record. If a crashed private hash state is unavailable,
restart that compact pass once and report its cost; the normal-build growth bound
is not a claim of finite work under unlimited crashes.

### Pre-v1 clean hard cut — approved identity, no schema migration

**Maintainer decision:** [#964 comment 6112668478](https://github.com/nashspence/riverhog/issues/964#issuecomment-6112668478).
The new normalized index tables and versioned extraction/dataset commitment
replace the retired expanded-membership schema and runtime entirely **before the
first release candidate**. There is no support for migration of an old DB schema,
rolling upgrades, old/new index coexistence, dual reading/writing, legacy
extraction fallback, compatibility adapters, or old-cursor translation.

The index/dataset and read-generation identity change is specifically approved.
Continue to freeze the new deterministic values through the owning current-source
contract/tests; the approval is **not** permission to rebaseline archive roots,
journal octets, member/history IDs, input authority or semantic query results.

**Exact integration sequence:**

1. Replace existing current schema definitions/DDL and fixtures with the new
   source-normal tables, indexed selectors and bounded private staging. Remove
   retired membership tables/extractor runtime paths and their expectations in
   the same pre-v1 hard cut; do **not** create migration scripts, schema-version
   bridges or an old-layout load/convert operation. Keep G22 implementation
   ownership separate.
2. Current-schema startup validates its declared structure. When pointed at an
   incompatible old database it **fails closed** with an actionable fresh-state
   initialization requirement. It never upgrades, dual-reads, rewrites or silently
   resets it. Requiring explicit operator intervention is not a promise of a
   supported old-to-new upgrade procedure.
3. Start with newly initialized **current-schema** state. Regenerate only
   *rebuildable derived discovery-index state* by running the new extractor over
   independently validated archived member/history/provenance data, under normal
   build fencing, source capture, completeness and retry rules. Publish only a
   fully verified new index generation.
4. Preserve authoritative archives, documentary evidence, custody receipts and
   non-rebuildable operational state. Do not call an entire application database
   disposable because discovery indexes are rebuildable. If pre-v1 saved
   operational state cannot be reconstructed safely under the new schema, stop
   and obtain explicit owner/operator disposition; this design supplies **no**
   migration or compatibility mechanism for that state.
5. Use the existing generation/read-identity conflict semantics when an old
   cursor or saved read identity is presented. The old value is invalid and the
   caller restarts discovery with the new generation. The server does not
   translate, recognize as compatible, or silently mix old and new cursors.
6. Confirm current/new-only archive rebuilding, restart/replay, claim/fence,
   permissions, scope/query equivalence, cleanup, SQL plan/growth constraints,
   and protected derivative-identity freeze tests on the final source. No
   zero-downtime or cross-version deployment promise is made.

Any mention in prior reference history of **forward-only migration, rolling
transition, retirement of a concurrently running old index, or optional hard-cut
alternative is superseded**. There is only the clean pre-v1 hard cut for this
G26 integration. A later v1-to-v2 policy is out of scope and not established.

Old discovery cursors still invalidate when generation/read identity changes;
the existing conflict/restart result is the *current interface behavior*, not a
compatibility adapter. No reference to an old schema is needed at runtime beyond
rejecting incompatible current-state validation.

## 6. Query implementation without reconstructing R

Keep `_selected_collections`, current catalog/provenance permission checks,
artifact-scope filters, tag/description filters and source/root/read identity rules.
The index narrows candidates; it does not grant permissions. Each page checks the
current authorization and read generation again. Filter before emitting support.

### 6a. Exact assertion predicates and interval selection

Evaluate the existing SQL posting predicate as a candidate filter and then the
existing exact `assertion_clause_matches` against each distinct raw assertion.
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

### 6d. Pagination, counts and query workspace

Join/intersect the per-clause MEMBER result projections; do not join each clause's
full support-row relation before DISTINCT. Select the first exact support per
clause only for the bounded returned member set. Keep member order
`(collection_id, artifact_id)` and existing current-generation cursor fences.

A reusable query projection is bounded disk-backed derived state keyed by exact
query/read identity (including authorization-view identity), with lease, capacity,
expiry and cancellation cleanup. It must not store reusable permission decisions.
Resume complete member results by key; do not recompute all prior pages. A missing
or expired projection may rebuild with separately reported cold cost or use the
existing explicit invalidation contract; never return partial work as complete.
Do not hold a database snapshot/connection throughout a slow client's lifetime.

Exact counts count distinct matching MEMBERS or the specifically requested output,
not internal factor paths. Explain aggregate query cost separately. An explicitly
requested full flat provenance relation can require O(|R|) output; ordinary build,
seal, first-support and member enumeration must not pay that expansion in advance.

## 7. Honest candidate growth declarations

Let H be unique admitted source bytes, A raw authored assertions, D authored
root/import/dependency/graph incidences and their bytes, N member bindings, S exact
snapshot headers. U = H+A+D+N+S abbreviates a vector of measured work, not adding
bytes to SQL time. Source input costs, G22/G23 semantic validation, G01 parsing and
canonicalization, lexical/profile extraction, encryption and indexed operations
are separate named delegates. Independently growing pointer depth or parser work
must be charged to its actual extraction profile; it is not a new unlimited
"necessary metadata" allowance that can hide internally manufactured products.

- **G26 build/storage/compact seal:** O(U) source/fact visits and compact encoded
  bytes/state, plus declared O(K log K) indexed/order costs. No term N*shared-A,
  S*journal-A, or all-member transitive membership materialization is permitted.
  Holding a complete raw summary per prefix would violate this implementation goal.
- **Common OR-only scope query:** O(M+F+L+N_q) useful visits over matching source
  records, reached factor nodes and links, plus exact predicate, sorting/index and
  result costs. F/L refer to the named unary selection slice, not expanded pairs.
- **Guarded query:** explicit correlated-slice bound from 6c, not an unconditional
  linear end-to-end claim. Record member/query dimensions and actual failed probes.
- **Full explicit output:** add true requested result bytes/tuples, with deduplication
  and ordering cost. Do not call unused intermediate rows requested results.
- **Memory:** configured source/page/cache windows and active-query limits. Spill
  exact frontiers/facts to owned disk. Model Python sets/heaps are not production
  bounded-memory evidence. Quota failure is explicit, never truncation.
- **Retry:** committed source units reused under the exact build fence; partially
  staged work replays only its declared unit. Changed source or deliberately lost
  derived state requires explicit rebuild accounting. Unlimited failures are not
  a finite-runtime promise.

No 600-second guarantee is inferred. This design removes the source-independent
Cartesian construction/sealing cost, but G22 corrections, other owners, cold crypto
and guarded/general queries retain separately stated costs. G26 does not silently
waive the approved whole-lifecycle composition requirement.

## 8. File-level integration job

| Existing owner/path | Change |
| --- | --- |
| `catalog_provenance_index_models.py` | Add normalized source facts/root/import/dependency rows and staged work/terminal records; re-key lexical postings; remove the expanded-membership dependency in the same pre-v1 hard cut. Preserve build-qualified FKs/inverse indexes. |
| `canonical_discovery_rows.py`, `canonical_discovery_extraction.py` | Reuse exact scalar/profile/core-edge extractors; extract raw facts once, attach born/retired coordinates, no latest-head replacement. |
| `canonical_discovery_relevance.py` | Replace production Mapping that enumerates all member assertion memberships with the fixed source-normal accessors and correlated-scope evaluator. Keep the old implementation only as a TEST oracle during parity work, then remove retired runtime path. |
| `canonical_discovery_build.py` | One shared source build and bounded stage advancement; no per-member complete closure/materialization loop. |
| `canonical_discovery_index.py` | Preserve `_require_pending`/publication fences; exact bounded staging and retries; complete source manifests; new compact commitment and extraction policy. |
| `canonical_discovery_rebuild.py` | Archive-only source adapter feeding the SAME build engine; no reconstruction from old search rows. Resume bounded source work, not just max member key. |
| `services/collection_uploads.py` | Replace `_advance_catalog_canonical_index` member/membership counters with build phase/task cursor; same final index readiness before terminal publication. |
| `canonical_discovery_search.py` | Replace `_matching_members`/`_exact_support` flat joins with exact fact predicates, unary propagation and guarded checks; keep filters, read fence and public response semantics. |
| neutral provenance/archive libraries | Add a narrow immutable source-fact projection API only where needed, backed by their exact validator. Do NOT import server selectors or interpretation into them; do not bypass current public read checks. |
| Riverhog current-state schema/fixtures | Replace current owner DDL/schema/model and exact fixtures as a pre-v1 hard cut; no migrations, upgrade paths, legacy readers/writers or startup conversion. Check current-schema initialization, fail-closed old layout, archive-backed rebuild and non-rebuildable operational state. |
| qualification native discovery plan fixtures | Seed the new truthful source-normal projection; assert relevant indexed paths, including inverse incidence and guarded negatives. Do not merely update old index-name strings to get a green test. |
| #963/#964 Closure/Audit witness wiring | Declare actual cost profiles, exact source/workload/applicability, counters and completed evidence. No second target or scope registry. |

The parked `codex/960-discovery` experiment can contribute snapshot-link staging,
source capture and negative/fencing tests. Its advertised hash/query expansion is
NOT the final build/seal strategy. Reconcile its actual patch before reusing code;
this producer read the issue description, not the private local patch file.

## 9. Executable payload and validation plan

`factored_index.py` is a small SQLite model of exact staging, interval leaves,
unary edges, same-member AND guards, deterministic compact sealing, least-fixed-
point traversal, reverse candidates and minimum unguarded support propagation.
`native_compiler.py` lowers actual validated Riverhog fixtures to that model for
scope equivalence. It deliberately uses in-memory/per-prefix views and is **not**
a ready-made production source-normal extractor. Do not inherit that scalability
limitation. Neither file is a new public API nor the final SQLAlchemy current-schema integration.

Tests cover exact native primary/context/late-bound/retained scopes, corrections,
multiple exact prefixes, real imported source-binding proofs with/without causal
links, deterministic retry order, disconnected/diamond/cyclic factor worklists,
false-positive OR candidates, exact missing/changed inputs, and no flat expansion
in common search. Private selector cycles in model tests are not lawful provenance
cycles; the native validator still rejects invalid provenance before extraction.

Before landing, implement and pass:
- Differential relation and exact-query/support parity for all four scopes, profile
  pins, assertion states, literals, Unicode/bytes, sparse/dense/import/fork cases.
- Source-normal production work counts for increasing N, A and S independently,
  including growing prefixes, small page/pack capacities and cache bypass; do not
  substitute the model's physical row counts for actual full-builder evidence.
- Real PostgreSQL EXPLAIN/counter witnesses for staging, unary and guarded search,
  no-match and paged workloads. Reject a single SQL call hiding a full repeated scan.
- Mid-source, mid-stage, final-hash and CAS interruptions; missing source terminal;
  changed bytes/root; late revocation; stale epoch; same-key changed retries; failed
  new generation retains applicable previous one; index loss rebuilds from archives.
- Query ties, generation cursor conflict, revoked key/artifact scope and expiry;
  no cross-member leakage from shared factors or AND operand unions.
- Approved derived-index identity hard cut, new-only schema and exact freeze-fixture integrity; no migration/backcompat tests as success paths.
- Mandatory locked local and exact-SHA remote gates, appropriate final-image full
  publication and independent recovery, source/scratch cleanup and timing review.

The reference is design/qualification input rather than a claim these production
integrations have already run. G22 remains unchanged; #960's approved closeout
and #961/#963/#469 responsibilities remain in force.
