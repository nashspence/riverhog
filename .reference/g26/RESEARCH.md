# G26 research, source findings and alternatives

Source checkpoint: `152d095038df0c0d0ed642cf265d47b11c65a754`.
This research recommends the design in DESIGN.md. It does not establish a released
contract, general query lower bound, or full production performance result.

## Primary literature

1. Bakibayev, Olteanu, Zavodny, **FDB: A Query Engine for Factorised Relational
   Databases** (2012), https://arxiv.org/abs/1203.2672 . Factorization retains shared
   relational structure instead of immediately flattening many-to-many results.
   Its specific algorithms/results are not a theorem about Riverhog's four scopes.
2. Bakibayev, Kocisky, Olteanu, Zavodny, **Aggregation and Ordering in Factorised
   Databases** (2013), https://arxiv.org/abs/1307.0441 . Aggregation and desired-order
   enumeration require evaluation techniques that preserve factorization. This
   supports treating member counts and first-support ordering as part of G26,
   not compressing storage and immediately expanding everything during query/hash.
3. PostgreSQL 17, recursive WITH evaluation:
   https://www.postgresql.org/docs/17/queries-with.html . UNION removes duplicate
   result rows; UNION ALL retains them. A depth/path field can defeat duplicate
   suppression on a diamond or cycle. Use exact semantic state keys and indexed
   worklists, not path enumeration with an outer LIMIT claimed as a safety proof.
4. PostgreSQL 17, set operations and SELECT:
   https://www.postgresql.org/docs/17/queries-union.html and
   https://www.postgresql.org/docs/17/sql-select.html . Set semantics, exact
   correlation and planner materialization choices matter. One CTE name does not
   prove the database avoids repeated work. Verify deployed query plans/counters.

No third-party engine, public graph language, generic factor expression DSL or
new cryptographic accumulator is recommended. A small fixed relational lowering
at the existing discovery owner is sufficient for the scope rules we audited.

## Audited source facts

- `canonical_discovery_relevance.py`: documentary inclusion is broader than causal
  attribution. `_index` / `_walk` record raw assertions, effective State traversal,
  contexts and exact external anchors. Own bound late metadata and imported late
  metadata have different applicability. Imported late claims require BOTH import
  reachability and causal inclusion for the same member; opaque profile payloads
  are not generic graph edges.
- `packages/riverhog-provenance/.../history.py`: retained/bound extents, exact roots,
  source-binding proofs, documentary foreign/fork references, and deduplicated
  history selection have their own validation semantics. A factor graph cannot
  replace these checks with mere reachability or a warm permission cache.
- `canonical_discovery_build.py` and `services/collection_uploads.py`:
  `stage_relevant_member` and `_advance_catalog_canonical_index` reconstruct and
  expand member relevance separately. A snapshot marker only suppresses repeated
  snapshot staging, not member-specific closure and membership generation.
- `canonical_discovery_rows.py` / index models: current assertion identity/storage
  is snapshot-qualified, so multiple exact prefixes can replicate raw assertions
  and postings. A born/retired ledger plus exact snapshot headers eliminates that
  additional prefix-by-assertion storage product without using the latest head.
- `canonical_discovery_index.py`: `_dataset_sha256` feeds every ordered expanded
  membership to SHA-256. Replacing only the membership table with compact links
  while expanding that same stream still hashes the entire flat relation. The
  recommendation is an explicit new derived-index preimage/domain, not an asserted
  shortcut for obtaining the old digest from a different representation.
- `canonical_discovery_search.py`: posting SQL narrows candidates, but native
  `assertion_clause_matches` decides actual same-assertion/profile support. Clause
  supports are ordered; `_read_identity` binds current generation and selectors.
  Shared factors must not widen current access, artifact scope or cursor validity.
- `canonical_discovery_rebuild.py` and the state models: publication is fenced by
  root, collection epoch and pending/active generation. New factors must join that
  lifecycle, not bypass it through a side cache or discard durable operation state.

DESIGN.md maps each fact to explicit tables/accessors, build transitions and query
algorithms. Source hashes in evidence.json pin this inspection. Existing G22 code
is unchanged; its exact-prefix projections can support interval extraction only
after the usual successful neutral validation and current source fences.

## Alternatives seriously considered

### A. Snapshot links, sparse memberships, old expanded generation hash

The integration agent's parked experiment is described in #964 comment 6102286067:
https://github.com/nashspence/riverhog/issues/964#issuecomment-6102286067 . It reports
scope/digest/retry controls and bounded staging, plus reached-source capture. Its
private local patch was NOT available to this producer; do not imply source review.

Reuse its actual tests and owner-correct pieces after reconciliation. It is a useful
bridge but not the chosen final representation: the described count/hash expansion
retains work proportional to all member-assertion pairs. It also does not by itself
remove repeated closure traversal or multiple-prefix raw assertion duplication.

### B. Only adjacency; evaluate every member from scratch on demand

This can keep stored data compact and is useful for rare complex guarded queries.
It is not the common-query plan: repeated State/history traversal per member can
recreate N*shared-history work with few returned members. The chosen design uses
shared reverse minimum-witness propagation for unary paths and an explicit separate
profile for correlated guarded work, with exact prefilters and cached query operand
projections. It makes no universal output-linear claim for arbitrary reachability.

### C. Transitive closure or global bitsets/BDDs

Precomputing all member/reachable-fact pairs defeats compact-build goals. Bitsets
reduce constants but their bit count still grows with the member dimension; BDD
size is data/order dependent. Neither provides the unqualified linear guarantee
wanted here. Bounded query-local sets are implementation tools, not a new archival
representation or a hidden constant-time treatment of a large bit vector.

### D. Keep the old digest with an algebraic multiset hash

Rejected. It would not be the same ordered SHA-256 commitment and would introduce
new collision/security and normalization obligations. Use standard deterministic
hashing of a specified compact source-normal preimage, disclose the changed index
identity and review its protected/state consequences.

## What the prototypes establish, and what they do not

The SQLite FactorIndex model tests exact immutable staging, compact deterministic
sealing, tuple deduplication, same-member AND guards, shared graph traversal and
minimum witness propagation without flat expansion. A second adapter uses actual
Riverhog validated journal/history fixtures and compares exact four-scope tuples
with the maintained member_relevance implementation, including real source-binding
proofs and causal/noncausal imports.

That adapter deliberately retains small Python indexes and per-prefix views. It
is a semantic model, NOT the production source-normal/bounded-memory builder or
PostgreSQL implementation. Its successful scope tests do not validate every query,
foreign-data failure, migration or HTTP access path. The production design explicitly
requires interval facts and virtual parameterized joins instead of those expanded
small-fixture views.

Native tests use the exact upstream RFC8785 0.1.4 implementation in an external
import shim. Its Git blob is `3137d3326b98938affadb1be711ee411eb2ab86e`; it is not
bundled in this reference. No warning policy was relaxed. The locked distribution,
Docker, PostgreSQL and full application/gate environment was unavailable; missing
installed riverhog-server metadata prevented the attempted broader HTTP/rebuild
selection. The attempt is recorded as failed, not passed by excluding its outcome.

All growth counters are model/source-level evidence, not hosted wall-clock or a
completed 128-file qualification. Declarations require the final maintained owner
implementation and exact-source evidence through #963/#964/#469.
