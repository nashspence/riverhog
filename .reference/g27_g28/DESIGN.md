# G27/G28: exact bounded traversal and named query execution profiles

**Recommended integration design, not accepted owner declarations or a production
conformance claim.** Owning issue #964; external-reference convention #903.
Inspected source: `main@152d095038df0c0d0ed642cf265d47b11c65a754`, tree
`991e994457b487a7dca016b7fc0409485cef25c6`. Reference parent:
`6c66476fc88738b51526c2295f95b5720bb60140`. Reconcile with the exact integrated
post-#963 source. This document supplies the missing G27/G28 recommendations;
it does not reopen the accepted G26 current-baseline/Alembic decision. The later
onboarding clarification (comment 6114967253) is preserved: no detailed cost models
or historical guide snippets are copied into AGENTS.md.

## 1. Decision and non-decisions

Select four complementary changes, in this order:

1. Make ordinary non-null keysets perform real composite index seeks; preserve
   the existing nullable helper and error/token contracts. Separate point,
   ordered-range, residual-filter and complete-enumeration declarations.
2. Evaluate an assertion clause once per source fact/query, with one posting
   traversal and exact same-section semantics, then project through G26's compact
   scope relation. Do not execute whole-assertion matching once per member.
3. Prepare a complete, disk-backed **private query result projection** once for an
   unchanged read/authorization view. Page that projection, rather than rebuilding
   relational joins and rehashing the entire selected collection set on every page.
   Use a transactional invalidation fence as a fast *unchanged* test, not as a new
   public read identity or an authorization cache. Conservative rebuild is the
   selected policy when the fence changes but the existing public digest does not.
4. Replace repeated collection-summary aggregate scans with exact transactional
   delta aggregates. Keep all returned fields and corruption/ambiguity checks.
   Freeze the point-summary bound only after this owner-level work is qualified.

There is no new public search DSL, regular-expression feature, prefix operator,
full-text ranking, count endpoint, snapshot promise for mutable browse, or numerical
latency target. The existing literal language is sufficient to define tractable
profiles; do not quietly restrict it. No blanket `O(results)`/"near-linear" claim.

A complete-result projection is chosen over a resumable partial result stream:
it has an unambiguous completeness boundary, exact minimum supports and count,
and no hidden restart of full query preparation on every page. Its costs are
higher time-to-first-page and output-sized scratch, both explicit. A stateless
recompute-on-every-page implementation is simpler but does not meet the selected
unchanged-view complete-traversal profile. An exported database snapshot held
open for a client lifetime is rejected. A bitmap/all-pairs precomputed history
closure is rejected for the G26 storage-amplification reason.

## 2. Existing contracts to preserve exactly

### G27 is not one consistency contract

**Mutable browse.** `http_api_contracts.browse`, `riverhog_api/browse.py`,
`riverhog_core/browse.py`, `services/collections.py`, `services/search.py`, and
upload-session browse retain operation/selector/principal/key-bound HMAC tokens,
current per-request permissions, existing expiration and validation behavior,
page limits and ordered unique keys. The browse token pins a position, not a
historical database snapshot. Insert/delete/sort-key mutation between requests
can change the set; do not describe the sequence as an exact immutable snapshot.
A quiescent complete traversal must neither omit nor duplicate any selected row.
No OFFSET or growing `NOT IN(all_previously_seen_ids)` implementation.

**Exact discovery continuation.** Preserve the existing request identity and
`riverhog-discovery-read/v1` canonical transcript, returned query/read identities,
three-string position `(read_identity, collection_id, artifact_id)`, order,
source supports, and stale-view `Conflict` behavior. The request identity includes
`page_size`; changing it is not equivalent to changing the mutable-browse limit.
The private workspace ID, attempt, epoch and database row IDs never enter this
wire contract. Changed public read digest still rejects the cursor; a cache miss
or irrelevant mutation alone must not invent a new stale-cursor rejection.

**Sealed ordinal authority pages.** `services/retrieval.py:list_plan_artifacts`
already uses `(plan_id, artifact_order >= ordinal)`, a bounded lookahead and an
exact sealed ETag/owner/key fence. Preserve this path rather than replacing it
with the mutable helper. Apply the same classification to existing exact-set and
exact-authority OpenAPI elements discovered by Contract Closure; do not copy this
research family inventory into main as another surface registry.

**Internal complete iterators.** `iter_collections`, `iter_archive_copies`,
`search.iter_artifacts` and upload `iter_artifacts` currently keep a read snapshot
through generator consumption. Preserve their exact owner-defined iteration view.
Use explicit `close`/`finally` on early exit and cancellation. A DB cursor may be
held during a bounded server-owned capture, not unbounded HTTP consumer waiting.
At an egress boundary capture into managed disk storage under one snapshot, close
it, then transmit bounded frames; retain completion/integrity witnesses. Charge
capture, disk bytes, cleanup and returned bytes, not only the final page read.
Do not silently split an exact internal iterator into independent snapshots.

### G28 semantics are closed and specific

`riverhog_protocol/artifact_discovery.py` supports `equals`, `contains`, and
`bytes-equals`; `exact` and ASCII-only folding; exact JSON Pointers; optional
(kind, assertion state, exact profile pin); four distinct scopes. There are at
most 16 clauses and 16 values per clause in the current request. String values
are bounded by 256 codepoints and 1024 UTF-8 bytes. These are existing query-size
limits, not a license to cap assertion/member/result cardinalities.

All values of a clause match **one assertion and one declared pinned profile
section**. Different clauses may have different witnesses for the same member.
Keep `bool != int`, exact byte representation, native pointer escaping and
literal `%`, `_`, backslash behavior. ASCII fold is not Unicode casefold. Opaque
nested profile lookalikes do not become schema-declared sections. Keep scalar
posting order, first matching posting per predicate, and first complete declared
profile section; do not choose the section that happens to finish first.

Keep G26's `member`, `input-history`, `collection`, `recorded-history`, effective /
retracted / any distinctions. Imported late claims need BOTH imported history
and causal applicability for the **same member**. They are not ordinary OR edges.
Choose the same minimum support `(journal_id, sequence, assertion_id, scope)`;
use G26's owner-reviewed tie treatment only where that design requires it.
Complete all potentially smaller guarded witnesses before accepting a unary
minimum. Scope indexes are derived from authenticated archive/journal facts,
not new provenance authority. Query support must identify the original source.

## 3. G27 implementation

### 3.1 Non-null seek fast path

Extend `keyset_statement` with an owner-explicit keyword such as
`nonnull_ordering: bool = False`. Keep all native page-size, order and position
arity checks first. For a nonempty position with all non-null values, and an
owner-proven non-null ordered tuple, emit:

```sql
WHERE (k1, k2, unique_id) > (:p1, :p2, :pid) -- asc
ORDER BY k1 ASC, k2 ASC, unique_id ASC
LIMIT :page_size_plus_one
```

Use `<` and plain DESC for descending order. On proven non-null keys, omit
redundant NULLS modifiers on BOTH initial and continuation pages: SQLite can
otherwise introduce a sort. The nullable fallback keeps its native null ordering.
A one-column key can use a
scalar comparison. `sqlalchemy.tuple_` is the portable composite implementation
for the maintained SQLite/PostgreSQL backends. Preserve all base predicates.
Do not strip conditions from an already-built native statement; construct the
fast predicate from the original base statement. Do not infer proof from a
non-null cursor, or from an ORM column subsequently made nullable by an outer join.
Nullable/unproven keys retain the existing helper, receive a separate scan/index
profile, and must still pass the native ordering/error oracle. Future optimized
nullable partitions need their own proof, not sentinel COALESCE values.

Opt in at the actual non-null catalogue/member tuples in `services/collections.py`
and `services/search.py`, and other proven call sites found during Closure binding.
Discovery's private result tuple is non-null by DDL. Keep a unique final tie key.
A matching compound B-tree must have equality-fixed selectors first, then the
full ordering tuple; reverse scans are acceptable for uniform reverse order.
Keep key collation the same in index, SQL ordering and cursor interpretation.
Artifact IDs are fixed lowercase hexadecimal and collection IDs are numeric;
do not sort serialized collection-ID strings lexicographically.

Current member PK `(collection_id,artifact_id)`, global artifact-order and
bytes-order indexes cover their named operations. Add a normal B-tree
`(sha256,collection_id,artifact_id)` for an unscoped exact payload digest lookup;
the existing `(collection_id,sha256,artifact_id)` is not the same access path.
Keep the existing indexes needed by other owners. This additional index charges
one index update per inserted/deleted member. Do not claim every combined sort /
selector pattern is index-covered: validate the supported combinations, and
assign the remainder an explicit residual-scan/sort declaration.

Capture EXPLAIN and actual rows/VM instructions at both the beginning and deep
inside the key range. An index name in an EXPLAIN output is not proof of a seek:
`SCAN ... USING INDEX` may still restart at the beginning on every request.
A nominal tuple SEARCH is also insufficient: on the inspected SQLite engine,
`(k, integer_rowid_alias) > (...)` can seek only `k` and rescan the entire tied
prefix. The retained negative probe demonstrates this. Require an all-leading-
keys-equal adversary and a genuinely deep cursor, not only short tie groups.

For an owner whose tuple plan only partially seeks, select the tested
`seek_partitions` fallback in `reference.py`: for a d-key boundary, visit disjoint
ranges in descending prefix length. For `(a,b,c) > (pa,pb,pc)` these are
`a=pa AND b=pb AND c>pc`, then `a=pa AND b>pb`, then `a>pa`. Reverse the strict
comparison and ordering for DESC. Order each range by the complete tuple, keep
all base predicates, and fetch at most p+1 per range. Consume ranges in this
order in ONE request snapshot; stop once p+1 combined rows exist, then truncate
to p+1 before normal lookahead handling. The ranges are disjoint and already
lexicographically ordered, so no global sort or dedup is needed. Retain a unique
final key. Each physical plan must seek its equality prefix plus scalar range.
Worst-case page work is `O(d*log Z+d*p+B_fetched)` and bounded `O(d*p)` rows;
d is the owner's explicit fixed ordering width, not an unbounded hidden constant.
The matched member-text tuple path and the rowid prefix-partition fallback have
separate complete-traversal work tests with 512/2048-row tie-heavy inputs, both
orders. Retain the simpler scalar seek for a one-column integer primary key.

The probe executes the native SQLAlchemy helper and the candidate, not an
invented OFFSET stand-in. Its rowid tuple runs are retained as UNQUALIFIED
negative controls, not evidence of the selected indexed-page bound.

### 3.2 Filter and authorization domains

`page_size` bounds *returned material*, not all search work. For a scan profile,
define the examined domain from the authoritative relation after declared fixed
equality scope, before residual predicate/authorization evaluation. State its
size and predicate bytes. Do not define it as "whatever rows the planner touched."
Choose exact artifact-capability membership as a driving relation where narrower;
use indexed semijoins rather than a large materialized permission IN list.
Charge the real authorization delegate and check live capability/claim expiration,
revocation, consumer key and fence on every request, including a cached page.

### 3.3 Collection summaries without hidden fan-out

The current `_collection_summary_query` performs per-collection copy counts,
object/publication byte sums, current description/tag coverage, and manifest
min/max. It is NOT a constant-size point lookup merely because the HTTP result
is small. Select/filter/page **collection keys first**, then enrich at most the
returned page plus lookahead; do not evaluate summaries for a skipped prefix.
Implement these derived relations under the catalogue owner:

- `collection_summary_totals(collection_id PK, archive_copy_count,
  remote_object_bytes, description_bytes, tag_head_bytes, tag_node_bytes,
  retained_copy_count)`. Sum remote storage exactly as the present four sums;
  do not exclude retired/failed/pending rows that the native sum includes.
- `collection_summary_description(collection_id, published_revision,
  published_identity, copy_count)` unique on the first three fields.
- `collection_summary_tags(collection_id, published_revision,
  published_tag_set_identity, copy_count)` unique on the first three fields.
- `collection_summary_manifests(collection_id, sha256, object_count)` unique and
  ordered on `(collection_id,sha256)`; keep only positive counts.

For each copy `(collection_id,store)`, its retained contribution is precisely
`copy.state == uploaded AND NOT EXISTS retirement`. Its description contribution
is one count at the publication's `(published_revision,published_identity)` iff
retained AND description.state == published. Tag contribution is analogous.
The read compares the group at the collection's **current** revision+identity
with retained_copy_count, preserving the existing zero-retained behavior.
Changing a current collection revision is then a point lookup, not a scan or
rewrite of all copies. Manifest contribution includes exactly rows with
`object_id == manifest`, regardless of other state, as the current query does.
Read ordered first/last manifest groups with LIMIT 1. Preserve missing/ambiguous
root failure rather than replacing it with `CollectionRecord.archive_root_sha256`.

Maintain counters from old/new base-row contributions in the SAME transaction.
For copy/retirement/publication state changes, read at most the affected copy's
old and new description/tag contribution (point keys); decrement old groups,
increment new groups, remove zero-count groups. Object/publication byte changes
add `new - old`; insertion/deletion use zero on the absent side. Enforce positive
counts/nonnegative totals and bounded integer domains. Acquire the collection owner row locks BEFORE modifying contributors, in
collection-ID order, then compute old/new contributions and deltas. All writers
for a collection use that same coordinator; do not wait for an AFTER trigger to
serialize a stale cross-table snapshot. SQLite uses the existing serialized write
transaction discipline. Lock affected summary keys in the same order. Exact duplicate retries add no delta;
conflicting retries must still fail at their current owner boundary.

Centralize this in catalogue-owned SQLAlchemy helpers plus database-level
invariant tests covering EVERY write path. The mutation checklist is
`collection_uploads`, `archive_records`, `archive_copy_jobs`,
`archive_copy_retirements`, `collection_descriptions`, `collection_tags`,
`mutable_document_reclamation`, `collection_mutations`, `collection_deletions`,
and reconstruction/bootstrap paths in `catalog_db`. Bulk deletes and cascades
must apply grouped deltas or tested triggers; do not only hook ORM instance events.
Do not replace the selected coordinated owner writes with ORM-only callbacks or
untested cross-table AFTER triggers. The selected invariant is
transactional old/new deltas, never asynchronous reconciliation before reads.

Initialize from current state once in a bounded grouped scan when explicitly
rebuilding derived summaries; those source records include local operational
state and are not replaceable by archives alone. Do not delete receipts, pending
jobs or other authority to make a rebuild convenient. Missing/inconsistent
summary state fails closed until rebuilt; no partial zero defaults. Current
pre-v1 baseline DDL/models/fixtures are edited together, retaining Alembic and
synthetic forward upgrade/interruption tests. Test against the unchanged native
aggregate oracle through all mutations, rollback, duplicate retries and restart.
This part is a precise design, not implemented by `reference.py`.

## 4. G28 predicate evaluation and query algebra

### 4.1 One exact predicate evaluation per fact

Use one query-local fact match relation keyed by `(query clause, immutable source
assertion identity, applicable state/prefix class)`. Reuse the exact payload's
lexical match across memberships and snapshots, but apply effective/retracted
intervals separately; different assertion payloads or source journals never alias
merely because assertion IDs coincide. Identical payload matching may be shared
by verified payload digest only with source attribution kept separately.

Replace the pinned-section Cartesian filtering in `assertion_clause_matches`
with `clause_matches_once` in `reference.py`, adapted into its existing owner.
Build a character trie for the matching declared section `/data` roots, recording
the original declared-section ordinal. Walk each posting pointer once, respecting
path boundaries (`/data` or `/data/`, not `/database`). The current core's declared
roots are disjoint; add an invariant test over its declared slots. A future core
schema adding overlapping roots requires owner analysis, not silently choosing
one trie terminal. For each routed posting update the first unmatched predicate
winners for that section. After traversal choose the first fully satisfied
section in native declaration order. Without a profile use one winner vector.
Preserve the returned native `ScalarPosting` objects exactly.

Costs: input payload parse/portable validation/posting extraction plus key ordering
already required by the owner; `O(L + d*B_values + d*V_values + G*d)` for routing and
literal evaluation with `d <= 16`, `L` total constructed/read pointer characters,
`G` matching declared sections, and the bounded query-literal length retained as
an explicit condition. Comparison/hash/key bytes are not unit-cost if unbounded.
Python dictionaries give expected trie edge lookup, not worst-case hashing.
This removes the `G*V_values` section-filter product. It does NOT replace the
materialized JSON parser or claim constant memory for an unbounded assertion.
Peak includes the raw assertion plus winner pointers `O(G*d)` and trie characters.
A production external-memory parser is a distinct owner change, not required by
this helper claim. Charge sorting of object keys and pointer construction.

Keep SQL posting lookup as a candidate superset until exact verification. String
LIKE/collation, chunk overlap and prefix filtering cannot become an authority
substitute. For exact typed equality, use a bounded type/representation/value
hash key and verify exact value/source bytes on collisions; arbitrary long
TEXT cannot be promised to fit an unlimited B-tree key. For strings folded with
ASCII mode compute the same native normalization. Fixed-length hashes require
collision verification, not a mathematical no-collision assumption.

Evaluate all predicates within one declared section; SQL must correlate both
assertion source and section identity. A join of independently satisfied values
from different assertions or profile sections is wrong even if DISTINCT removes
result duplicates. Whole raw assertion matching occurs once per source/query,
not once per output member or once for each page.

### 4.2 Source-factored query plan

Consume G26's source-normal fact/index adapter. No new rule engine and no copied
expanded Membership table. Each clause produces one disk-backed relation:
`clause_member(clause_ordinal,collection_id,member_id,minimum_support_id)`.
Each support row retains source coordinates, scope and native exact value
supports, shared by ID instead of copying a large assertion into every member row.

For ordinary unary/shared histories: seed exact matching facts, apply born/
retired prefix intervals, propagate minimum support through G26's typed reverse
selectors. Settle a selector once at its smallest key, deduplicate nodes/edges,
and merge shared roots. The priority queue and visited set spill to indexed disk.
Do not enumerate paths in a diamond DAG. Charge queue/index comparisons explicitly.

For imported guarded histories: compute a safe OR-relaxed candidate set, then
run G26's exact same-member AND-guard fixed point on each query-relevant candidate
slice. A guard activates only when both sides are reached for that member. Never
use another member's import witness. Memoize/deduplicate `(member,typed selector)`
within the query; no exponential path recursion. The profile's conservative
worst case is a sum of candidate slices, bounded by `C_guard*(V+E)` before index
costs. This is a justified graph/query bound, NOT strict linear or output-linear.
Sparse zero-output correlated cases must be measured. Do not mask this profile
as ordinary unary propagation or invent a faster algorithm without proof.

Intersect clauses by `(collection_id,member_id)`, retaining one minimum per clause.
Implement intersection by ordered merge or keyed semijoins over the clause-member
relations, not support-tuple cross joins followed by DISTINCT. The output has at
most `d_clauses` support links per matched member. Scan all potentially smaller
supports before sealing. A zero-result projection is valid only after terminal
clause/selection traversal. Exact counts, where an internal consumer needs them,
count distinct output members, never supports or raw join rows; no new HTTP count
operation is introduced.

### 4.3 Legacy textual search remains a separate mutable-browse operation

`services/search.py:_search_candidates` currently unions member matches for ID,
payload digest, collection text/description and tags. Preserve its normalization,
substring meaning and sort/error behavior. Push explicit collection and admissible
authorization scope into candidate construction. First find matching collection
IDs/tag membership, then semijoin member rows once; do not fan out every matching
tag through all members before deduplication. Keep independent member ID/digest
matches in the union. Apply the actual principal filter before projecting results.

Named B-tree equality lookup applies only to exact fields/routes that use equality.
Legacy `%literal%` matching is not a prefix search. SQLite general substring and
short/nonselective PostgreSQL patterns get a searchable-input scan profile.
Existing PostgreSQL trigram indexes are useful accelerators, not a universal
`O(log Z + output)` promise, and their candidate verification/sorting costs count.
Do not change SQLite's existing LIKE-based public behavior during this performance
change; canonical discovery's exact native verifier continues to distinguish its
own literal semantics. Pagination reexecutes mutable query work by design, so a
full multi-request legacy scan traversal declares the sum of its per-request scan
costs; it does NOT inherit discovery's prepare-once profile. A quiescent internal
complete iterator can execute one scan/order, with capture and output charged.

## 5. Private exact query projection and lifecycle

### 5.1 Relational interface (private to server owner)

Add `canonical_discovery_query.py` and `catalog_discovery_query_models.py` (names
proposed, not cross-package APIs). `SqlAlchemySearchService.discover` orchestrates:

```text
prepare_query(request, principal, expected_read_identity | None) -> ReadyQuery
read_query_page(ready_query, after_member, page_size, principal) -> native page
release_query_attempt(query_id, attempt, reason) -> bounded cleanup
```

Suggested persistent records, current baseline only:

```text
discovery_read_fence(singleton PK=1, source_incarnation, revision bigint)
discovery_query(query_id PK, source_identity, query_identity, principal_binding,
  authorization_view_identity, algorithm_revision, read_identity,
  observed_fence_revision, attempt, phase, lease_owner, lease_until,
  expires_at, result_count, support_count, scratch_bytes, terminal_digest)
discovery_query_result(query_id, attempt, collection_id, artifact_id, ...small hit...,
  PK(query_id,attempt,collection_id,artifact_id))
discovery_query_support(query_id, attempt, support_id, exact_native_support_payload,
  PK(query_id,attempt,support_id))
discovery_query_hit_support(query_id,attempt,collection_id,artifact_id,clause_ordinal,
  support_id, PK(query_id,attempt,collection_id,artifact_id,clause_ordinal))
```

Index workspace reuse by the full query/source/principal/authorization/algorithm
binding and phase/expiry. Never alias distinct principals when the optional native
authorization-view identity is null; always bind the explicit principal and key
identity as the browse contract does. Support payloads must be self-contained
native source references/value evidence, or source generations must remain leased
through use. Choose self-contained support payloads for READY projections so
routine old-build GC cannot break a valid page. Large raw assertions stay in the
G26 source store, not each support payload. Result ordering keys are NOT NULL.

`algorithm_revision` is a private cache schema/algorithm discriminator, not a
public digest extension. Workspaces are rebuildable, never authorization,
source evidence, receipts or recovery authority. Do not merge `.reference` model
classes into main or expose their model hash/exception types.

### 5.2 Atomic invalidation fence

Current `CatalogSyncStateRecord.committed_revision` alone is insufficient:
`canonical_discovery_index.publish_index_build` switches `active_build_id` without
publishing a catalog event. Add a separate private monotone transactionally
updated fence; its source_incarnation is the existing catalogue source identity,
not an independently invented source authority. Do not use a
PostgreSQL SEQUENCE as a transactional commit clock. A rollback must roll back
both catalogue change and fence bump. Overflow fails the mutation, not wraparound.

Install equivalent current-baseline SQLite/PG triggers for any INSERT/DELETE or
relevant UPDATE to `CollectionRecord`, `CollectionTagMembershipRecord`,
`CollectionArtifactRecord`, provenance IndexState and Generation header rows,
and catalogue source-identity reset. Initial correctness-first implementation may
bump on ANY update to these header/base tables (including pending generations).
Do NOT bump for query workspace changes or immutable fact staging invisible to
readers; completed source facts must remain immutable, enforced by the G26 owner.
Coarse false invalidation is allowed and measured; missing invalidation is not.
Use exact SQL table names from model metadata when generating DDL; test trigger
coverage from mapped dependencies, bulk SQL writes and cascades, not only ORM hooks.
Version the private fence source incarnation on destructive DB reconstruction.

Application access policy and time-dependent capability expiry are checked anew
through their existing owner before every page; a DB change clock cannot notice
time passing or replace current key/claim validation. Query-cache lookup takes
the current authorization fingerprint. Dynamic grant inputs not represented by
that fingerprint must either participate in invalidation or disqualify reuse.

A READY query can be reused only if its complete source/principal/query binding
and its observed `(source_incarnation,revision)` equal the current values in the
request read snapshot. This is a sufficient unchanged test, not a proof of what
changed. When different, recompute the **existing** `_read_identity` transcript.
A different digest -> existing Conflict. Equal digest -> internal cache miss and
full preparation at that same public digest, not 409. This conservative rebuild
also covers mutable rows not individually represented by the old transcript;
do not infer unchanged query results solely from that digest. An irrelevant
change can cost a rebuild but cannot silently change public invalidation rules.
`ProjectionStore.RebuildRequired` demonstrates that distinction; it is internal.

### 5.3 Preparation, ready publication, serving and recovery

1. Validate request/token and current authority with native behavior. Single-flight
   preparation by full private binding uses a monotone attempt and lease, not a
   process-global unbounded dictionary. A stale worker cannot publish or delete
   a later attempt. Join or recompute after lease expiry; no new public async API.
2. Under one explicit backend read snapshot, capture the source/fence stamp and
   evaluate selected-collection transcript with the current native validator.
   Build the complete G26 query projection into **private separate scratch**, so
   a SQLite read snapshot does not attempt to upgrade itself into a writer. Stream
   native rows with bounded fetches; spill graph queues/sets, sorting and results.
   Keep that snapshot through query evaluation for source coherence; it is owned
   by preparation, never by HTTP page delivery or client think time. A killed or
   resource-exhausted preparation is incomplete and publishes no READY result.
3. Close source cursors/snapshot before handing results to HTTP. Copy bounded
   result/support chunks into the private BUILDING database attempt, charging both
   scratch and durable bytes. Record terminal traversal and exact row/support
   counts, plus a deterministic integrity digest over the sorted workspace rows.
   Retries use unique keys: identical row replay is no-op, conflicting replay fails.
   Maintain inserted counts by actual inserted-row deltas, not COUNT(*) each batch.
4. In a short publication transaction, lock the attempt and fence in the declared
   global lock order; take the fence lock first and the attempt lock second (never lock source rows
   for update in this transaction), then confirm attempt/lease and that the source fence is unchanged
   since the read snapshot. Different fence -> abort/restart preparation (after
   native read-identity check for continuation), even if the public digest happens
   to be equal. Validate terminal manifests/counts/digest before atomically setting
   READY. A partially copied file/table is never serveable. Release scratch in a
   finally path and independently reap abandoned attempts with fenced ownership.
5. Each page authenticates/checks current permission/capability and validates the
   signed token first, then reads READY metadata, current fence and `p+1` sorted
   result rows in one request snapshot. Fetch at most `p*d_clauses` support links
   by bounded joins; avoid one whole graph query per hit. Serialize bounded material
   after closing DB resources. More -> next position is last RETURNED member;
   otherwise complete=true, no token. No page budget may silently skip matches.
6. Eviction/expiry/missing local workspace is a cache miss, not evidence of an empty
   result or changed read. Rebuild from source at the cursor's native read identity;
   normal stale/permission errors still apply. READY data survives process restart
   if present and valid; no snapshot handle is required. Expired/cancelled workers
   fail fenced writes. Deleted/corrupt workspace state fails closed and is rebuilt.

Quota by bytes, rows, active queries, per-principal and aggregate storage; do not
use page_size as a result limit. Include raw-row maximum working set, backend
cursor buffers, DB temp spills, snapshot duration, copy amplification, queued
attempts and cleanup. Select existing resource/lease configuration patterns in
RuntimeConfig with explicit validation; no arbitrary new numeric acceptance caps
or timeout relaxation is proposed here. Capacity rejection uses the existing
unavailable/resource-failure boundary, not a successful shortened result.

## 6. Proposed operation-scoped declarations

Integrate these profiles at their existing owners using #963's common declaration
and evidence machinery. These labels are research names, not a second registry.
Let `I(k,Z)=O(k log(2+Z))` fixed-width indexed operations, `S(k)` comparison sorting
with compared bytes charged, `P` pages, `p` page capacity, `B_out` output bytes,
`C` selected collection headers, `N` selected members. Add actual authorization
and delegate costs; no composite passes with a missing delegate.

| Profile | Completion boundary and proposed work | Memory / retained state |
|---|---|---|
| G27 point-member / sealed point metadata | one exact response: `I(1,Z)+B_out` plus live access | response + bounded DB page buffers |
| G27 point-summary, AFTER delta integration | fixed number of aggregate/group/min-max point probes: `I(1,Z_summary)+B_out` plus access; base-row writes pay delta index work | constant number of groups read; retained aggregate groups bounded by base contributors |
| G27 ordered range, no residual scan | one page `O(log Z+p+B_page)` with real matching seek; quiescent traversal `O(N+B_out+P log Z)` | `O(p+B_page)`; no full seen-ID set |
| G27 residual-filter page | one declared base-domain scan/seek plus predicate/access costs and optional `S(matches)`; state actual scanned logical domain | bounded page; sort spills; do not relabel as index-only |
| G27 exact internal enumeration | one snapshot scan/order + capture + output, not the cost of unrelated repeated HTTP calls | bounded RAM + output-sized disk capture if egress requires it |
| G28 typed indexed fact lookup | exact matching value posting domain, verified source values and interval states + `I` + member projection delegate | bounded candidate batches + query-local disk dedup |
| G28 literal searchable-input scan | one pass over defined selected unique searchable bytes/values, bounded program multiplier and pointer/key-order costs + projection/order | largest admitted source assertion + configured buffers, spill for joins/results |
| G28 common unary scope propagation | once per clause: unique relevant facts/typed nodes/edges + queue/index work `I(V+E,Z)` + emitted clause-members | `O(V+E+clause_members)` disk, bounded queue/cache RAM |
| G28 same-member guard profile | exact sum of member-specific relevant slices; conservative `O(C_guard*(V+E))` plus indexed work and literal costs | reusable scratch per member plus dedup/output relations; not a claimed strict-linear profile |
| G28 intersect/seal | ordered clause-member intersection or indexed semijoins; `S(Q)` only if order not already available; at most clause-count links per output member | `O(Q+support_payload_bytes+Q*d_clauses)` disk; bounded staging RAM |
| G28 exact projection pages, unchanged view | preparation ONCE (`C` transcript + selected query profiles + seal/copy), then `O(P log Q+Q+B_out)` plus live auth for all pages | page-sized RAM; complete-result disk charged at preparation |

Changed fences, evictions and retries explicitly repeat preparation. For `R`
admitted preparations the lifecycle bound is the sum of their actual declared
input dimensions and profile costs, plus all page reads and cleanup; arbitrary
unbounded churn has no finite completion bound. No claim that an unrelated
mutation is free. Metadata-only digest validation is not the entire query cost.

Searchable bytes and candidate/slice domains are defined from source relations
before running the query. Output, unique raw facts and shared logical membership
are distinct. Hash collision checks, JCS/object-key sorting, SQL index maintenance,
DB row visits, source reads, temp writes, copying and result serialization remain
visible. Existing page or query-literal maxima are conditions, not proof that an
algorithm scales in independently varied members/facts/history/program size.

## 7. Landing sequence and acceptance checklist

**A — pure owner helpers.** Integrate non-null keysets and the one-pass matcher,
register exact operation bindings and narrow counters, pass native semantic oracles
and full null/tie/error tests. No query schema dependency. Do not opt in nullable
or residual-filter statements to the index-only declaration.

**B — owner data paths.** Integrate collection-summary deltas and required member
lookup indexes in the one current `v1_0001` baseline. Prove native summary parity
through every listed mutation path and trigger/bulk/cascade/rollback case. No
old-schema converter, no parallel pre-v1 reader. Keep Alembic intentionally.

**C — G26 + G28 plan.** On the accepted G26 fact schema, implement fact matching,
common unary and same-member guard paths, clause-member intersections and exact
minimum supports. Differential against native expanded semantics on compact
fixtures: independent/shared/partially shared/dense, effective/retracted prefixes,
real imported binding proofs, and correlated negative/late-claim cases. Do not
hide an expanded membership product in staging or in the generation digest.

**D — private result lifecycle.** Install the complete fence trigger coverage,
query attempts, bounded scratch/staging and ready publication. Integrate with
`discover_artifacts`, `SqlAlchemySearchService.discover`, API response/token path.
Keep request schema and supports unchanged. Add source-fence drift, auth expiry,
ABA/recreate/source reset, GC, copy failure, killed writer, lease replacement,
rollback and multi-connection tests on SQLite and PostgreSQL. A BUILDING or
incomplete empty projection cannot serve. An unrelated change may rebuild but
must not become a new public 409. Check current auth on every cache hit.

**E — conformance evidence.** Bind profiles/counters and witnesses in existing
Closure/Audit/#963 machinery; derive surface coverage. Run both backend query
plans and work witnesses, cold and warm cache, matching and absent/sparse output,
first/deep/terminal pages, many ties, all grant patterns, many shared snapshots,
large individual assertions, and cheap 2x cardinality series. Use absolute semantic
work counters and generous justified implementation envelopes, not only noisy
wall-time ratios. The small SQLite/native observations here do not qualify PG.

**F — ordinary integration rail.** Locked formatting/type/lint/unit, distribution
smoke, build, required Linux/image/recovery/security/complete lifecycle proofs and
exact-SHA CI/CodeQL, with #963 distinct functional/growth/timing outcomes. Carry
#960's maintained 600-second report-only profile and explicit shortfall disposition;
do not waive it or claim it met. #469 consumes approved declarations and executed
witnesses. Full owner approval of formulas/applicability/resource conditions and
these proposed private schema changes remains an acceptance checkpoint.

`reference.py` and its 53 tests are executable algorithm/lifecycle input, not the
production integration described in B–F. `RESEARCH.md`, `counters.json` and
`evidence.json` distinguish inspected source, controlled native helpers and model
observations. No runtime/mainline, archive identity, release or provider change
was made by publishing this reference.
