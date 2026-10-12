# G27/G28 owner algorithms retained in the bounded-work revision

Read [DESIGN.md](DESIGN.md) first. These seek, delta and exact-scope algorithms
remain selected, with its lifecycle, streaming, lock-order and growth conditions.
No paragraph here authorizes whole-query transactions, whole-assertion RAM,
unchanged-global-fence retries or an implicit output limit. Native helper/model
results in evidence.json remain historical at their recorded source.

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
counts/nonnegative totals and bounded integer domains. For mutations also publishing a searchable descriptor, acquire the discovery
head coordinator BEFORE collection owner rows; then lock collection rows in
collection-ID order BEFORE modifying contributors, then compute old/new contributions and deltas. All writers
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
complete iterator can execute one scan/order over retained immutable inputs, with
bounded work checkpoints. It must not keep a source transaction open through
that scan; see DESIGN.md for mutable-page and exact-enumeration semantics.
