# G27/G28: bounded work over retained exact inputs

**Maintainer-approved policy and architecture for integration, not deployed
behavior or completed qualification.** The exact snapshot-at-admission discovery
and scoped current-only public contract change were approved in
[#964 comment 6118482544](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118482544).
Owning protocol/HTTP/client encodings, resource parameters and actual
semantic/equivalence/authority proof remain acceptance obligations. Issue #964; reference convention #903. This revision responds
to the maintainer's instruction to revisit the earlier design as intent
calibration, not to preserve its architectural choices merely because they were
previously recommended. The selected changes and their public consequences are
explicit below. Existing archive, custody and live-authorization authority are
not made optional by that instruction.

Audited source: `main@152d095038df0c0d0ed642cf265d47b11c65a754`, tree
`991e994457b487a7dca016b7fc0409485cef25c6`. Direct reference parent:
`263abca4d8b09f390d90f2b977b10c1557961bc1`. Reconcile against actual post-#963
integration source; supplying this design does not close the issue's acceptance
gates. This document replaces the prior G27/G28 default, including its §§2,5,6,7.
[ALGORITHMS.md](ALGORITHMS.md) retains the detailed owner algorithms that survive.

## 1. Default policy to adopt

**Bound resources by the work step; preserve meaning across steps; charge the
whole lifecycle.** A finite logical total is not limited by a request, buffer,
transaction, checkpoint, lease or diagnostic fixture size. A coherent logical
snapshot is a retained immutable input identity, not a database connection.

1. Every operation over an arbitrarily large permitted total uses bounded
   exchanges and interruptible, checkpointed work quanta. Bound fetched/decoded
   bytes, physically examined rows/edges, emitted bytes, atomic write sets and
   continuation state, not only the number of returned hits. A SQL LIMIT after
   an unrestricted scan/join/sort does not establish bounded work.
2. Close database cursors, transactions and borrowed connections at a bounded
   fetch/commit boundary, before evaluation, external I/O, HTTP delivery, sleep
   or client think time. No full-query or full-export MVCC snapshot. An exact
   internal generator must not transfer transaction lifetime to its consumer.
   Small point/page transactions remain appropriate; prove their access plan.
3. Exact long operations pin immutable inputs once and retain them through
   completion or explicit termination. Checkpoint progress and bounded state;
   process restart or ordinary content changes must not restart completed work.
   No whole-catalog unchanged-fence loop as the default correctness mechanism.
4. Match each unique fact once within the declared query scope; share immutable
   source work and factor relationships. Charge indexing, ordering, program
   size, required dense output and hard correlation separately. Do not rename
   accidental repeated scans or expanded shared history as necessary input.
5. Capacities are source-linked operator policy with explicit admission,
   defer/throttle/failure semantics. Never report partial work as complete or
   turn a memory/transaction quota into an undocumented logical-total maximum.
   Large values require segmented processing, not a hidden per-value RAM cap.
6. Admission and scheduling account for CPU, I/O, scratch, retained versions,
   active queries, and cleanup. Fair bounded scheduling prevents one query from
   monopolizing a worker or connection. Retention is finite operational storage,
   not free merely because it is immutable. Release resources incrementally.
7. Current authorization is independent of historical content. Pins do not grant
   permission, extend claims, defeat revocation, or become archive authority.
8. Declare and measure preparation, all quanta, checkpoint writes, page reads,
   recovery/replay, cold-cache reads, maintenance and reclamation. Timing misses
   remain distinct from invalid or missing growth/functional evidence.

These are repository-wide default **requirements**, not a universal pagination,
job, snapshot or component API. Existing owners retain their progression models.
A justified exception requires an operation-specific owning declaration, reason,
resource conditions, expiry/cancellation behavior and executed witness. Calling
an operation "internal", "read-only", "one request" or "bounded-memory" is not
an exception. A finite test or operator timeout alone is not a complexity proof.
Do not promise hard real-time completion if storage or the operating system hangs.

## 2. Selected architecture and deliberate contract changes

Select a **private copy-on-write directory of searchable collection versions**,
short atomic root pinning, leased immutable source generations, and durable
cooperative query work. This is a concrete Riverhog catalog implementation, not
a new archive format or a general framework imposed on applications/components.

Do NOT select (a) complete evaluation inside one database read snapshot,
(b) chunking mutable reads and restarting the entire query when a global fence
changes, (c) copying the complete catalog on every admission, (d) a new whole-
collection member/history product, or (e) mandatory all-results preparation for
simple indexed reads. A versioned-row/event-history alternative can be correct,
but its predecessor lookup, retained-change scans and reclamation costs must be
proved; it is not the chosen default. Existing catalog-sync's live bootstrap and
change-feed progression is not repurposed as an exact immutable query view.

### Exact discovery: snapshot at admission, not latest on every page

The first accepted discovery invocation pins one complete searchable directory
root. Selection uses its exact published collection metadata, member indexes,
G26 generation identities, and admission authorization policy. Every subsequent
work step and page uses that same view. A newer description, tag set, added
collection or reindexed generation does not change or restart the query. A new
invocation is how the caller asks for newer content.

This intentionally replaces the earlier recommendation to preserve
`riverhog-discovery-read/v1`'s selected-header rehash/stale-current-view behavior.
It is an observable pre-v1 contract revision, NOT a hidden cache optimization
and NOT implied by the separate G26 extraction-identity approval. The revised
snapshot/read/token/pending semantics are now explicitly **approved** by
[#964 comment 6118482544](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118482544).
Freeze their exact owner-owned protocol, HTTP/client/CLI fields, formulas and
test vectors before claiming integration acceptance. Reject retired
read/cursor formats; no compatibility mode or rolling dual interpretation.

Keep query literals, same-assertion/profile correlation, exact source-support
meaning (with explicitly segmented transport below), numeric collection-ID plus
artifact-ID ordering, all four provenance scopes,
current page-size/query maxima and archive/member/history identities unchanged.
A snapshot identity is a server query-view commitment, not archive authority.

### G27: keep distinct consistency contracts

* Mutable browse stays mutable between logical pages. A non-residual indexed page
  uses the proven seek helper. If a page requires a potentially unbounded residual
  scan, pin a private input view for that **one logical page** and expose bounded
  pending work until it is complete. Its next browse-page token remains only a
  position; the next logical page selects a new current view. A pending-work token
  is not the next-page token. The `/search` owner is the concrete initial consumer.
  This adds an explicit pending outcome where needed without promising a stable
  whole-browse snapshot or calling repeated per-page scans linear traversal.
* Exact discovery keeps one pinned view for the whole invocation. Use a streaming
  indexed plan when order and completeness can be proved incrementally; otherwise
  use the resumable prepared-query plan in §5. Both have identical view semantics.
* Sealed ordinal authority pages keep their existing root/ETag/ordinal contracts.
  Do not replace them with browse tokens or query workspaces.
* Exact internal enumeration pins its owning immutable source view or captures
  it through an already proved restartable source-version mechanism. Then release
  DB resources after bounded loads. A short transaction over a mutable database
  followed by another is NOT an exact snapshot. Apply this explicitly to searchable
  catalog enumeration; archive-copy/operational iterators need their own source-
  version binding and cannot falsely inherit the searchable directory's coverage.

## 3. Immutable source directory: concrete data and writer protocol

### 3.1 What a leaf commits

A private `discovery_collection_version` identifies exactly one searchable
published collection view: numeric collection ID, catalog incarnation, archive
root and archive generation, artifact-set and provenance identities, immutable
member-inventory handle, description revision/identity and bounded-content handle,
tag revision/set identity and immutable tag-node root, search-normalization
revision and exact ingest-source/search-text handle, G26 complete-generation handle/core/extraction identity or explicit
unavailable-index state. Include every field actually read by selection or
projection; no query-time fallback to mutable `CollectionRecord` or live tag rows.

The leaf is fixed-shape references and scalars, not embedded member/tag/assertion
lists or a whole description. A pinned generation's rows and referenced payload
segments are immutable. Archive-qualified source facts, history extents, supports
and manifest completeness remain G26's responsibility. A query requiring a missing
or invalid generation fails explicitly; it does not silently omit that collection.
Queries without provenance predicates use the independently complete member view.

Admission authorization policy is separately bound (§6). Its historical tag
selection uses the pinned tag view; current permission checks use current state.
The directory may retain descriptive plaintext already permitted in the server
catalog, never payload bytes or new provider plaintext custody. Scratch containing
sensitive evidence stays within the existing protected host boundary, with scoped
access, cleanup and configured at-rest protection where required by its owner.

### 3.2 Private ordered map

Use a persistent binary radix map over the existing `sequence63` collection-ID
domain, in numeric bit order. Let `w=63` for that existing codec; it is not a new
collection-count limit. A branch has depth plus at most two immutable child
references; a leaf has collection ID and version reference. Empty children are
null. Root/children are content-addressed and exact immutable-byte equality is
checked on digest reuse. This map remains private; do not reuse the public tag-set
codec for differently typed values or change the tag/archive digest domains.
Existing tag copy-on-write code is implementation precedent, not a new owner.

A single collection replacement/deletion path-copies at most w branches and one
leaf. Unchanged subtrees are shared. Ordered traversal keeps at most w+1 directory
frontier entries and visits each reached node once; sparse uncompressed paths
cost up to O(w*C), not a universal 2C bound. The consecutive-key probe's ~2C+w
counts are fixture-specific. A B-tree lookup for stored node IDs still costs
O(log Z) per node; do not call total SQL machine work constant or strict linear.
Adding unrelated mutations does not add work to an already pinned directory walk.

Concrete private records (names are implementation suggestions):

```
discovery_head(singleton PK, incarnation, directory_root, publication_counter)
discovery_node(node_id PK, kind, depth, left_id, right_id, version_id,
               canonical_node_bytes, direct_refs, allocation_bytes)
discovery_collection_version(version_id PK, fixed_descriptor_fields...)
discovery_source_handle(handle_id PK, owner_kind, immutable_identity,
                        complete, retention_refs, storage_bytes, phase)
discovery_reap(object_kind, object_id, PK(object_kind,object_id))
```

Large source data use G26's indexed facts and bounded segment manifests, not
`canonical_node_bytes`. Metadata for a handle is fixed-size; its many descendant
records are retained/reaped by the owning bounded manifest mechanism. Directory
and query tables are catalog-owned derived/operational state, never archive proof.

### 3.3 Transactional writer coverage and lock order

Prepare/seal variable-size description/tag/member/G26 source versions in bounded
steps first. Then, in the SAME short transaction as the relevant catalog change,
validate their current archive/root/owner fences, replace the affected descriptor,
path-copy its directory entry, and switch `discovery_head`. An acknowledged
searchable publication must not leave a silently stale directory. No async mirror
that claims it includes a committed catalog mutation before it actually does.

All writers to searchable state take locks in this global order:
`discovery_head -> collection owners in numeric order -> index-state/summary
owners -> immutable handle/reference records in deterministic order`. Acquire
head coordination before changing contributors, not in a cross-table AFTER
trigger after taking conflicting locks. Reconcile the existing catalog-event
watermark lock order too; no path may take event/head locks in reverse order.
Root pin admission takes head then query/pin records; it takes no source-row locks.
Every mutation of directory references (including GC and resurrection) takes the
same short head coordination lock before object/handle rows. This deliberately
simple serialization prevents parent/child lock inversion; it is part of the
measured write/maintenance cost. Reapers never take collection/event locks or
acquire head after child locks. Mark a source handle REAPING under that coordinator;
then its owner deletes descendants in separate bounded units without reacquiring
head/collection locks. A REAPING handle cannot be newly published or pinned.
SQLite uses the existing serialized write discipline; PostgreSQL uses explicit
owner locks. Use bounded lock/statement wait policy, without raising existing
safety timeouts merely to make a test pass.

Update every actual mutation owner: collection publication/import/rebuild/deletion,
member inventory sealing, description and tag publication, G26
`publish_index_build` and invalidation, search-normalization/extraction changes,
source reset, bootstrap, bulk and cascade paths. The existing catalog-event
watermark alone misses G26 publication and is insufficient. Fail invariant tests
on a new uncovered mutation. No raw live UPDATE of a sealed view is permitted.

A small one-collection commit writes at most a fixed path plus fixed metadata and
references. Stage large changes outside that transaction. Do not silently split
an existing atomic multi-collection contract: use its bounded publication manifest
or seek specific owning approval. The head serializes final publication, not whole
preparation/query execution. Declare and qualify this short write bottleneck; if
it becomes material, use an explicitly proved bulk/path-copy strategy rather than
unbounded transactions or a global lock around source staging.

### 3.4 Atomic pin, reuse and maintenance

In one short transaction, authenticate admission, read and retain the complete
head root, bind its incarnation and query owner, reserve configured query resources,
and create the idempotent work record. Admission reads a fixed number of header/
quota records; it does NOT enumerate C collection headers or pin C generations
individually. The root retains its descendant source handles transitively.

Use DAG reference retention: newly interned nodes own their fixed child edges once;
current-head/query/staging roots own explicit external references. Removing a root
queues zero-reference objects, not a recursive DELETE CASCADE. Reaping pops at most
a configured number/byte budget, locks and rechecks zero references, deletes the
object and releases its fixed outgoing edges. Resurrection and cleanup race under
the same locks. Source-handle reapers visit bounded pages; a generation row cannot
cascade-delete millions of facts. Restrictive FKs and phased owner deletion prevent
`CollectionRecord` removal from bypassing query retention. Deletion still removes
current visibility/permission immediately; retained bytes grant no access.

A pin may retain old data for collections later excluded by selection. That cost
is real: account exact global allocated/retained bytes and operational pin ages.
Do not compute an expensive exact per-query unique-byte attribution at admission;
label any per-pin logical reservation conservative, and keep it distinct from
physical shared-byte totals. Avoid multiplying shared objects by query count.
Enforce configured global/per-owner admission and retention budgets at allocation,
not only at final sealing. Under pressure, explicitly expire/defer query work and
reap bounded state before it obstructs durable custody; do not silently drop a pin
or invalidate archive facts. No infinite lease, hidden hard total or free cleanup.

## 4. Computation and G27 owner optimizations

Retain the non-null composite seek and tested disjoint-prefix fallback,
transactional summary deltas, exact one-pass clause matching, minimum-support
propagation, same-member guards and keyed intersections from ALGORITHMS.md.
The following conditions replace their earlier memory/transaction assumptions.

### Bounded data operators

`load_source_chunk(handle, key_or_ordinal, byte_limit, record_limit)` validates the
query/pin/attempt, reads a bounded detached chunk, then closes its transaction and
connection. Evaluate outside it. A high-degree node carries an adjacency cursor;
processing one node cannot mean loading all its edges. Disk-backed queues, dedup
sets, joins and sort runs persist their own paged state. Checkpoint root references
and cursors, not full queues/sets/plans. A physical access-plan test must show a
chunk doesn't scan the whole source before returning a small result.

Large assertions, profile-section sets, scalar values, UTF-8 strings, descriptions
and supports are independently varied extents. Extract once into source-normal
segmented facts. Do not fetch one unbounded ORM Text/BLOB or call `json.loads` on
an entire unbounded assertion at query time. Stream literal containment with a
bounded-pattern algorithm such as KMP, preserving ASCII-only folding, exact
codepoint/byte semantics and chunk-boundary matches. Equality includes exact
length/representation and streamed verification after any digest candidate check.
Keep first native posting/first fully satisfied declared section; persist winner
vectors by section when they exceed a quantum. The old in-memory trie/matcher is
a semantic oracle/helper, not the proof of this streaming implementation.

Simple ordered member/index lookups may return pages before scanning the whole
input, but only with proof that no preceding result or smaller support is unseen.
For ordinary complex provenance queries, choose resumable complete preparation:
selection -> fact-match -> scope-project -> clause-intersect -> order/seal.
Persist per-operator frontiers and counts. A pending operation is not an empty
page. Emit complete results only after all relevant terminal obligations finish.
No mandatory full-result build for the simple plan and no opportunistic early
results from an unqualified complex plan.

Factor immutable G26 evidence once per query/profile. Preserve exact
`bound-and-required-history` versus `complete-retained-history`, accepted inherited
transfer extents, and the independent meaning of State byte extent. Retained-only
history is not member attribution; imported late claims require both historical
inclusion and causal applicability for the SAME member. Different clauses may
have distinct witnesses. All minimum-support/order checks complete before sealing.
No restriction of existing literals/scopes or expansion of archive authority.

### Summary and mutable-page work

Page collection keys before enrichment. Maintain totals, revision-and-identity
coverage groups, and ordered manifest digest counts by old/new transactional
contributions, exactly as ALGORITHMS.md specifies. Use the lock order in §3.3.
Bounded group lookups must preserve zero-retained, missing/ambiguous-root and
corruption behavior. Rebuilding summaries uses their actual operational source
records; archives alone cannot replace receipts/jobs/unpublished state.

For `/search`'s unindexed literal or residual filter, select one logical page's
private immutable input view. Scan bounded source ranges, use a bounded top-p+1
heap for the requested total ordering where no matching ordered index exists,
and persist continuation between quanta. Retain only the necessary candidates;
comparisons/byte work still count. Return the native page only when its next p+1
results are exact. The following page is a fresh mutable-page view. Its complete
browse sequence may legitimately pay repeated scans; declare that trade-off and
prefer exact discovery for a stable full traversal. Do not call trigram or LIKE
queries universally indexed/output-linear. Keep native normalization semantics.

## 5. Durable work, idempotency, HTTP and completion

### 5.1 Private execution interface and state

```
admit(query, principal, invocation_key) -> query_id, retained_view, work_token
claim_quantum(query_id, worker) -> attempt_epoch, checkpoint_version
load_chunk(...) -> detached bounded input
execute_quantum(detached_input, checkpoint) -> bounded staged delta
commit_quantum(query_id, epoch, expected_checkpoint, delta_manifest) -> phase
read_page(query_id, position, limits, live_principal) -> exact page | pending
cancel(query_id, live_principal) -> bounded cancellation acknowledgement
reap_query(query_id, quantum_limits) -> remaining | reclaimed
```

Persistent query metadata includes owner/key, logical request digest, admission
policy/scope identity, source incarnation, retained root/pin, extraction and
algorithm revisions, phase, checkpoint version, worker epoch/lease, idle expiry,
resource reservations/usage and terminal manifests. Result/support rows have
(query, attempt, exact member/clause/support keys). Queues, captured selections,
operator state, runs and merge frontiers live in paged tables/segments. No unbounded
inline manifest, status JSON, IN list, serialized resume token or mutable result
rewritten once per output. Bind support evidence to exact original source facts.

Durable work phases: `ADMITTED -> SELECT -> MATCH -> PROJECT -> INTERSECT -> SEAL ->
READY`; simple ordered plans may instead use `STREAM`. `CAPACITY_BLOCKED` retains
its checkpoint and suspended phase; `FAILED/CANCELLED/EXPIRED -> REAP -> RECLAIMED`
are explicit terminal paths. Phase choice doesn't turn a preparation milestone
into result completeness. Missing required source is a failure, not zero matches.

Each quantum first obtains a bounded worker lease, reads its fixed checkpoint,
loads bounded input, releases source resources and computes outside transactions.
It writes bounded staged outputs, then commits their manifest and checkpoint using
compare-and-swap on `(query_id, attempt_epoch, checkpoint_version)`. Duplicate
identical delta replay is a no-op; conflicting reuse fails. An expired/replaced
worker cannot publish or delete its successor's state. Checkpoints advance only
with durable associated output; orphan stages are bounded-reaped. Cancellation
is checked between small units and again at publication. A process crash repeats
at most unfinished quanta, not all earlier source work. Large sort/graph operations
must expose real substeps; naming one whole sort a quantum is forbidden.

SEAL hashes ordered bounded result/support segments and validates terminal counts
incrementally. Its final transaction compares the stored terminal manifest,
checkpoint/epoch, pin validity and owner/source identity, then changes READY with
a fixed write set. It does not rescan/hash Q rows while holding the transaction.
Do NOT require equality with the current directory root or global content counter.
New content is not a query failure. Corruption, expired retention, changed source
incarnation or current authorization are different, explicitly failing cases.

Retained sealed segments are one result store; avoid a compulsory second complete
copy into SQL. SQL tables may themselves be the staged/result store. Account any
necessary copy rather than hiding it. Reclamation is part of lifecycle work even
when scheduled after delivery; record both result-complete and resources-released.

### 5.2 Concrete public progression (current-only pre-v1 revision)

Use the existing `/v1/artifacts/discover` owner, not a second query language.
Retain `ArtifactDiscoveryRequest` as the logical predicate document. Introduce an
invocation envelope containing `query` and an `idempotency_key` using the existing
canonical-visible-text/max-200 convention at its owning declaration. The key is
invocation transport identity, not part of `query.identity()`; the client creates
it once and persists it through admission retries. Unique owner/key/admission-key
records reject a different logical query. An identical retry resumes the original
view, never silently starts at a newer root. Retain replay records for the declared
operator-configured idempotency window; do not claim indefinite replay after that
window or unlimited tombstone storage. Signed continuation expiry remains binding.

`POST /v1/artifacts/discover` accepts that envelope and either no token (admission)
or the owner-typed continuation/page token. Each invocation drives at most one
configured quantum or joins existing progress; it does not synchronously wait for
all preparation. No long HTTP poll or DB session is needed. The server may also
schedule the same leased quanta fairly; correctness/progress must not depend on
an undocumented separate daemon or on the client resubmitting the initial query.

* `202`: a closed `riverhog-artifact-discovery-pending/v1` response with query ID,
  logical query identity, opaque snapshot/read identities, phase, bounded work
  token and retained-until time; no artifacts and no `complete:true`. Advertise
  retry delay using the existing HTTP convention. No growing progress array.
* `200`: `riverhog-artifact-discovery-page/v2`, same exact hit/support semantics,
  query/read identities, query ID and retention expiry, bounded artifacts,
  complete flag and next page token. Use fixed-shape support references as below,
  rather than forcing an unbounded source pointer/value into one hit. Empty terminal success requires full proof;
  a pending no-match-so-far state is never this response.
* `DELETE /v1/artifacts/discover/queries/{query_id}`: owner-authenticated idempotent
  cancellation. It fences work/releases the root reference in bounded metadata
  work and acknowledges cancellation; cleanup proceeds in bounded quanta, not a
  giant cascading delete. It is neither archive deletion nor permission grant.

A logical page retry is position-idempotent; pending-work tokens resume server
checkpoints and are not client-controlled scan positions. Tokens bind operation,
owner/key, query ID/digest, pin/source/read identity, page position if any, and
expiry, with existing cryptographic token discipline and fixed size. Worker
attempts stay server-side so takeover doesn't invalidate valid client tokens.
Reject retired token purposes/formats; never reinterpret them as a new snapshot.

Use `riverhog-discovery-read/v2` over an explicitly specified current canonical
header: logical query identity, existing catalog incarnation, opaque retained-view
identity, admission authorization/scope binding and extraction identity. The
opaque view identity is a server-MAC binding of query ID, incarnation, private
root and relevant identity domains. Do not expose a globally correlatable raw
catalog root or a forgeable pin. A query retry has the same read identity; a fresh
query may have a different identity even for equal logical results. This is stated
publicly, not confused with archive or result-set equivalence. Initial hashing
is fixed-header work, not an unbounded selected-header scan.

Expose the equivalent pending-work envelope to `/search` only for its long logical
page preparation, keeping successful page contents/next mutable position as §2
specifies. Native client low-level calls expose pending distinctly. Convenience
iterators persist invocation/work/page tokens, honor retry/backpressure, permit
cancellation and bound local memory; they never restart admission on a 202 or
accumulate all hits. CLI traversal writes progressive results and distinguishes
pending, failed and terminal states. Source-bound OpenAPI/extent/client tests must
cover the additional outcome and cancellation route; no silent surface exclusion.


### Variable-size supports: bounded transport, unchanged evidence meaning

The current `DiscoverySupportOut.pointer` is a plain string, not an established
small carrier bound. A bounded number of hits therefore does not prove bounded
response bytes. In the new page/v2, each clause/value match is a fixed-shape
`DiscoverySupportReference` with support identity, exact byte count, media/format
identity and a bounded first-record-set reference. The referenced current support
document preserves the exact native fields: journal anchor, entry, assertion and
referent identity, kind, state, relationship, pointer, representation and value
digest. No field is dropped, shortened or interpreted as merely advisory.

Use the repository's owned bounded record-set/chunk/terminal mechanisms for this
query-owned projection, with exact byte and completion commitments. A concrete
`GET /v1/artifacts/discover/queries/{query_id}/supports/{support_id}` reads a bounded
chunk page by signed ordinal/token and byte limit. Native 63-bit sequence/carrier
bounds retain their existing owning meaning; do not add a maximum total support
size. Validate that the support belongs to a hit currently authorized for this
query before revealing it; a support shared by several members needs an exact
bound member context, not 'anyone with the query ID'. Scope its token to that
member and check current provenance access on every chunk. Keep the source pin
and support segments until the invocation's declared retention ends.

The client exposes streamed support content and may offer explicitly materialized
convenience reads whose output-sized memory is honestly declared. A convenience
iterator claiming bounded memory must not automatically assemble an unbounded
pointer/support document. This transport revision is explicit additional contract
work, not preservation of the old inline wire schema. Extent witnesses include
one single very large support, not just many ordinary supports. The compact query
literal maxima remain unchanged; they are not maxima on source metadata.

### 5.3 Expiry, admission and fairness

Use validated RuntimeConfig fields for work record/byte budget, memory, active
quanta, statement/lock wait, scratch and retained-source bytes, worker lease,
idle retention, query admission counts, idempotency retention and cleanup budgets.
Choose documented defaults from qualification, not invented throughput promises.
Exact values are operator capacities, not query/member/history semantic maxima.
A source statement must have an interruptible bounded plan; timeouts are safeguards
and do not prove that LIMIT bounds examined work.

Worker leases are short and renewable per committed progress; query retention is
separate. A progressing query can renew retention under the declared capacity
policy without a hidden hard total-duration limit. Idle/explicitly pressure-expired
queries return a documented expired-view result (recommend HTTP 410); loss of an
unexpired required pin fails closed, never retargets to latest. Check auth before
revealing whether another owner's query exists. Capacity admission failure uses
explicit 429/503 as owned by the existing error framework; suspended capacity work
is reported separately from executable pending work, with a checkpoint-preserving
resume path. Do not poll busy or automatically raise limits.

An active pin remains retained on ordinary cache eviction. Dropping a page cache
can reload sealed segments. Destroying operator checkpoints is not ordinary cache
eviction: either retain/recover the exact checkpoint under its contract or report
explicit invalidation. Do not advertise bounded crash replay while discarding its
only durable progress state. Archive-only recovery may discard all derived query
state; it never relies on a live query to recover custody or meaning.

Fair scheduling admits bounded numbers of quanta per owner and rotates runnable
queries; waiting for I/O does not monopolize a worker/DB connection. A finite
admitted pinned computation with sufficient retained resources, continuing worker
service and stable authority can finish while arbitrary *content* updates occur.
No promise is made under endless revocation, storage loss or capacity denial.
Measure per-owner progress as well as aggregate throughput.

## 6. Authorization and semantic invariants

Pin content, NEVER current permission. Every call authenticates the owner/key and
checks current account permission, claim/capability/fence, expiry and revocation.
An optional null `authorization_view_identity` is not a cache-reuse proof. Bind the
server's exact effective grant/config revision or a canonical identity derived by
its actual authority owner, plus immutable artifact-selection identity. Do not
create a weaker second authorization interpreter or hash an unbounded access list
on every page. Mutation of the underlying authority must change its owned binding.

Admission's policy applied to pinned tags describes the initial selected domain.
New content or new tag-granted access does not enlarge an old query. On delivery,
use bounded indexed current collection/tag/capability checks for each returned hit
(and any lookahead information that would disclose access). A deleted or currently
forbidden member causes an explicit whole-page access failure; NEVER omit it and
claim the snapshot result is complete. A changed effective principal/claim binding
invalidates the invocation. Match/support provenance exposure additionally requires
current provenance permission. Recheck at the page's authorization/serialization
boundary; no promise to claw back bytes already lawfully delivered before revocation.

Differentiate content mutation from permission-affecting tag mutation: the former
doesn't restart computation; the latter can prevent delivery. Do not require a
whole-catalog global authorization epoch to equal its initial value when bounded
current owner checks suffice, nor allow old tags to override a current denial.
The model uses deliberately simplified authority strings and a current-hit check;
it does not prove production tag/grant/HTTP integration.

The following must remain exact across both history extents and every scope:
member and payload identities (including distinct members with equal bytes),
selected prefixes, retractions/forks, imports and inherited extent, sparse and
truly dense lawful relationships, first profile/posting witnesses, minimum
supports, order, pagination and complete terminal results. Root/handle digests
verify derived storage integrity, not archive proof or source-binding authority.
Independent recovery preserves artifacts, description, tags and selected provenance
without the directory, query service or database. A query pin never extends a
custody/claim lease or authorizes source release or archive retention changes.


### Correctness argument the integration must preserve

Directory induction: the empty root is complete; one validated atomic descriptor
replacement path-copies only that key and leaves every other child unchanged.
The publication transaction commits the source pointer and new directory root
together. Thus a successfully pinned root names one coherent committed view,
not a mixture obtained by consulting mutable rows later. All queried children are
immutable and transitively retained, so independent short loads have the same
meaning as reading that view together. Ordinary later root changes are irrelevant.

Retention induction: each stored edge owns one child reference; each live pin/head
owns one root reference. Reaping deletes only a zero-reference object after the
shared coordination check, and releases only its own outgoing references. No
object reachable from a live root can be reclaimed. Staged handles are separately
rooted until publication or explicit abandonment. The argument depends on complete
writer/GC coverage, not merely an in-memory pointer being present.

Execution induction: a committed checkpoint names durable operator inputs and
outputs; CAS prevents a stale epoch/version from advancing it. A lost quantum
leaves that checkpoint unchanged. Operators run deterministic exact semantics
on the same retained facts; terminal output follows only after every requisite
operator frontier is exhausted. Bounded replay and no skipped capacity-blocked
work follow. Native G26 scope/witness equivalence is a separate obligation and
is not proved by these state-machine arguments.

## 7. Proposed complexity and extent declarations

Bind declarations beside actual operation owners through existing Closure/Audit
and #963 machinery; this file is NOT another runtime inventory/target authority.
Use `w` for the owning fixed key width, `C` pinned directory entries, `D` reached
private nodes, `F/B` unique relevant source facts/bytes, `V/E` lawful graph nodes/
edges, `d` query-clause/program dimensions, `Q/Bout` exact result cardinality/bytes,
`K` committed work quanta, `P` output pages, `U` content publications, `Z` indexed
store size and `L` replayed uncommitted units. Count variable-key comparisons and
all lower-layer bytes; fixed-size SHA/reference assumptions are explicit.

| Operation | Work and storage obligation |
|---|---|
| Admit and pin | Fixed header/retention/quota operations plus actual auth delegate; O(log Z) indexes, no C scan. One retained root, not C per-query generation pins. |
| Directory replacement | O(w) node reads/new immutable nodes and indexed/reference updates per bounded descriptor change; O(U*w) retained mutation allocation before GC, charged to writes. Global short publication serialization is measured. |
| Directory walk | O(D), D <= O(w*C), plus node-ID index costs and streamed descriptor bytes; O(w) frontier memory. Fixed consecutive-key tests do not establish a tighter general bound. |
| Indexed G27/simple G28 pages | O(log Z+p+page bytes) only for proven full-key seeks and bounded current auth. Prefix fallback explicitly charges ordering width. No full-preparation mandate. |
| Residual mutable page | Declared source-domain scan plus predicates and O(N log(p+1)) bounded top-p ordering if required, in resumable quanta. Repeated mutable pages repeat this cost; no whole-traversal linearity claim. |
| Literal matching | Unique fact/posting traversal plus segmented B bytes, exact program/pointer/state terms. Query-sized KMP state; unbounded sections/winners spill. Extraction/validation delegate costs remain explicit. |
| Unary provenance | Per-clause unique typed nodes/edges/facts plus priority/index costs and emitted clause-members. Persist adjacency/frontiers; no member-by-shared-history expansion. |
| Same-member guarded profile | Sum of member-specific necessary candidate slices, conservatively Cguard*(V+E) before indexes, plus literal cost. Sparse zero-result adversaries required; NOT universally linear. |
| Intersect/order/seal | Keyed/merge intersection, necessary external ordering O(Q log Q) comparisons with byte costs, O(Q*d) support links and output-sized disk. Every run/merge/seal checkpoint bounded. |
| Complex query lifecycle | Once-per-retained-view preparation at the selected profile, + K bounded checkpoint overhead + page seeks/Bout + L bounded replays + actual retention/cleanup. Content publications do not multiply completed query work. |
| Maintenance | Each unreachable immutable node/edge and result segment released once with actual index work; bounded transactions, no recursive cascade or O(total) final cleanup step. |

Per-step RAM, transaction rows/bytes, tokens and checkpoints are independent of
logical total, apart from explicitly owned representational/key/query constants.
A quantum may require O(log Z) index work: "bounded" does not pretend an index
probe takes constant time on all database sizes. Complexity bounds for declarations
include writer/pin/maintenance amplification and cold data, not only warm readers.
Output-sized disk is acceptable when explicit; mandatory O(Q) RAM is not.
No actual default capacity numbers or v1 conformance are established by model runs.

## 8. Integration map, current baseline and acceptance

Implement in this order; do not mark dependent composite declarations conforming
before their production witnesses exist.

1. Reconcile exact integrated source and issue-owned approvals. The snapshot/read/
   pending and bounded-work *semantic direction* was expressly approved in #964
   comment 6118482544; freeze the exact protocol fields, canonical identities,
   extent/error vectors and tests at the actual owners. The reference remains
   non-authoritative and is not evidence of runtime conformance.
2. Integrate seek/matcher helpers and delta-summary invariants (ALGORITHMS.md),
   with source-bound differential tests. Extend current `v1_0001` DDL/models for
   directory/handles, query state, indexed lookup and summaries. Preserve Alembic,
   explicit bootstrap and synthetic future forward-revision/interruption tests;
   no old-pre-v1 conversion, dual reader or automatic destructive state reset.
3. Wire directory publication at the actual source owners, especially
   `canonical_discovery_index.publish_index_build`, collection publication,
   descriptions/tags, deletions and `catalog_db` reconstruction. Update restrictive
   generation retention/FKs and shared lock ordering, including catalog events.
   Old roots remain usable while leased; current deletion/access remains effective.
4. Add catalog-owned `discovery_views` and `canonical_discovery_query` internals
   (or equivalent focused names), bounded operators/queues/run manifests and query
   lifecycle models. Adapt `canonical_discovery_search` to retained sources, never
   live-table fallback. G26 source-normal segments and guarded scope compiler feed
   these operators. Keep server implementation out of reusable/client packages.
5. Own envelope/read/pending/expiry/cancel definitions in `riverhog-protocol`,
   API search schemas/router and existing HTTP contract declaration path. Adapt
   `riverhog-client/client.py:discover_artifacts`, its tests, CLI consumers and
   actual reachable callsites discovered by Closure. Bind `/search` pending work
   separately from its mutable next-page contract. Do not hand-edit generated
   contract expectations as authorization for the semantics change.
6. Bind positive/negative/growth/restart/resource witnesses at each owning surface;
   generate exact scope/evidence through Closure/Audit/#963 and consume it in #469.
   No duplicate inventory, target registry or detailed policy ledger in AGENTS.md.
   Keep #961/#963 ownership and #960's report-only 600-second profile/disposition.
7. Run ordinary locked format/type/lint/unit/dist/build and required local portable
   Linux/image/recovery/lifecycle/security proof, then independent exact-SHA GitHub
   checks. Retain interruptions/failures and timing dispositions honestly. No
   provider/release operations are authorized just to measure these changes.

Mandatory adversaries include: continued content commits during every work unit;
quiescent equivalent view comparison; pinned-generation replacement and deletion;
root-pin versus GC races; actual PG row-lock ordering/deadlocks; SQLite WAL progress
and PG old-version reclamation; source reset; null auth identity; key/claim expiry,
capability scope changes and tag-based access loss; restart at every operator and
seal boundary; stale/duplicate workers; chunks crossing large-string/byte/pointer
boundaries; high-degree/deep graphs; both inherited history extents/all scopes;
no-match completion; capacity change/resume; idle expiry; large cleanup queues;
first/deep/terminal pages and slow consumers. Test two backends and real API/client
behavior, not only an abstract state machine or a synthetic counter threshold.

## 9. Evidence and remaining work

[bounded/model.py](bounded/model.py), [bounded/test_model.py](bounded/test_model.py)
and [bounded/probe.py](bounded/probe.py) exercise a real SQLite WAL model of
immutable radix roots, constant-header admission, no open transaction during
computation, bounded CAS work checkpoints, content changes without restart,
capacity suspension, source/current-authority failure, cancellation and bounded
reference cleanup. The independent streaming KMP controls cover chunk boundaries.
These are synthetic model witnesses, not production G26 queries or public contracts.

[bounded/evidence.json](bounded/evidence.json) records exact executed tests,
commands/source hashes and gaps; [bounded/counters.json](bounded/counters.json)
records fixture counts. Earlier 53 helper tests/12 native tests in evidence.json
remain historical results from the parent handoff, not rerun or combined passes.
No production directory integration, streaming G26 compiler, summary mutation
coverage, actual new HTTP/client/CLI contract, PostgreSQL concurrency, full locked
qualification or timing target is claimed complete. This revision supplies a
selected algorithm/lifecycle policy and testable integration instructions, not
an assurance that unexecuted integration is already correct.
