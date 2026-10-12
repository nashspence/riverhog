# G26: segmented source facts, resumable sealing, retained generations

Recommended replacement design for #964/#903. Audited source:
`152d095038df0c0d0ed642cf265d47b11c65a754`, tree
`991e994457b487a7dca016b7fc0409485cef25c6`. Reference parent:
`4621be70fe2229ef7de169862ff1ec5274dec00f`.

Earlier recommendations are calibration. Retain the exact factored relation in
[SEMANTICS.md](SEMANTICS.md); replace the old whole-assertion staging, complete
hash-pass replay allowance, competing query workspace and current-generation
continuation rules. This is concrete integration input, not implemented production
or a claim of complete growth conformance. The new compact commitment is
**explicitly approved** by [#964 decision 6118482544](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118482544)
as a distinct derived identity; exact implementation encodings/golden vectors
and integrated correctness remain subject to owning review.

## 1. Decision and boundaries

Build one source-normal immutable generation from the exact union of selected
verified archive facts. Every phase, including a single large assertion, long
value, high-degree dependency, sealing and reclamation, advances through bounded
physical work. Preserve all four scopes, both history-request extents, independent
State byte extent, native matching and minimum support. No primary-only shortcut,
implicit corpus-wide widening, merged effective graph or same-byte attribution.

G26 owns authenticated source extraction, compact index representation, build
completeness, publication and generation retention. G22 supplies neutral exact
journal validation; G23/G24 own corpus/history validity. G27/G28 consumes retained
immutable generations and owns invocation admission, pending/continuation, current
authorization, result checkpoints and support transport. Do not implement another
query lifecycle here. Mutable browse and exact discovery remain different owners.

Ordinary content updates do not revoke a pinned immutable generation. Current
authorization, deletion and retention expiry still govern whether it may be served.
Pins are not grants and cannot authorize source release or bypass archive authority.
No long catalog read/write transaction, held source iterator, global-content fence
retry or whole-generation cleanup is the default implementation.

## 2. Source qualification and normal form

Use SEMANTICS.md's raw assertion birth/retraction intervals, exact snapshot headers,
shared history/root/import/dependency sets and member bindings. One assertion's
lexical facts are extracted once for the admitted source union, not once per member
or prefix. Match effective/retracted status with exact intervals, not latest-head
state. Forks and foreign references retain their exact original preimages.

Qualify journal keys by the verified source admission and complete chain identity.
Within one admitted corpus, shared journal IDs require the native shared-prefix
consistency proof. Never deduplicate across sources merely by UUID, payload hash,
path or apparent similarity. Identical bytes may have distinct member/record identities.
Shared validation does not cache current access decisions. G22 source facts are
usable only after exact validation and source-terminal/fence evidence; speculative
topology or a partial captured stream cannot be published as a discovery source.

Use a fixed current normal form at canonical_discovery_extraction/rows owners:

- source, entry, assertion, interval, exact snapshot, member/history and typed
  relationship headers have bounded identifiers or references to segmented values;
- variable canonical JSON, text, byte values, pointer tokens, native profile names
  and reference preimages use ordered byte segments plus terminal length/count;
- path nodes store parent identity and one exact escaped-token stream. Reuse the
  declared source path prefixes instead of copying a deep pointer into every
  posting. Posting rows reference path nodes and native ordinal/type/value streams;
- complete declared profile sections and canonical edges are individually enumerated,
  not arrays hidden in an assertion-row payload. Opaque profile lookalikes remain
  ordinary lexical data, never graph edges or declared sections;
- fixed scalar/index keys retain actual type and collation. Unbounded values are
  not SQL index keys or giant IN lists. A digest accelerator must verify exact
  bytes where equality matters and must not become permission or source identity.

Stable path/field ordinals come from the native canonical traversal order, not
arrival order or random IDs. Ordering JSON keys has an explicit canonicalization
cost. The maintained parser may materialize one permitted entry under its existing
entry-byte contract; that does NOT permit a whole journal, all postings, all root
sets or a state-sized checkpoint. Deep traversal uses an explicit spillable stack,
not Python recursion proportional to an undeclared extent. New larger-entry profiles
need their own owner-reviewed parser bounds. No capability is silently restricted.

Preserve native first-posting and first-complete-section semantics. Do not regenerate
text/bytes value hashes from a preview, encoded substitute or truncated pointer.
G27/G28's exact pointer predicate walks token identities; only requested support
needs full pointer reconstruction, streamed through its support transport. Count
source-token/value bytes and requested pointer bytes separately. Repeatedly expanding
all pointer prefixes is avoidable internal amplification, not necessary input.

## 3. Large assertions are segmented work, not invalid data

At the audited source, stage_assertion_page rejects >20,000 postings in one
assertion, >50,000 postings in a page and >10,000 text chunks per posting/page;
IndexedAssertion also materializes postings/profiles/edges as tuples. Merely raising
those constants does not fix extent. The generic JSON grammar can contain >20,000
scalars below the entry-byte limit; full enclosing profile/journal validity is a
separate obligation, not established by that generic-value diagnostic alone.

Replace the private all-in-one IndexedAssertion staging carrier with:

`begin_assertion(source_key, assertion_key, origin, canonical_stream_ref)`
`advance_assertion(source_key, assertion_key, extraction_cursor, budget)`
`stage_fact_segment(build, task, segment_key, exact_bytes, expected_version)`
`finish_assertion(build, task, source_terminal)`

The extraction cursor includes source byte/token position, category and logical
ordinal, declared-section position, pending value/path cursor and stream hash state
or fixed-fragment progress. Large cursor stacks live in paged work relations, not
one JSON checkpoint field. Do not resume by reparsing the entire assertion and
skipping every earlier posting; an admitted bounded entry parse replay must be
explicitly bounded/charged, and completed posting generation is not repeated.

Each segment has source/build/worker bindings, kind, logical record key, ordinal,
byte length and integrity identity. Retries compare exact existing data and terminal
metadata; an identical retry advances no counters, and a changed retry fails.
Reject gaps, conflicting final markers, tails after terminal and changed sources.
Segment order within a logical stream is contiguous; independent streams may stage
in any order. Empty values/posting sets have explicit terminals and do not vanish.

Row/byte/work budgets limit ONE call. Exhaustion yields resumable progress, or an
explicit operational admission/capacity result, not permanent semantic invalidity
of an otherwise valid assertion. Oversized rows/strings must be split before SQL
binding; a DB string-size exception is not the intended large-value protocol.
Treat memory, disk, active jobs and lease duration as source-linked operational
policy, with distinct capacity/pending/cancelled/expired outcomes. Do not infer
support ceilings from a benchmark size or test's deliberately tiny segment size.

The native G22 8 MiB default entry limit remains separately owned; this recommendation
does not remove it. G26 also does not require a new unlimited single-entry format.
Existing journal/member/history logical totals still progress over bounded entries
and structural record sets without derived-index-only cardinality maxima.

## 4. One durable build engine for upload and archive-only rebuild

Begin under existing pending collection/root/provenance/epoch fences. Capture the
exact immutable source and independently authenticated expected member/root/import
manifests. Admission and source-work registration are idempotent. Both upload and
archive-only rebuild use the same engine and extraction policy, never prior search
rows as the source of truth.

The phases are MEMBER_SCAN, SOURCE_DISCOVERY, SOURCE_VALIDATE, FACT_EXTRACT,
REFERENCE_VERIFY, CANONICAL_ORDER, COMMIT_LEAVES, COMMIT_FOLD, READY_TO_PUBLISH,
PUBLISHED, with explicit blocked/failed/cancelled/retired cleanup states. Source
registration/validation/extraction may interleave where dependencies permit.
Every phase has a saved key/byte/adjacency cursor and bounded substeps; phase names
alone are not evidence that work is bounded.

Register child obligations in the SAME transaction that advances the parent's
source cursor. Use exact task identities and an indexed outstanding-work queue;
retrying task registration does not increment outstanding counts. Mark each terminal
only after its source coverage, child obligations, actual row/byte totals and exact
retry evidence agree. Preserve independent archive/member-binding completeness,
G22 entry/anchor validation and both selected history extents. A count of whatever
the extractor inserted cannot prove the extractor did not omit required content.
Semantic source coverage and derived-row enumeration are separate checks.

Resolve large dependency/root/import sets once with a disk-backed deduplicated
worklist. Keep high-degree adjacency continuation separate from the node's visited
bit: a node is not complete while its outgoing cursor is unfinished. Native cycle,
foreign-anchor and same-member source-binding proofs remain required. Retain exact
bound-versus-documentary membership, not merely the union of observed journal names.
G23/G24 delegate costs and unresolved qualifications block composite acceptance;
G26's own bounded loop cannot certify an unbounded delegate.

Load source/database chunks under short operations, release cursors and borrowed
connections, then evaluate/encode/hash outside them. Commit bounded rows plus the
next task checkpoint under the same epoch/version CAS. Durable source/segment files
are synchronized before their DB references. Restore owned work across process
replacement; do not attach job restart to a process temp directory's survival.
Ordinary restart replays only unfinished work units. Real loss/corruption of retained
state has an explicit rebuild/failure disposition and cannot be reported as warm resume.

Native validated summaries already have JournalStore.close support. Consume them
through G22's explicit handle lifetime, publish exact source facts, and release
connections before moving to the next source. Do not hold a complete summary for
each member or every exact prefix. Reused immutable projections remain subordinate
to archive evidence and current reads/fences. A pin does not override revocation.

Freeze pending fact input only when independent source/semantic obligations are
complete. Enforce immutable state during canonical ordering and sealing. Concurrent
new facts belong to a different attempt, not the read transaction of this one.
External sort/spool uses bounded runs and a persisted merge frontier; neither one
SQL ORDER BY over an arbitrary unsorted total nor all runs in one checkpoint is
an automatically bounded step. Prefer producer-assigned canonical keys and verified
ordered scans where available. Charge sort comparisons, key bytes and index work.

## 5. Deterministic resumable compact commitment

### Selected identity refinement

Select a domain-separated ordered Merkle commitment to the source-normal fact
stream, rather than a flat SHA state whose loss restarts the entire compact pass.
Use ordinary SHA-256 on bounded leaves/nodes; no custom crypto context codec is
required for G26. G22 MUST still preserve its existing flat journal/prefix SHA-256.
These are different commitments at different owners, not interchangeable shortcuts.

This is the **approved** refinement of G26's previously proposed flat compact
transcript ([#964 decision 6118482544](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118482544)).
Use the selected `riverhog-canonical-discovery-factor-merkle/v1` domain and a
distinct extraction-policy identity describing the normal form, typed field
encoding, fragment rule and tree algorithm. The earlier G26 hard-cut approval
alone did not cover this refinement; the new issue approval specifically does.
Freeze/check exact canonical encodings and golden vectors through owner review;
never hash a changed preimage under an old policy/dataset identity.
No archived member, journal, history, provenance-root or payload digest changes.

### Normal form and exact tree

Table/record order is fixed by the extractor's schema: the source-normal relations
in SEMANTICS.md, including path/value segments and terminal records, sorted by their
explicit source-qualified canonical keys. Never include attempt IDs, SQL row IDs,
queue order, cache state, timestamps, worker epoch or operational page sizes.
Logical records retain types and exact canonical scalar encodings. Large keys are
represented by canonical source ordinals/segmented tokens, not truncated names.

For each logical record, stream its deterministic encoding into fixed 4,096-byte
fragments, with an explicit record terminal committing total bytes and fragment
count. This is an approved identity-format representation constant, NOT an operational
quota or semantic total maximum. Empty records have a terminal; no missing tail is silently
accepted. Operational reads/writes may subdivide or group fragments without changing
the identity. A fragment-in-progress has a bounded accumulator and exact offset.

Let B(x) be ASCII decimal length, ':' and the exact bytes x. A data-leaf preimage is
B('data') || B(table code) || B(canonical source/record key) || B(fragment ordinal)
|| B(fragment bytes). The terminal-leaf preimage is B('end') || B(table code)
|| B(record key) || B(total canonical bytes) || B(fragment count). Table codes,
keys and integers have one frozen canonical encoding, not SQL repr/host endianness.

For ordered leaf payloads d[0..n), use SHA256(empty) for n=0, SHA256(0x00 || d)
for a single leaf, and SHA256(0x01 || left || right) at each internal node with
the largest-power-of-two split below n (RFC 9162's tree shape). Domain-separated
leaf/node hashing and ordered keys make boundaries, order, duplicates and empty
content meaningful. Never use XOR, unordered sums or transport-batch hashes.

The outer dataset identity is SHA256(native canonical JSON of a fixed-shape header
containing the NEW format/extraction identity, independently verified source-manifest
identity, logical-record count, leaf count and Merkle root). Existing generation
identity binds that dataset to the exact archive root, provenance and core contract.
Generation identity changes deliberately; a rebuild under the same source/policy
must reproduce it byte-for-byte regardless of scheduling, source arrival, worker
budgets, segment packing, retries or process restart.

### Bounded carry, fold and finalization

Persist the next canonical record/fragment key and leaf count, a single pending
carry (digest and level), and a frontier relation keyed by tree level. One step
hashes bounded fragment bytes outside the catalog transaction or combines a bounded
number of pairs, then commits its checkpoint. A carry through O(log n) levels is
itself resumable; it is not hidden inside an indivisible append. The top-level
checkpoint references the frontier, not an unbounded in-memory array. Store exact
record terminals and totals through the same replay-safe transitions.

At end of the canonical source stream, fold occupied levels from smallest to
largest: combine each higher-level subtree on the LEFT of the accumulated right
suffix. Checkpoint every bounded fold. Final publication reads the completed root
and fixed terminal header; it does not rehash the dataset or run per-snapshot COUNT
queries under its lock. Building the tree is O(encoded fact bytes + leaves) hashes
in total, plus explicit ordered-index access; frontier state O(log leaves) lives
on disk. Tree shape and final header are independent of work budgets.

A valid root is not source authenticity: source validation/completeness must finish
before it is publishable. A lost checkpoint cannot be replaced with a caller-asserted
root. Ordinary crashes resume the committed leaf/carry/fold position. Actual durable
loss requires an explicit integrity failure/rebuild and is not the ordinary path.
The model uses shorter fixture fragments and a diagnostic leaf codec; it tests the
algorithm, not the final production dataset encoding or approved freeze vectors.

## 6. Publication, retained generations and incremental reclamation

Prepare and validate all large source/index/descriptor data before publication.
In the short existing _require_pending/publication fence, verify attempt epoch,
archive/provenance identity, source/semantic terminals, completed commitment and
current authorization/deletion conditions. Publish active_build_id AND the new
searchable-version descriptor/directory root through G27/G28's common coordinator
in the SAME transaction. Follow its head -> collection -> index -> reference lock
order. Include publish_index_build, not just catalog-event writers. Failure leaves
the previous applicable generation intact; no READY marker for a partial build.

Published fact generations are immutable and addressable by exact content identity.
A retained query pins G27/G28's directory root, which transitively retains these
source generations and segments. A new active pointer does not delete or mutate
an older pinned generation. Multiple immutable generations in the ONE current
schema are retention, not abandoned-pre-v1 compatibility. Do not retain a legacy
extractor, dual interpretation or alternate authority. Identical deterministic
completed generations may share verified data; build-attempt identity remains distinct.

G26 supplies a narrow generation-source accessor returning bounded ordered detached
facts and source-support references. It does NOT create query workspaces or rebuild
an expired query itself. G27/G28 owns invocation expiry/new admission, live authority,
current-content versus pinned-content semantics and exact support chunk delivery.
Source revocation/deletion blocks serving as its authority requires; a retained old
root does not restore access. Avoid a global current-root equality test on each
query or at query READY: ordinary unrelated writes must not restart exact work.

Reclamation uses shared pin/ref coordination: a generation cannot enter deleting
while any retained head/query/staging root still reaches it. Once atomically marked
deleting with zero roots, no new pin is admitted. Reclaim source/fact/path/value/
frontier/segment references in bounded batches with saved cursors, then remove the
header. Disable or restructure generation-wide ON DELETE CASCADE and large DROP/
TRUNCATE shortcuts. Shared segments are removed only after their last reference;
row/object identity and epoch fencing prevent ABA/double decrement. Cancellation
releases only its own root and schedules incremental cleanup. Existing deletion,
privacy and custody obligations still control retention and response visibility.

Measure actual unique/shared physical bytes, in-flight allocations, orphan segments,
old generations and database maintenance. Refcounts/lease limits are not free and
may need explicit operational backpressure. Archive-backed derived index rebuild
cannot discard receipts, claims or other non-rebuildable operational state. Pins
must not turn optional derived state into a new source-release prerequisite.

## 7. Costs, witnesses and integration order

Measure unique admitted source H, authored assertions A, explicit incidences D,
member bindings N, exact snapshot headers S, path/token/value bytes and canonical
fact fragments K separately. Build/source visits follow H+A+D+N+S plus declared
G22/G23/G24, schema/canonicalization, index and ordering costs. Segmentation adds
linear carrier/terminal overhead, not N*shared-A or S*journal-A. Pointer-prefix
sharing avoids an internal depth-by-posting product. Requested expanded support
or a genuinely dense relation has separately declared output cost.

Keep G27/G28's unary/shared minimum-support and same-member guarded-query profiles.
Guarded candidate slices may be superlinear; do not label them output-linear.
Memory follows admitted source/entry/fragment windows and bounded operator state.
Disk includes facts, work, current and retained generations, logs and GC backlog.
For a retained correct source/attempt, ordinary F crashes add F unfinished quanta,
not F whole-source/whole-generation passes. No finite completion promise is inferred
under unlimited faults, revocation, storage exhaustion or arbitrary delegate work.

Implement in this order:
1. Reconcile exact post-#963 source; apply the approved Merkle commitment
   direction, then freeze its exact normal form, implementation vectors and
   session/handle and operational-policy bindings at their owning contracts.
2. canonical_discovery_rows/extraction: resumable source/path/value/profile/edge
   streams; new private staging carriers and direct-source witnesses above every
   prior threshold. Keep test-only native logical oracle, not a legacy runtime.
3. canonical_discovery_source/build/rebuild: one source union and G22 session
   consumption, durable obligation/progress state and exact source terminals.
4. canonical_discovery_index plus a private commitment module: immutable ordering,
   persisted leaf/carry/fold pipeline; constant-size seal/publication checks.
5. catalog_provenance_index_models, source mutation owners and G27/G28 directory:
   atomic source publication, retained generation references and bounded GC.
6. canonical_discovery_relevance/search: G26 exact source accessors into G27/G28,
   no old current-generation cursor/workspace interpretation; retained-view API
   changes and transport remain explicitly owned by G27/G28.
7. Edit the existing v1_0001/v1_ddl/models/fixtures in place. Preserve Alembic,
   StateSchema, explicit bootstrap and synthetic future forward-upgrade/interruption
   tests; no migration or compatibility with discarded pre-v1 application layouts.
8. Register extent/growth/authority evidence through existing Closure/Audit/#963
   machinery and run the normal locked/local/built/remote qualification rail.

Required production witnesses: valid native large-profile assertion with >20,000
postings; long text and deep paths with small work budgets; exact both-history-extent
and all-scope/query/support parity, retractions/imported guards/forks; keyspace and
zero-result adversaries; per-phase process kill and state corruption; exact digest
across scheduling/budgets/rebuild; independent source completeness negatives;
publication/query pin/revocation/GC races and PostgreSQL plans; no unbounded final
hash/count/delete; source and summary handle release; full archive-only recovery.
Existing G26 model/native evidence is historical, not evidence these new production
paths have run. See ../g22_g26/README.md and evidence.json for this revision's actual
executed diagnostics. Numerical defaults, end-to-end conformance and target timing
remain subjects of their required owner evidence, not invented passes.
