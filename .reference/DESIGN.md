# Shared primary facts across member closure turns

This is a finite executable design for
[#964's primary-member request](https://github.com/nashspence/riverhog/issues/964#issuecomment-6870475991),
extended by the [focused lifecycle request](https://github.com/nashspence/riverhog/issues/964#issuecomment-6871280583)
and audited at `f35123c4dcb79de171627257c72633b9d72378f0`. The accepted nonprimary-prefix
and terminal imported-primary work stays separate. This reference changes the
member preparation boundary, not the outer finalizer architecture.

## Source and lifetime boundary

The source problem is `_validate_provenance_closure`: each member opens the same
primary prefix, runs Native validation, then walks producer and selected graph
facts. With F members sharing B bytes, merely changing the byte accessor would
leave repeated semantic and producer-preimage work. The fixture varies fanout,
producer record extent and unrelated Occurrence/observation documentary text
independently, so none of those repeated paths can hide behind another axis.

The proposed owner is `UploadPrimaryFactsOwner`, held by the collection upload
service. One admitted index belongs to one original collection construction and
write epoch. It survives changes of the workflow claim. The actual scheduler
creates a new claim for each progressing turn, and `ArchiveCatalogFinalizer`
represents one original turn, so neither is an adequate cross-turn cache key.

| Object | Lifetime and fact | What it does not establish |
|---|---|---|
| Current catalog finalizer | Original claim, construction, epoch, cursor, result and commit receiving | Permission for another turn |
| Fresh sealed admission | Exact source/capture/job/progress/outcome/catalog/workspace identity under current row locks | Archive or member acceptance |
| Fresh Native result opening | Independently owned read-only accepted source with its actual workspace lease | A historical final graph or lasting permission |
| Primary preparation index | Detached source facts and exact historical intervals within one construction/epoch attempt | Terminal corpus acceptance |
| Producer/context memo | Completed checks for an exact effective selected context revision and accepted obligation | Permission to use another source or skip member checks |
| Member cursor | Provisional progress after all current member checks | Successful closure or root publication |

Every lifecycle/preparation/member call still enters the existing catalog finalizer.
The finalizer checks the current claim and write gate and preserves its original
preimage/postimage/result. In source-bearing preparation and member turns, the
primary owner obtains fresh sealed metadata,
compares the entire admitted identity with its pinned source, and opens a fresh
accepted Native result using `UploadJournalResultResources`. The exact selected
prefix must equal `anchor_at` in every coordinate. A final exact source/job probe
precedes successful preparation or member return. No old SQLAlchemy Session,
permit, copied final graph or terminal imported-history corpus is reused.

Each Native opening is received through its original resource frame before the
turn returns. This deliberately makes cold handle reopening ordinary behavior:
there are no warm Native handles whose eviction can trigger source replay. The
cross-turn semantic state consists of the private disk projection and application
memo metadata. Their original construction scopes and exclusive namespace lease
remain with the live owner until receiving completes.
Missing accepted workspaces, changed catalog identity or changed job metadata
fail explicitly; they do not silently trigger new Native validation.

## Bounded source and producer preparation

The primary owner creates a **separate** `CanonicalCorpusValidator` instance and
enables its optional historical selector projection before inserting a source.
It uses that instance's source-index primitives only. It never borrows the raw
terminal corpus and never calls corpus acceptance to grant primary authority.

Source indexing advances at most one accepted Native frame per call. Each frame
already has Native's entry-size admission. The complete frame is read once per
distinct exact source in this owner attempt, including later corrections needed
to identify historical assertion lifetimes. The caller receives `None` while
preparation progresses; the catalog turn may report work but leaves the member
cursor unchanged. No archive-wide member or journal limit is introduced.

The source identity is pinned before the first frame. Native source checkpoints
and application memo checkpoints use separate admitted SQLite files. The Native
source extent is therefore the reconciliation point after a lost application
memo acknowledgement; the next call checks exact overlap and seeks that extent.
It does not trust the memo cursor as evidence that a frame exists, or replay
already indexed frames because a second database reply was lost.

`PrimaryProductionWork` replaces the whole-context Python lists and fragment
sort in the old per-member producer call. Its phases are:

1. Ingest one selected producer, manifest or fragment extension into indexed
   private relations, checking the exact contract/profile identity.
2. Check each producer source-context identity against its exact manifest.
3. Check each fragment-record key has a manifest, using keyset seeks.
4. Walk manifests and their fragments in numeric offset order. Each call handles
   one fragment or one relation/phase transition, carrying a SHA-256 continuation.
5. Mark the context prepared only after all counts, byte lengths, contiguous
   offsets, canonical base64, fragment limits and complete digests agree.

The algorithm preserves duplicate-manifest rejection, equality of source-context
sets, empty/missing record rejection and exact fragments. Retrying an original
intake uses its immutable assertion identity and does not duplicate a record.
Producer preimages, fragment lists and the full selected graph never accumulate
in Python. Large valid records require more bounded calls and disk storage.

Context memos use the Native change-version vector of the exact selected
producer/requirement keys, not only a particular prefix digest. Two different
accepted anchors with unchanged relevant facts can share the completed checks.
Insertion or retraction of a relevant assertion changes that vector. Changes to
unrelated documentary assertions do not cause another producer hash pass.
Different effective producer revisions remain explicit selected work, not a
claim that arbitrary correction histories cost nothing.

## Historical selection and narrow semantic work

The source index preserves assertion birth and retirement. A selected object is
the latest assertion born at or before the selected sequence, provided the
sequence is before its retirement. A retired gap is absence; the query must not
fall back to an earlier version. Original assertion identity remains attached
to every selected fact. The exact prefix header is checked independently.

The optional selector adds indexed keys for extension subject/property and
journal-subject artifact/role. Assertion lifetimes use disjoint dyadic interval
nodes. Point selection seeks only the nodes containing that position and merges
a bounded frontier of fixed metadata. It does not scan all retired assertions,
unrelated subjects or zero-result graph rows. Every returned page closes its
cursors and carries only a detached continuation key.

The fixed interval domain is **derived from existing accepted Native source
bounds**, not a new `sequence63` public contract. `CheckpointSHA256` already
rejects a source beyond `(2**64 - 1) // 8` bytes. Native frames are contiguous and
each encoded frame contains at least RS and LF. Thus a successfully accepted
source cannot contain `2**60` frames; the private `2**63` interval domain is a
strict superset. The test exercises the existing continuation limit and this
implication without allocating a huge source. History's public sequence grammar
continues to be nonnegative integers.

Selected objects are narrow Native facts, explicitly not complete assertions:

| Kind | Retained member fields |
|---|---|
| Delivery association | Identity, assertion, context, role, slot, State and verification observation |
| State | Identity, assertion and Occurrence |
| Observation | Identity, assertion, State, size, SHA-256 and direct-measurement existence |
| Occurrence | Identity, assertion, artifact and exact materialization hint, when present |
| Extension | Identity, assertion, subject, property and exact profile value |
| Journal subject | Identity, assertion, journal, artifact and role |

Opaque notes, identifiers and unrelated evidence descriptions are consumed once
while preparing the source and do not enter every member's lookups. Required
hint and profile value bytes remain selected data and are counted; the design
does not disguise those required outputs as constant-sized metadata.

`verify_selected_member_binding` contains the same member checks as the original
`verify_member_binding`; the existing summary-based entry delegates to it and
retains independent byte verification for its reader callers. The new primary
caller supplies indexed existence of the same journal/member-history predicate.

Completion context validation checks exact uniqueness, profile and accepted
obligation once per context revision. Its memo retains only the accepted
execution identity or explicit absence. Each selected State's output is checked
against its own effective output revision; a later correction cannot borrow an
old output memo. No full context declaration is decoded again for every member.

## Member acceptance and rejection timing

Every member still checks its exact artifact ID, size, digest, delivery context,
association, role, slot, local State, direct observation and Occurrence. The caller
still verifies the final descriptor bytes against their binding, equality with
the accepted early primary, complete selected root/import record sets, and every
nonprimary selected root through its existing fresh-prefix proof.

The finalizer still checks the exact materialization hint or the explicit
omission decision, and writes the completion output and member cursor only
after those checks. A producer, completion, historical-selection or member
failure cannot advance that member's cursor. Error discovery may occur in a
preparation turn or in a different order among checks for that same member;
preparation progress is not successful member progress.

Two changes in detection timing are explicit:

- A raw payload alteration that leaves admitted sealed metadata unchanged may
  allow provisional member progress using already accepted Native facts. The
  unchanged complete **current raw** terminal barrier rejects it before
  `provenance_closure_validated` or root publication. This reference does not
  turn a prior successful Native validation into current raw-byte acceptance.
- The separate primary source index enforces existing documentary identity
  consistency while combining its sources. A cross-source immutable identity
  collision can therefore be rejected during primary preparation rather than
  waiting for the terminal corpus. The unchanged terminal barrier already
  rejects that collision; no formerly valid collection is excluded.

Generic provider freshness and current-read fences are unchanged. Terminal
imported-history source proofs, full input State checks and bound/retained extent
rules remain with their existing owner.

## Cost model and evidence

For one live construction/epoch attempt, let D be the distinct exact admitted
sources, B the sum of their complete accepted frame bytes, H their assertion and
correction atoms, C the distinct effective selected producer-context revisions,
R the producer records/fragments required by those revisions, and F the members.
Let M include required selected member/hint/output metadata, and S the selected
history-set work already owned by the caller. These dimensions are independent.

| Work | Intended accounting |
|---|---|
| Raw catalog primary bytes during member/preparation turns | Zero |
| Native semantic validation advances in those turns | Zero |
| Accepted source frames/bytes decoded and indexed | Once per distinct source: B, with frame/hash/index work observed |
| Optional lifetime projection | H times a bounded derived interval width, with ordinary B-tree index costs |
| Producer metadata and preimages | Once per distinct relevant selected context revision: R |
| Fresh admission and cold Native opening | One bounded metadata/opening unit per preparation/member call; fully charged |
| Member semantic selection | Indexed selected facts and required M, including sparse negative results |
| Current final descriptor/history-set checks | Existing exact caller work S, retained and counted separately |
| Raw terminal barrier | Existing complete raw/corpus/history work, outside this improvement |
| Cleanup | Indexed application discovery, exact exclusive custody, one admitted file extent or phase per worker turn, and actual original receiving; no source replay |

This is not a claim of constant-time SQLite I/O. B-tree depth, metadata widths,
source-index expansion, SHA work, capacity failures, reopening, actual receiving
and elapsed time remain visible. The source declarations describe bounded
operations; the real-caller witness independently checks aggregate frame bytes,
producer visits/hashes and member visits against the actual distinct sources.
Counters are observed at source/Native/SQL/delegate boundaries, not inferred from
the final result or merely copied from the owner's counters.

The complete result is roughly source preparation plus selected context work,
indexed current metadata work and required member/history outputs. It has no
F-times-complete-source or F-times-opaque-documentary term for shared primaries.
Changing selected context revisions can create additional selected work, which
must be reported rather than absorbed into a supposed universal linear claim.

## Capacity, receiving, retirement and restart

Each active primary construction holds two admitted SQLite connections: one
Native source index and one application checkpoint/memo database. The number of
active owners is bounded by existing retained-construction configuration, and
the existing storage connection/cursor/operation limits remain effective.
Cold Native result opening needs its own headroom. An empty failed index is
received before its slot is returned; a prepared nonempty index is not silently
evicted to make another request appear successful. Busy/full is explicit pending
work, not a partial-as-complete result or an archive cardinality limit.

A capacity refusal before acquiring an owner, or at a first empty construction
whose exact resources have all received, completes an unchanged-cursor catalog
scheduling turn. The caller returns `True` to keep the provenance stage pending
for a fresh turn; in the existing pipeline `False` would mean the stage is done.
This is a received scheduling yield, not claimed member or preparation progress.
It lets the registry release the waiting construction's reservation so a
collection already holding prepared facts can finish. The real registry witness
checks both a single retained-construction slot and actual SQLite saturation.
An uncertain original close or a failure after source admission cannot take
this deferral path and retains the original receiver.

The actual partial constructions, connections, Native results and leases remain
reachable through their original owners. An uncertain close retains its grant
until that same original confirms receiving. Closing one facade cannot certify
an independent reader's return. The real catalog finalizer still handles lost
commit acknowledgements from its captured before/after/result, including the
case where preparation made progress but the member cursor did not change.

Forward completion retires primary preparation before entering the unchanged raw
terminal pass. Service shutdown joins existing finalizer work and then receives
primary preparation before closing storage admission. The terminal-retirement
hook runs in the existing terminal worker only after current authority, committed
terminal decision and original forward receiving have been established. It
receives derived preparation before reporting local execution stopped.

## Registered lifecycle and crash recovery

`PrimaryFactsJobRecord` belongs to the Riverhog application catalog. Its immutable
allocation binds the exact workspace ID, original collection/construction/epoch,
process owner, and private root identity. The namespace pin records the actual
directory and lease identities. A version and irreversible phase fence every
mutation. No foreign key can cascade this disposal ownership away with upload
or content rows. The unique collection index permits one unfinished allocation,
including retired namespaces, so repeated capacity refusals cannot accumulate
unbounded disposable directories for one collection.

Both indexes live in **one** registered flat namespace. Native owns only
`corpus.sqlite3` and its `-journal`, `-wal`, and `-shm` files. The application owns
`checks.sqlite3` and the same sidecars. The application owns the exclusive
`.lease`, the directory, and final catalog receiving. Neither source metadata nor
archive authority moves into this lifecycle record.

The real catalog caller separates three turns:

| Turn | Committed prerequisite | Permitted effects and result |
|---|---|---|
| Allocate | Current upload construction and epoch | Register exact workspace/root identity; return pending with no namespace or database effect |
| Pin | Freshly read committed allocation | Create/acquire the exact namespace and exclusive lease, synchronize it, record actual directory/lease identities; return pending with neither database created |
| Prepare | Freshly read committed namespace pin | Exclusively create the two private databases through their retained construction scopes, then perform the existing bounded accepted-fact preparation |

The owner remembers a weak identity of the metadata turn's transaction only to
exclude another call inside that same uncommitted transaction. It retains no
Session or transaction permission. A fresh read determines whether an uncertain
allocation/pin commit happened. A pin rollback reuses the still-held original
namespace; it cannot recreate the databases or mistake an in-memory successor
for a committed pin. A failed construction retains its actual partial original
until receiving, then retires its registered allocation. No `mkdtemp`, recursive
deletion, pathname prefix search, or directory enumeration participates here.

On restart, a new process does not open either old index as accepted facts.
`UploadPrimaryFactsRetirement` discovers rows with indexed state/workspace seeks,
including bounded wraparound. The process token is only a candidate hint. The
physical original checks the current complete catalog fence, the actual retained
root and pinned namespace, and acquires the original exclusive kernel lease.
A live owner, uncertain close, replaced root/directory/lease, or stale catalog
identity cannot authorize disposal. A registered creation prefix without a
committed namespace pin permits only empty-prefix receiving: every exact owned
data name must be absent. Unknown children remain untouched and prevent final
`rmdir`.

Retirement commits before any byte mutation. Each physical turn advances at most one phase and removes at most one admitted
extent from one exact named file.
The phases are `corpus`, `memo`, and `directory`, with separate committed
transitions. The Native corpus facade and neutral file primitive receive the
held directory identity; a replacement selected between the application's check
and the primitive's open is rejected before any file mutation. No SQLite parser,
Native validation, source payload accessor, or whole-source replay runs during
reclamation. Data-sidecar removal and the final directory removal are synchronized.

The directory phase commits before lease unlink and `rmdir`. A restart after
lease unlink still requires the pinned directory identity; a restart after
`rmdir` proves synchronized absence under the pinned root. The catalog record is
forgotten only after final namespace receiving and all original native handles
and file-reclamation resources have actually closed. A lost acknowledgement or
unconfirmed close retains the same original and its resource charge for receiving;
it does not repeat an uncertain worker mutation.

The existing `WorkspaceRetirementRegistry` supplies bounded ordinary retention,
terminal physical execution, original worker joining, and close retry. Receiving
runs before ordinary archive maintenance, so a received empty capacity refusal
can release the sole construction slot and allow its cleanup to progress. New
cleanup admission alternates before and after actual finalization selection; a
long retired extent cannot repeatedly capture that sole slot ahead of independent uploads. Busy
candidates advance only volatile seek hints; independent eligible namespaces can
continue. Shutdown stops dispatch, joins workers, and receives their actual
originals before storage dependencies close. It leaves unfinished durable records
for restart instead of draining an arbitrarily large archive synchronously.

The schema addition is a deliberate pre-v1 baseline replacement with both checked-in
dialects and the exact PostgreSQL fixture updated. Startup still validates its
expected schema; this reference adds no implicit upgrade or erasure path. Rebuilding
derived facts after cleanup is charged to a new attempt. Ordinary per-turn Native
result reopening does not discard the prepared index and causes no replay.

## Remaining integration boundaries

This branch is an executable design, not delivered production qualification.
The integration owner must reconcile it with current source and decide what to
adopt, reproduce or reject, recording a finite disposition in #964.

The focused lifecycle extension exercises actual killed processes at registration,
partial namespace/index construction, extent mutation, and final namespace
receiving boundaries. Source-owned catalog, physical and caller observations
remain separate from elapsed-time measurements; [VERIFY.md](VERIFY.md) identifies
the exact small suites and evidence collection. This qualifies the stated finite
reference path on its tested platform, not an entire delivered deployment.

The synchronous outer terminal transaction and its reconstruction after an
interrupted attempt remain separate recorded limits. No broad finalizer redesign,
archive-format change, relaxed encryption, revised
contract gate, whole-path G13 closure or downstream release acceptance is claimed.
Backend, delivered/built service and independent exact-final-source GitHub checks
remain with normal integration. #991 retains attribution and actual parallelism
investigation. Aggressive 1,000 / 10,000 / 100,000 campaigns remain held.
