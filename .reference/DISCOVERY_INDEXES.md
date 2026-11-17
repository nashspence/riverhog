# Discovery, derived indexes and read-only verification

This is proposed implementation input for [#999](https://github.com/nashspence/riverhog/issues/999),
under the non-authoritative [#903 reference workflow](https://github.com/nashspence/riverhog/issues/903).
Product source audited and tested here is
`c28eceaa2dfec1b3712135469ba12230e74f5ffa`. The reference contains no product patch and does not
claim integrated checkpoint economics, installed recovery or power-loss qualification.

The selected change is to keep derived discovery work private, retain the exact original
admission and resource owners, and expose only a complete index through the existing finalizer.
Read-only verification uses one fully verified operation-local descriptor index. It creates no
optional recovery store. Long rebuilds use the one composed #995 budget and #996 generation
mechanism; they do not acquire an independent allowance per journal, member or index table.

## 1. Authority and the audited owners

The governing obligations are [AGENTS.md][agents], [README.md][readme], #994/#999 and the existing
archive readability contract. [#958's growth addendum][958-growth] requires one complete
descriptor verification before indexed use, selective lookup rather than repeated whole-index
scans, complete receiving, and separate accounting for RAM, live scratch, cumulative writes and
output. [The #971 coordination decision][958-coordination] leaves #964 as the shared traversal
implementation lead, #958 responsible for representation/readability, #546 for installed
independence, #442 for provider terminal proof and #509 for principal continuity. This reference
does not introduce an archive format, a new authority registry or a #969 feature commission.

| Actual owner at the pinned source | Audited behavior | Selected disposition |
| --- | --- | --- |
| [`CanonicalProvenanceArchiveReader`][reader] and [`ProvenanceMetadataIndex`][metadata-index] | `prepared()` scans the complete metadata/root/terminal sequence before making indexed reads available. The private SQLite index has selective journal/sequence and kind/sequence indexes. `seal()` commits once and makes that connection query-only. | Keep complete verification, selectors and original receiving. Remove only the unnecessary final data commit of this disposable index; keep its original private transaction query-only until close. It is never a resume authority. |
| [`DiscoveryMetadata`][discovery-metadata] | Retained `discovery.sqlite3` uses DELETE/FULL and directory/file synchronization, checks original filesystem custody and current worker, and commits each derived work transaction. It mixes exact opening/header bindings with replayable descriptor, selection, framing, rank and corpus progress. | Preserve mandatory original identity/custody separately. Allocate a new disposable live file for derived work under the current schema cut; do not turn synchronization off on the old retained file. Adopt an optional complete generation only through the shared policy. |
| [`DiscoveryValidationOwner`][validation-owner] and [`DiscoveryJob`][corpus-jobs] | Warm slots serialize work; original Core/Native handles and the workspace lease survive cooperative turns. Failed close retains the slot. Job state binds original build, workspace, attempt, archive root, policy/catalog and frozen source/selection manifests. | Reuse this owner and its currentness/receiving protocol. Advance private progress across turns without adopting it after every yield. Keep original accepted inputs and resource liabilities mandatory. |
| [`rebuild_canonical_index`][rebuild] | Server-owned archive maintenance uses `attribution=None`, a prepared/cached reader, exact member-history closure and Native validation. It stages a whole member per catalog transaction, then checks/hashes the complete dataset under a writer. | Prepare in bounded private work; install bounded required rows; consume the #997 prepared completion/CAS seam. Preserve server maintenance attribution and store allowances. No new application grant or HTTP rebuild route. |
| [`_ArchiveCorpus`][rebuild] | The two-entry cache currently removes its oldest summary before `close()` is confirmed. A lost close result leaves that original outside the cache. | Retain up to two usable summaries plus one unresolved eviction. Block further admission while receiving is unresolved; remove only after the exact original reports `storage_received`. |
| [`canonical_discovery_index`][index] and [generation models][index-models] | Pending build IDs/epochs fence workers; complete identity includes the exact deterministic dataset hash and extraction contract. Complete generations reject further staging. | Preserve encoding and semantic extraction. Use #997's content revision, immutable prepared completion and short final acceptance; do not implement a second finalizer. |
| [`SqlAlchemySearchService.discover`][search-service], [`discover_artifacts`][search] and [`SqlAlchemyCanonicalProvenanceService`][provenance-service] | Search runs in a read snapshot. Page identity binds source, selected collection/index generations, metadata revisions and authorization view. Missing required index state fails explicitly. Provenance disclosure rechecks current principal and scope. | Evidence-backed no change to query semantics, current authorization, cursor fencing or missing-versus-empty behavior. Reads never execute rebuild, checkpoint, provider recovery or cleanup. |

The current read index already avoids a commit per descriptor. Its single final commit is the
narrow measured optimization below; the result is not evidence that all current discovery
persistence is redundant. Durable index rows, accepted exact source selections and original
resource/close records have different consumers.

## 2. Concrete execution and persistence cut

### Admission and the private executor

Keep `begin_index_build`, the exact original `DiscoveryJob` identity, source/storage incarnation,
frozen source and selection manifests, policy/catalog identities, construction receipts and
kernel custody. A worker claim continues to fence publication and resource use; it is not an
optional checkpoint. Missing or corrupt mandatory source, manifest, custody or opening outcome
is an error, even if some derived SQLite file remains readable.

Refactor the synchronous `rebuild_canonical_index` wrapper to drive bounded quanta through the
same discovery execution owner. Preserve the existing explicit `riverhog-api index rebuild
--collection … --json` command and its result shape; the CLI is an executor, not a status poll.
The public discovery and canonical-provenance read routes remain read-only. The wrapper obtains
the existing server maintenance admission and keeps the selected archive reader/source custody
owned through preparation, acceptance and final receiving. No application key is charged or
invented for this server-owned read; store allowances still apply as they do today.

Create a new live derived file under the admitted owner namespace, with a distinct worker
incarnation and original construction record. Use the #996 disposable SQLite configuration and
bounded execution discipline on this new file only. Never reopen an old retained
`discovery.sqlite3` and reinterpret its data as disposable. The durable job keeps mandatory
identity/resource records; descriptor indexes, selector/rank/framing/corpus cursors, partial
hashes and private extraction rows live in the disposable file. Source-consistent private
transactions still provide rollback and warm logical consistency; they do not promise crash
recovery. Progress objects report live positions separately from a selected recovery frontier.

Use `discovery-working-<file_id>.sqlite3`, `discovery-generations.sqlite3` and
`discovery-capture-<file_id>.sqlite3` in that existing protected workspace. Each `file_id` is
32 lowercase hexadecimal characters allocated once and bound to the original worker/candidate
construction before opening; a retry reuses its original registration, not a new random name.
Register the working/capture SQLite sidecars and the retained DELETE journal explicitly.
Update `journal_validation_workspace.py`'s exact filename validation and discovery reclamation
phases together. A syntactically matching name without its original ownership record is not
permission to open, adopt or remove it. Optional generation/capture files are created only
after the shared grant; ordinary working-file admission remains the original resource boundary.

Freeze exact source and selection manifests through the existing corpus input machinery, then
validate their shared Native source/corpus state once under #996's live owner. Member relevance
and output extraction consume these exact frozen views in bounded pages. Reuse a verified
prefix by its full journal/anchor/source/configuration identity; a later or shorter prefix is
not interchangeable merely because the journal ID matches. Do not create an independent full
corpus validation or checkpoint budget for each member. Necessary member/history edges and
output associations remain attributable work; sharing is not permission to omit them.

The existing [`stage_relevant_member`][build] call cannot remain an arbitrarily large writer
transaction. Split its prepared output into the existing bounded stage operations using stable
keys and exact duplicate comparisons. Install the member-complete association only when its
full prepared output extent has been installed and verified. The maximum member ID, a top-level
row count or the last callback is not proof that preceding output is complete.

### Optional complete recovery generations

Use #995's ordered admission unchanged: cheap completion and mandatory boundaries win;
restart mode and zero optional count suppress optional capture; unknown estimates/accounting
do not become zero. The default remains adaptive, 120 seconds of net saved replay, fraction
1/8, unlimited optional count and two usable retained generations. Include source reread,
Native replay, full restore/revalidation and capture/selection/GC cost in the actual candidate.
Waiting for a provider or the next scheduling turn creates no new completed useful work.

A discovery capture is one shared root grant. Quiesce the serialized worker at an eligible
safe boundary. Each participating owner produces a complete child descriptor: Core's private
discovery state uses the #996 page-map mechanism as a Core-owned child; Native captures only
its Native-owned tables/control state. Keep the file/construction/connection admission and
original receiving of each child with that owner. A paged, immutable dependency manifest binds
the exact Core state, selected source/selection manifests, Native child descriptors and live
phase. The selected root stores the manifest digest/count, not an unbounded descriptor list.
Do not select a Core cursor newer than its corresponding Native state or restore a collection
of independently newest files.

Reuse the #996 separate retained store and authenticated fixed-depth page map. The initial
admitted capture visits the complete live state once; subsequent captures inspect only the
bounded WAL interval and changed pages, with exact truncation tombstones and complete manifests.
Selected images remain physically distinct from mutable/discardable working files. The retained
store keeps its fixed DELETE/EXTRA durability policy; no duty-cycle PRAGMA toggling. Preserve
#996's finite tracking envelope and explicit loss-of-tracking suppression instead of silently
copying the entire growing database again or accumulating unlimited WAL.

Ready child manifests are not selection. The current owner transaction selects only the exact
complete dependency closure, settles the original grant/accounting and retains the original
receivers through ambiguous replies. Read back the original grant/descriptor after a lost reply;
neither a newer local filename nor a wall-clock timestamp chooses recovery. Maintain the two
usable generations plus admitted candidate/reader pins under the shared capacity rules. Bounded
reference-count reclamation receives all dependencies before physical removal. A selected root,
unreceived candidate or live reader cannot be deleted to make a new optional checkpoint fit.

On cold restart, verify the selected envelope, full dependency closure, exact source/history,
algorithm/schema, worker/source authority and restored Native semantics before advancement.
Missing optional state causes replay from the same accepted archive inputs. A corrupt latest
generation may use the last usable accepted generation under #996's fallback rules, with new
promotion suppressed when tracking continuity is lost. No current permission is cached in a
generation. Required source corruption remains fatal.

### Complete output and its cache

Completion freezes the live derived/Native view and records the exact mandatory semantic
outcome. It does not force a final optional snapshot. The later missing/corrupt derived-result
case uses #996's narrow `DerivedNativeCacheUnavailable`; it schedules/coalesces bounded
revalidation under the exact outcome/source/history/configuration key, then lends independently
owned frozen readers. It does not turn broad runtime/database/custody errors into successful
fallback, nor replay a whole journal separately for each member query.

An index remains derived. Rebuilding or evicting its cache must leave archived journal bytes,
recording IDs/times, assertion and retraction history, member-history descriptors, sealed roots,
custody receipts and publication identity byte-for-byte unchanged. Read-only verification still
verifies complete selected authority rather than trusting cache existence.

## 3. One index completion and publication seam

Consume the [#997 prepared-finalization contract][997-finalization] directly. Preserve its exact
dataset encoding and `PreparedIndexCompletion` bindings: build ID/worker epoch, construction,
accepted input revision, immutable source-binding digest, archive generation/root/provenance
identity, core/extraction contracts, captured content revision, validated counts, dataset hash
and resulting generation ID. Rebuild uses the same seam for a published collection; it replaces
only the complete derived-index selector. It does not republish archive history or replace the
immutable receipt for the original publication.

Every mutation of any of the nine dataset tables participates in `content_revision` in the same
transaction, including profile/value/text/edge changes, bulk cleanup, replacement and cascades.
Build IDs and versions never recycle. Capture fixed starting revisions; check those same
revisions and fetch each bounded keyset page in one read snapshot. A later revision invalidates
the whole private candidate. Complete per-snapshot entry/assertion checks and the full
deterministic dataset hash remain required; equal row counts cannot certify equal contents.

Acceptance uses fresh current rows under the existing collection/index owner locks. Compare
the fixed source binding, source/storage incarnation, immutable root, build/worker fence,
accepted input revision, content revision and exact completion result. Recheck current deletion,
terminal and authority gates using the existing maintenance/admission owner. Cached principal
permission or a previously valid reader is not the final currentness check. Keep the original
selected source/admission resources pinned until the candidate outcome is known. If it is
stale, discard the derived candidate while preserving sealed archive custody and the previous
active generation.

For initial publication #997 atomically exposes the complete index and collection and stores
the original publication receipt. A later explicit index rebuild atomically switches only the
index selector through `publish_index_build`. Lost acceptance replies reconcile the exact
original completion/selector result; there is no parallel “rebuild succeeded” authority.
Original reader/worker/Native receiving remains a separate condition from result availability.

## 4. Read-only and original receiving contracts

The operation-local prepared descriptor index is not a saved generation. Finish the complete
root/terminal verification before setting it ready; retain selective indexes for journal/kind
access. Subsequent reads use the original query-only connection. An unknown requested journal
still raises the existing absent-journal error; an empty selective descriptor query is valid
only after a complete preparation. Missing metadata, mismatched root or corrupt terminal never
becomes successful empty data. A status or search request never prepares a persistent rebuild.

Removing the final disposable-index commit leaves its inserted rows visible to that same
connection. It does not remove schema operations, buffer spilling, SQLite journaling or close
I/O. Closing rolls back/discards the operation-local state; it cannot be reopened for recovery.
Retain the connection and all its cursors until original close returns successfully. Before-
close failure and a lost reply after actual close both keep that original reachable and prevent
reprepare. Only a retry of its original receiver releases the directory. Serialized thread
handoff is allowed under the existing owner; concurrent mutation/read is not added.

The `_ArchiveCorpus` correction follows the same rule. The reference `ReceivingArchiveCorpus`
retains an evicted original in its cache until `storage_received`, blocks new lookups after an
uncertain eviction, and receives every remaining original during cleanup. Capacity is two
usable entries plus one pending eviction, not an unbounded retired list. Production attaches
that cache to the discovery execution slot before its first Native open; exceptions leave it
reachable through the slot's original cleanup owner. Reader/cache/workspace deletion waits for
all Native summaries, worker execution and original database/stream receivers. The process
must not forget an original merely because a local context manager exits with an exception.

The standalone recovery reader is a separate implementation in
[`a_riverhog_recovery_tool/_provenance_reader.py`][recovery-reader]. Apply the same #964
one-descriptor-pass/terminal-verified/selective-index/receiving contract there, as described in
the recovery handoff. Keep that implementation owner-local and independent of Core/client
imports and databases; importing Core's reader is not the selected solution. Use the same
fixed archive fixtures to prove semantic parity. This is one shared traversal correction with
two existing owners, not competing formats or a new general registry.

Search keeps its current page read identity, complete generation requirement and current
disclosure checks. Missing/wrong-contract indexes return explicit unavailability; valid empty
results remain distinguishable. A changed archive/index/auth view invalidates the old page
cursor. Query-only optimization cannot revive a revoked principal. The existing selected-
collection identity walk is still real work per page; this reference does not claim constant
global query cost or silently redesign the query service.

## 5. Growth, privacy and current-state cut

Account separately for metadata objects/bytes, journal/source bytes, frames/semantic edges,
selected member/history associations, extracted index rows/postings/text, changed pages,
capture/restore bytes, live file/sidecar reservations and reclaimed objects. Report cold,
warm and evicted behavior, repeated attempts and final original receiving. A successful
prepared reader opens each metadata object once; query payload reads and required output
fan-out are separate. Do not call descriptor-index reuse a proof that Native replay, archive
payload reads or all repeated-member work are linear.

Use the existing growth declarations and their delegated operation profiles. Bound each
execution and installation batch, plus total live storage through owner admission. Count
private rollback/WAL spill, candidate failures, reference-count cleanup and file deletion.
If optional capture cannot fit, preserve mandatory work and accepted generations; do not add
an arbitrary archive-size/history-count ceiling. If mandatory execution itself cannot admit
its next unit, return the existing explicit deferred/capacity result and keep its liabilities.

Protect scratch using the existing ownership, no-symlink, mode and file-incarnation rules.
Capture only the already admitted Core discovery metadata and Native operational tables.
Never capture artifact payloads, decrypted object-cache buffers, credentials, arbitrary
process memory or authorization caches. Operation-local authenticated plaintext streams do
not become a persistent plaintext tier. Archives remain encrypted through existing contracts.

Follow the shared pre-v1 hard cut. The new executable refuses populated incompatible
operational state; it does not guess that a missing old workspace was disposable. Drain/receive
old executions with their old executable and retain mandatory source, intent, outcome and
construction IDs. Optional legacy images may be quarantined and replayed only after original
owners are settled and mandatory identity is retained. Initialize the new schema only in the
permitted new-empty state. Do not invent an archive export, reauthor history or delete user data
as a migration prerequisite.

Keep `test_missing_confirmed_core_storage_cannot_be_recreated`'s original-file refusal.
The new derived-cache fallback is a separate typed case with a new admitted live incarnation.
Rebaseline `test_actual_native_input_progress_stays_warm_and_resumes_after_owner_replacement`
only where it assumes every yield is durable: warm progress remains; cheap cold replay starts
from the selected authoritative/optional frontier, and a real admitted long capture resumes
from its exact complete generation. Preserve all semantic, currentness and receiving assertions.

## 6. Evidence and integration gates

### Observed here

[Baseline command/result][baseline-command], [JUnit][baseline-xml], [stdout][baseline-log] and
[maintained timing][baseline-timing] record 13 unchanged passing cases, no failures/errors/skips,
267.24 seconds pytest and 271.518583395 seconds whole process. Python is 3.12.3, SQLite 3.45.1.
The checkout was clean at subprocess start; only reference files were added during execution.
The numeric profile is the source-owned qualification profile, captured by `scripts.ci_timing`.

The selection includes complete-generation identity, bounded retry/fencing, current disclosure,
missing-index unavailability, stale source-registration rollback, missing confirmed storage,
four original Native/Core close cases, warm/cold Native ownership, and the real two-member
encrypted archive-only rebuild. That final fixture removes upload scratch, verifies exact
shared-history/member scope, compares unchanged custody receipts and performs cold/warm rebuild
reads. It is an in-process TestClient/SQLite local fixture, not installed recovery, PostgreSQL,
real provider, power-loss or a whole performance campaign.

[The experiment][experiment] and [tests][experiment-tests] execute the actual Core prepared
reader, index and SQLite, plus real Native `JournalSummary`/storage receiving for eviction.
[Counters][counters], [test output][test-log], [JUnit][test-xml] and [run conditions][validation]
record eight passing cases, no failures/errors/skips, 10.19 seconds pytest and 12.546809851 seconds
whole process. [Ruff/static results][static] pass for both reference files. The structurally
valid reader fixture supplies authenticated
plaintext at the ObjectReader boundary; its tiny journal byte chunks are not themselves a
Native semantic-validation fixture. The separate eviction test does create and validate real
Native journals. Do not conflate those scopes.

For three repeated header/journal/binding traversals, both paths verify four metadata objects
once, insert the same four descriptors, return identical exact result digests, enforce query-only
access and receive/delete the same original scratch owner. The unchanged index requests and
traces one final COMMIT; the proposed index requests and traces zero. This is a causal count,
not an fsync count or latency claim. Both create zero retained optional recovery generations.
The proposed open private transaction remains visible only to its original connection.

Two failed harness-reporting attempts are preserved: the first passed a tuple to canonical JSON;
the second exposed `dataclasses.asdict` reconstruction of `Counter` into non-JSON keys. These were
fixed in the reference reporting, not by changing product behavior, test conditions or skips.
Their raw evidence is linked from the final validation record.

### Required at the implementation/merge checkout

| Gate | Required witness and accepted result |
| --- | --- |
| Actual cheap path | Ordinary discovery/verification and retained-family entry both create no optional store or selection transaction; exact source/history/result bytes match unchanged Native semantics. Schema/private I/O remains separately counted. |
| Default-policy long resume | At least two real captures under the default composed policy; kill at source, Native, dependency and selection boundaries; restore exact usable closure. Measure all cost, count/debt, actual replay saved, cold restore and reclamation. Synthetic small thresholds alone are insufficient. |
| Publication and revisions | Mutate any of nine tables without changing top-level counts, race between revision check/page fetch, replace source/build/worker, revoke current authority and change deletion gate before final CAS. Reject stale candidate, keep old active index and sealed archive custody. |
| Exact selection/read semantics | Source/root/contract mismatch and corrupt/missing required descriptors fail; complete empty query succeeds; changed page generation/auth fails cursor; no query/status request executes replay/checkpoint/cleanup. Preserve original publication receipt across legitimate later index rebuild. |
| Receiving and capacity | Before/after actual close, partial constructor, disk-full/capture failure, slow retained reader and cancellation during execution. No new admission/reclamation bypasses an unreceived original; peak selected/candidate/pinned/garbage and sidecar bytes stay admitted. |
| Final cache hard cut | Delete only optional result state after known exact outcome; coalesce bounded revalidation, preserve exact history and source/custody errors. A missing original retained file is not repaired in place. |
| Readability/independence | Fixed historical archive fixtures and independent recovery with Core/server/client/checkpoint stores absent. Prepared-reader parity must include malformed terminal, missing objects, same IDs with different bytes and complete selected closure. |
| Backends/platforms | Actual supported SQLite and PostgreSQL catalog transactions/currentness, and shipped Linux/macOS/Windows runtime/filesystem receiving and crash windows. Audit installed SQLite topology/version; current local 3.45.1 evidence does not qualify #996 retained storage across platforms. |
| Growth | Scale descriptor count, journal count, tiny histories, shared prefixes, member associations and extracted text separately. Count actual dependency traversals, writes/syncs, retry/restore/GC and final receiving; do not hide repeated whole-corpus work behind a per-quantum bound. |

Reproduce the focused commands from their JSON records using the frozen environment rooted at
`/workspace/scratch/939b84c043b0/storage-999-env`. Before launching pytest, call
`scripts.validation_numeric.bind_numeric_environment(env, qualification_numeric_policy())`;
keep `TMPDIR`, `--basetemp` and `RIVERHOG_CI_TIMING_DIR` outside product paths. The final reference
tree is intentionally different from the clean baseline start: reproduce the unchanged baseline
on the audited source, and the reference tests on the handoff tree. The integration agent must
qualify its actual final source and merge checkout through the repository's normal gates.

[agents]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/AGENTS.md
[readme]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/README.md
[958-growth]: https://github.com/nashspence/riverhog/issues/958#issuecomment-6173688740
[958-coordination]: https://github.com/nashspence/riverhog/issues/958#issuecomment-6176895364
[reader]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/provenance_archive_read.py
[metadata-index]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/provenance_metadata_index.py
[discovery-metadata]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/canonical_discovery_metadata.py
[validation-owner]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/services/discovery_validation_owner.py
[corpus-jobs]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/canonical_discovery_corpus_jobs.py
[rebuild]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/canonical_discovery_rebuild.py
[build]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/canonical_discovery_build.py
[index]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/canonical_discovery_index.py
[index-models]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/catalog_provenance_index_models.py
[search-service]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/services/search.py
[search]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/canonical_discovery_search.py
[provenance-service]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/services/canonical_provenance.py
[recovery-reader]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/_provenance_reader.py
[997-finalization]: https://github.com/nashspence/riverhog/blob/a7ea19bf795c249476c9d767eb821d74bcf98b2a/.reference/IMPLEMENTATION.md#prepare-outside-the-writer
[baseline-command]: evidence/discovery_BASELINE_VALIDATION.json
[baseline-xml]: evidence/discovery_BASELINE.xml
[baseline-log]: evidence/discovery_BASELINE.txt
[baseline-timing]: evidence/discovery_BASELINE_TIMING.json
[experiment]: implementation/discovery_experiment.py
[experiment-tests]: implementation/test_discovery_experiment.py
[counters]: evidence/discovery_COUNTERS.json
[test-log]: evidence/discovery_TESTS.txt
[test-xml]: evidence/discovery_TESTS.xml
[validation]: evidence/discovery_VALIDATION.json
[static]: evidence/discovery_STATIC.json
