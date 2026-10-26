# Prepared primary recovery contract

Proposed design for [#981](https://github.com/nashspence/riverhog/issues/981),
under [#903](https://github.com/nashspence/riverhog/issues/903).
Source references below describe `f0a7c2e263bb96533d71c9a3b4d43544b77d7b85`.

## 1. Decision and boundary

**Before publishing a member, durably select its complete prepared primary.
Every retry uses that exact record. Reclaim its large bytes only after durably
recording custody that covers the same primary.**

Put preparation, input comparison, replay and receipt validation in the shared
`riverhog-client` producer owner. Supply durable storage from the calling
application. Use a focused file-backed implementation with exact member lookup,
immutable manifests and streamed byte files. Also retain the server's existing
exact custody receipts across finalization, using its existing member state and
point-read endpoint; section 6 defines this narrow lifecycle change. No new
reservation endpoint, database, general retry framework or workflow registry is
needed.

The existing service already accepts exact journal create/chunk/seal and binding
retries. `build_member_journal` generates new Native identities; calling it again
after an uncertain outcome is not a replay. The existing accepted-binding branch
checks only the artifact ID, and receipt validation does not compare the producer's
original primary. These are the focused integration touchpoints.

Preserve unrelated-journal rejection, Native octets/addresses, archive identities,
opaque artifact identity, original history, completion requirements and live
authorization. This design neither deletes old orphan journals nor reclassifies
them as dependencies. Discovery-source checkpointing, collection-wide discovery,
completion-journal authoring and independent archive recovery keep their owners.

## 2. Ownership and minimal interface

Thread one explicit durable-store handle through `CollectionProducer` and
`IncrementalCollectionProducer`, and through the derived writer/runtime wrappers.
No implicit temporary-directory or memory-only fallback may claim restart safety.
Make the pre-v1 change across maintained callers together.

The shared interface needs only these operations; names are illustrative:

| Operation | Required meaning |
| --- | --- |
| `open_construction(allocation, scope)` | Acquire live writer ownership; validate the current store and accepted session header. Distinguish explicit fresh initialization from resume. |
| `prepare_or_load(member, inputs, observation_factory)` | Compare the retry descriptor first; return the durable winning record. Run a factory/Native author only for a new record. |
| `replay(prepared)` | Reconcile the exact remote member/binding and advance existing stage/bind/materialization operations using retained bytes. |
| `accept_custody(prepared_or_compact, receipt)` | Validate member, completion requirement and full primary; durably retain the receipt/commitment before releasing bytes or reporting safe release. |
| `reclaim(member)` | Point cleanup after verified custody; idempotent and restartable. Terminal allocation retirement uses the application's existing member enumeration. |

`stop()` releases process resources and requests worker shutdown. It does not
erase pending state. Keep `resume_artifact_custody(identity)` usable without
reading already released source bytes: it must load the saved primary/commitment
and use the same receipt validator. A read-only custody lookup does not authorize
a new production request with different inputs.

## 3. Scope and immutable inputs

Namespace records by the existing application's durable allocation, stable
service/account scope, construction and opaque member ID. Neither a payload hash,
collection number alone, retry key alone, nor an expiring capability is a sufficient
namespace. Equal-byte artifacts remain distinct. Service aliases must resolve to
the same explicit application scope; switching deployment/account is a conflict.

Persist a construction header binding the exact idempotency/create request and
accepted collection ID, delivery context ID and server construction identity.
Acquire construction ownership and compare the retained service/account/create
descriptor **before** constructor create-or-resume or initial-tag mutations.
Validate returned identities against the retained header before member publication.
Also retain the actual `ProducerAttribution.construction_identity`: current
non-execution and execution paths derive it differently. Do not silently replace
one identity with the other. Credentials and capability tokens are excluded;
every resumed API call still obtains current authorization.

The immutable member record commits the following complete inputs:

| Group | Exact values |
| --- | --- |
| Member | Opaque artifact ID, verified length and SHA-256; existing raw-part authority remains owned by the upload path. |
| Producer | Producer app, adapter ID/version, source event, ingest source, canonical source context, actual construction attribution and full completion requirement. |
| Observation | Selection mode; original graph octets, selected observation/observer/artifact/state/occurrence identities, and the exact contract catalog pin used for validation. |
| Output | Output ID or explicit absence, ordered exact causal State references, materialization hint or explicit absence, and `allow_missing_materialization_hint`. |
| Prepared result | Journal ID, exact Native octets/length/SHA-256, complete binding including all anchor coordinates and delivery-association ID, and exact materialization decision. |

Use the repository's canonical JSON/scalar rules for a private, domain-separated,
versioned retry descriptor. Store complete preimages while pending and their
length/hash commitments in the manifest. Reject unknown fields/formats. This
private commitment detects changed inputs; it is not a new archive or Native
identity. Frame variable components unambiguously, distinguish absence from empty,
and preserve meaningful sequence order. Never serialize a Python callback or
infer identity from its memory address.

**Observation selection has three explicit modes:** supplied exact observation,
producer automatic observation, or an application-selected lazy observer. The
lazy mode declares its observer/contract and semantic configuration, including
host/source identity where relevant. Freeze its first result with the prepared
record. On retry, compare that declaration and reuse the frozen result without
calling the factory. A newly supplied observation must match its original graph,
IDs and catalog pin. Missing input must not silently switch supplied mode to
automatic mode. This lets CLI/derived callers recover without generating a new
observation, while still rejecting actual input changes.

Source path/reader handles are locators, not preserved identity. Continue to verify
actual bytes through the existing source reader. Once exact custody exists,
receipt reconciliation may use the stored verified member identity and must not
require released payloads to exist.

## 4. Current-only durable format

Use a private directory under the application's existing allocation. Store a
single recognized format, proposed `riverhog-prepared-primary/v1`; this is a
current-only private format, not an additional supported legacy format.

| Record | Contents and lifetime |
| --- | --- |
| Store/allocation marker | Format and allocation/scope identity, established durably by the application before creating the remote session. |
| Construction header | Immutable accepted session identities and create-input commitment; present before member publication. |
| Member `prepared.json` | Small manifest with scope/member/input commitments, journal file length/hash, binding and references to separately stored input/decision bytes. |
| Member byte files | Exact observation/input preimages, exact materialization decision, and exact Native journal; streamed files, not a whole-construction JSON object. |
| Member `decision-accepted.json` | Exact decision length/hash and a verified successful response commitment from the immutable setter; no unresolved acknowledgement is recorded as accepted. |
| Member `custody.json` | Compact retry descriptor commitments, member identity, exact primary, completion-requirement SHA and validated receipt. Durable before large-byte deletion. |
| Local closing/finish-ready records | Fixed allocation inventory commitment, bounded closure cursor, and completed decision/registration barrier; never archive authority. |
| Allocation terminal record, where supported | The application's durable final/disposition record; prevents reopening while final cleanup proceeds. CLI currently retains its allocation instead of offering retirement. |

Use safely derived member paths with bounded fan-out; never interpolate untrusted
source paths. Keep prepared data private to the application (restrictive modes,
no symlink traversal, no credentials or provenance bodies in diagnostics). This
adds pre-custody operational evidence, not archive authority or an independent
recovery prerequisite. Do not place it on archive providers or disposable caches.

Atomic preparation, under construction ownership:

1. Write candidates into attempt-specific private files/directories on the same
   durable filesystem, streaming hash/length calculation and checking short writes.
2. Validate complete Native content against its exact catalog and binding. Flush
   and fsync each referenced file, then publish it without replacing a winner.
3. Fsync the directories that make those files reachable. Publish the immutable
   manifest with an exclusive/no-clobber operation, then fsync its directory.
4. Return a prepared handle only after that barrier. Never stage a losing or
   uncommitted candidate. An uncertain local write is resolved by loading and
   validating the durable winner, or stops; it is not success by assumption.

A crash before the manifest leaves unreferenced temporary candidates and no
authorized remote primary publication. Remove those only after confirming the
previous local writer has stopped; only the manifest selects a candidate set.
Do not mistake orphan files with an expected name for a committed winner, or let
them collide with a newly authored candidate. A committed manifest with missing, truncated,
changed or mismatched bytes is a recovery error, not a request to author again.
Apply equivalent directory durability when creating the store itself.

Initialization is explicit; opening an existing allocation validates rather than
upgrades, erases or recreates it. Unsupported formats and unversioned nonempty
state fail with an actionable diagnosis. A future database choice must follow
`StateSchema` and explicit initialization/upgrade policy; none is introduced here.
Do not run a full-store integrity scan on each member operation.

## 5. Publication and restart protocol

| Durable state | Meaning | Allowed next action |
| --- | --- | --- |
| New member | Valid retained allocation/session; no earlier remote member publication | Prepare one record. |
| Prepared | Complete durable primary; remote staging/binding outcome may be unknown | Reconcile and replay this same record. |
| Custodied | Exact verified receipt and compact commitment durably retained | Release the source; reclaim prepared bytes only once the materialization decision is also durably accepted. |
| Retired allocation | Existing owner has durably closed/disposed the construction | Complete bounded cleanup; never reopen as a fresh construction. |

For an absent local record under a valid bound session, first perform the existing
point `get_collection_upload_session_artifact` for that exact member under writer
ownership. Only confirmed member absence in the valid open session permits new
preparation. An existing row, unavailable session, uncertain response or other
error stops; do not author a replacement before discovering the remote member.
Existing prepared/compact records follow their replay paths instead.

For new work, install the member intent **before member registration as well as
before primary staging**. This small ordering change makes a registered member
with no local record an explicit recovery error, instead of a safe-looking gap
between registration and preparation. It does not create a server transaction.
Imported histories keep their existing exact-byte ownership and cannot substitute
for the member's prepared primary.

For a pending member:

1. Load the retained record before observing/authoring. Compare all retry inputs.
   Register the same measured member and raw authority through existing calls.
2. Read the exact accepted primary. If present, require complete binding equality.
   If absent, upload the retained journal through the existing create/append/seal
   protocol, then bind the retained primary. Reuse every Native byte and address.
3. Apply the retained materialization decision. Verify that the successful setter
   response names that exact decision, then durably record its length/hash in
   `decision-accepted.json`. Lost or unrecorded acknowledgement requires replay
   while the session is open; success of an earlier phase is not custody.
4. Continue payload and required-history work with their existing owners. Derived
   input-history acceptance and completion requirements remain independently required.
5. Check custody and durably record it as described below. Return pending/error
   on bounded-call exhaustion or cancellation; never report partial work complete.

No persisted `staged=true` or `bound=true` hint is required for correctness. Remote
read-back and immutable replay resolve uncertainty. Never delete a staged journal,
choose a new journal ID, weaken corpus validation, or treat an unrelated primary
as equivalent just because the content hash matches.

Before calling `complete`, settle every member's materialization acknowledgement
through the existing allocation's bounded enumeration/checkpoint. Under writer
ownership, durably mark local closing and freeze the owner's exact member inventory;
reject new preparation, but allow replay of its existing members. Advance one
bounded closure cursor only after those members are registered, have the exact
primary and have durable decision acknowledgements. An allocated/prepared member
cannot be silently omitted. Persist a finish-ready marker bound to that inventory
before the first `complete`. Restart resumes the cursor or reuses finish-ready;
finalization polls do not repeat the whole inventory walk. This is a local barrier,
not a new server two-phase protocol. Do not require
all custody receipts before `complete`: closing the final pack may itself need
that call. If the session is terminal, use the retained exact receipt readback
specified in section 6, without staging or invoking setters on the closed session.
A finalized short-circuit must still drain or expose local cleanup. A missing
local decision acknowledgement after publication retains its preimage and reports
the discrepancy; a finalized flag alone must not invent the missing local proof.

### Missing state and the boundary around initial creation

The recoverable window starts with an already durably bound session header.
Applications must retain their original allocation and store identity across
restart. An expected missing store/header, an existing remote member without its
intent/compact record, or an accepted binding with no original input authority
fails closed. Never adopt it by reauthoring or by copying only its artifact ID.

Whole-state loss, legacy unrecorded staged journals, and an ambiguous first-create
acknowledgement require explicit reconciliation, with source and evidence retained.
A local `CREATING` marker alone cannot distinguish a lost fresh-create response
from a collision with an older remote session. `files_total == 0` does not prove
the session has no journals. This design makes no automatic-repair promise for
that ambiguity. It does cover lost journal-create/chunk/seal/bind acknowledgements
after the session header is durable, which is #981's demonstrated window.

If automatic first-create recovery becomes required, the narrow extension is a
service-owned, transactionally consistent pristine-session witness (no members,
journals, bindings or completion mutations), using indexed existence checks and
current authorization. That is an additional contract decision, not a prerequisite
or silently assumed endpoint for this handoff's focused implementation.

## 6. Custody, finalization and reclamation

### Preserve exact receipt readback after finalization

At the audited source, `_finalize` generates every custody receipt before catalog
projection, but `_publish_catalog_collection` then deletes the upload and its
member receipts. `get_artifact` requires that upload to exist. Thus an ordinary
finalization or lost response can remove the only remote readback before the
producer has retained its receipt locally. This must be repaired with the primary
intent lifecycle; waiting for all early receipts before `complete` can deadlock
the last open pack.

**Chosen narrow extension:** copy the original immutable `custody_receipt_json`
alongside each member during the existing bounded `_advance_catalog_artifacts`
projection, into a retained receipt field on the existing `CollectionArtifactRecord`.
Each batch validates and copies the exact receipt and advances its existing cursor
atomically. Require receipt/member equality and complete copy coverage before
publishing/deleting the upload. The existing per-member/count checks can establish
coverage without a new full-table scan. Never generate an equivalent replacement
receipt or change its `receipt_sha256`.

Extend the **existing exact upload-artifact GET** to fall back, after published
collection visibility, to that member's retained receipt. Return the existing
member identity, `payload_sealed=true` and original `custody_receipt` shape.
Its current `require_read_access` already handles finalized creator identity;
preserve current principal/key/creation/deletion-scope checks. Do not make closed
sessions writable or grant catalog/provenance export access. A live unauthorized
or revoked caller still cannot retrieve the receipt.

This is a proposed current-only schema/read-lifecycle change in the existing
application database, with explicit `StateSchema`/upgrade and boundary tests.
Treat retained receipt bytes as operational custody evidence with the existing
collection lifetime, **not disposable catalog projection**. Rebuilds, reindexing
and interrupted upgrades must preserve them by stable identity. Do not claim they
can recreate the original receipt from the archive. A nullable absence is valid
for an imported/recovered collection that never had this upload receipt; it is
not valid for new custody-transfer finalization. Missing original evidence yields
a retained-state error, never synthetic custody. Existing collection deletion owns
its eventual disposal; no TTL, per-client acknowledgement service or new registry
is introduced.

The cost is one fixed-shape receipt copy per member during existing projection,
`O(N * C)` retained metadata, and indexed `(collection_id, artifact_id)` point
reads. Count the actual backend seek cost and row/byte work. Do not substitute a
per-member final-root proof download: the current proof API rebuilds binding-tree
work over the whole collection per call and requires provenance/export permissions
that an upload-only producer need not hold. A future source-recovery design may
use those proofs; they are not the automatic cleanup rail selected here.

### Local custody and independent decision acceptance

Use the existing receipt model and
`validate_collection_upload_artifact_custody_receipt` for its self-commitment and
collection/member/length/digest checks. In both ordinary acceptance and
`resume_artifact_custody`, also require:

```python
receipt.primary == retained.binding
receipt.completion_requirement_sha256 == retained.completion_requirement_sha256
```

The service remains responsible for its validated payload, primary and inherited
history closure. The local receipt is obtained through the authenticated service
path; a self-hash alone is not proof of remote custody.

Publish and fsync `custody.json` before returning the receipt as safe release.
Custody does **not** prove materialization-decision acceptance: the current worker
can issue custody before that setter, while finalization rejects a missing choice.
Keep the exact decision and other prepared files until the matching
`decision-accepted.json` is also durable. A cold read-only custody lookup may
release payload custody, but must retain this unresolved publication obligation
for the next bounded replay step. This adds no prerequisite to the service's
existing meaning of custody.

Once both predicates hold, delete the large input/journal/decision files and fsync
their directory; the compact records keep all retry commitments and the exact
primary. On open, validate `custody.json` and the decision acknowledgement first;
only their verified combination permits the prepared files to be absent. Without
that combination, required preimages remain mandatory. Thus successful cleanup
is not mistaken for corruption on restart.
A crash may leave redundant bytes; it must not leave a pending record without
its only primary or an unresolved decision without its exact preimage.
If a receipt write fails (including disk full), retain bytes and retry it.

Keep compact input commitments, exact primary and receipt until the existing
application allocation retires or explicitly absorbs equivalent retry authority.
Otherwise a late append could change inputs after the only comparison witness
was deleted. Retire an allocation only after its existing terminal/disposition
contract settles every member; cancellation, age, timeout and failed-job status
alone cannot erase unresolved intent. A safe abort does not mean preservation
succeeded and requires the source owner's existing disposition authority.

`payload_sealed` may release internal range-reader state as today. It cannot
release prepared intent or the application's source. Successful stage/bind and
`stop()` have no reclamation authority. A finalization response must not bypass
receipt-first cleanup ordering.

## 7. Concurrency and execution ownership

Use one live writer per construction/allocation, through nonblocking OS ownership
or an existing application mechanism that covers every producer entry point.
Keep ownership while authoring, publishing, accepting receipts and reclaiming.
Inside that owner, existing bounded payload-upload concurrency remains available.
Independent constructions have separate ownership and can progress concurrently.

Use process-lifetime locks, not an age-based lock-file lease. Never unlink a held
lock or allow timeout/cancellation to prove a worker has stopped. Join or otherwise
quiesce local workers before releasing ownership. Remote requests that outlive a
lost response are safe to reconcile because every repeat carries the same record.
Current server permissions/processing fences still apply on every operation.

An exact compare/install remains useful defense against duplicate same-key calls:
an identical contender uses the stored winner, a differing contender conflicts,
and neither stages its own losing bytes. Do not hold a global application mutex,
database write transaction or unrelated construction lock across network waits.
Only filesystems with qualified no-clobber publication, fsync and ownership
semantics support this implementation; multi-host takeover is not implied by
putting its directory on a network share.

## 8. Resource and growth contract

Let `N` be retained member records; `P_i` the exact private inputs/observation bytes
for member `i`; `J_i` its Native journal bytes; `R` actual replay/receipt attempts;
`W` admitted concurrent work; and `B` a configured transfer buffer/page size.
Count source revalidation bytes and application inventory work separately.

| Operation | Required cost and accounting |
| --- | --- |
| Prepare | Linear writes/hashes in `P_i + J_i`; one immutable publication. No whole-construction rewrite or scan. Include Native author/validation work separately. |
| Point resume | Address one member and its commitments. No sort/glob, database full check, whole-store hash or `du()` on each append/retry. |
| Journal replay | Stream bounded chunks; linear local reads in bytes actually traversed per attempt, HTTP writes only for the unaccepted suffix. Charge retries explicitly. |
| Receipt/readback/reclaim | Exact member operation and fixed-size receipt/commitment work; one server receipt copy and one local byte reclamation. No corpus revalidation by the client. |
| Retirement | One complete owner-driven member traversal with resumable progress and bounded deletion batches. Account for interrupted steps; do not rescan an ever-growing completed prefix. |

Use bounded chunks as `content` to the current upload helper. It traverses input
to skip `accepted_bytes`; a cold retry can read the retained prefix even when no
prefix bytes are retransmitted. Count that `J_i` cost instead of claiming suffix-
only local work. Do not pass one giant blob into its bytearray-buffer loop.

Steady recovery storage is `O(sum_pending(P_i + J_i) + N * C)`, where `C` is the
fixed-shape compact commitment/receipt record; also charge in-progress candidates
and the server's retained receipt copies. Store the completion requirement SHA,
not a repeated full declaration, in each compact record.
Large materialization/context/causal data belong in byte files with commitments,
not in the compact receipt. `N * C` is necessary retained retry metadata, not a
hidden cap on the archive. Reclamation transfers/disposes it only through the
application's established allocation lifetime.

Preparation today returns a complete `ProducedMemberJournal.content: bytes` and
materializes observation graphs. This handoff **does not claim constant-memory
Native authoring**. The inherited per-member working set is at least
`O(max_i(P_i + J_i))`; new store/replay buffers add `O(W * B)` and no collection-
sized in-memory index. Integration must measure that dimension and either stream
preparation through the owning API or use explicit operator admission for the
existing per-member working set. Hidden metadata limits are not an acceptable fix.

Use the application's existing member allocation/output checkpoint for point keys
and bounded recovery/cleanup enumeration. Do not add a parallel status registry.
If an owner lacks a resumable enumeration, add that capability to its existing
owner and qualify its seek/deletion work. A single linear terminal walk is valid;
restarting that walk for each member is not. Count actual filesystem/index work,
not just high-level method calls. Existing CLI inventory or producer pending-source
scans are not qualified by this design merely because intent lookup is bounded.

Resource admission belongs to application/operator configuration. Budget pending
and temporary bytes, active writers and buffers; reserve space for control/receipt
records. Saturation explicitly defers/rejects new preparation before any primary
mutation while allowing custody/reclamation to progress. Never evict unresolved
intent, silently omit a member, impose a total collection ceiling, or hold a
coordination/status call waiting for disk/network work. Large collections may
progress through an arbitrarily long sequence of bounded steps.

Linear useful-byte work and per-member metadata work are distinct from backend
lookup costs. If a chosen existing index adds logarithmic seeks, declare and
measure that term rather than calling the entire operation strictly linear.
Repeated failure work is proportional to actual attempts and bytes traversed;
there is no claim that arbitrary retries cost the same as one attempt.

## 9. Application placement and integration cuts

| Owner | Durable placement | Required integration detail |
| --- | --- | --- |
| FTP | Existing private claim control root beside manifest/provenance | Reuse `_prepared_observation`; pass the shared store into `_publish_claim_once`. Keep unresolved intent through stop/restart and receipt-led claim cleanup. The current in-process custody lock alone is insufficient for cross-process ownership. |
| CLI | Alongside the existing XDG upload allocation keyed by root/retry key | `upload_cmd` must load saved intent before invoking the selected native observer. Use a declared lazy observation or retain its original result. Keep original opaque IDs; do not rewrite the whole allocation per member. |
| Derived writers | `TargetExecutionSession.state_root` beside output checkpoints | Thread placement through target runtime, transform runtime and both writer forms. `TargetOutputCheckpoint.producer_file()` restores no observation; shared lookup must precede default observation. Never use disposable `ProcessingWorkspace` as the store. |

Derived `source_context` includes claim/fence/execution evidence. Preserve the
original semantic inputs; refresh only live capabilities separately. A genuinely
different fence/execution cannot silently rewrite the prepared journal. Resume
only when the current processing contract authorizes the same production inputs;
otherwise surface that concrete conflict. Keep exact causal State references,
required input-history binding and completion obligations intact.

Extend existing failed/cancelled target-state retention to recognize pending
primary intent; age-based pruning must not destroy it. After custody, preserve the
existing ability to resume without rereading released target output. FTP final
claim deletion and target terminal cleanup must first settle or retain every
prepared member's receipt/commitment under the ordering above. The CLI currently
has no terminal retirement flow: keep compact records beside its existing upload
allocation for that allocation's retained lifetime. An explicit durable retirement
operation may be added by that owner later; it is not a prerequisite for this repair.

The integration agent owns routine source/module/test choices. Start with the
single actual-service interruption witness, then apply this same owner contract
to all three placements and the acceptance matrix. Do not merge `.reference/`
into product source. Record the finite consumption decision in #981.
