# Complete, incremental Native generations

This is the concrete storage design selected by the [#996 implementation proposal](IMPLEMENTATION.md). It is non-authoritative reference input. The accompanying Python programs test mechanisms using ordinary SQLite/filesystem calls; production must reproduce them through Riverhog's original admission, construction and receiving owners.

## 1. Backend choice and durability boundary

Keep the active semantic database **disposable** throughout its life. It has one owner/connection while executing or capturing; its WAL is private, its synchronization may remain OFF, and interruption can discard it. A separate optional `generations.sqlite3` stores immutable objects under fixed **DELETE journal mode, synchronous=EXTRA**, preserving the existing platform `fullfsync` policy and original owner directory synchronization. Verify requested PRAGMAs rather than assuming they took effect. Never lower the retained store's mode between checkpoints.

The store is created only after a #995 grant. It is not an archive, a retrieval-cache replacement, an alternate application database, or proof of source authority. Keep it in the existing registered owner-private Native workspace, with its existing no-symlink, ownership and permission checks. Source-owned filename admission, sidecar handling, reclamation cursors, extent reservations and final receiving must all recognize the new exact files. Do not create an unmanaged directory of segments beside the old workspace.

Use `generations.sqlite3` and its SQLite rollback sidecar for the retained store. Put `working-<worker_epoch>.sqlite3` and its SQLite sidecars under the original admitted worker namespace; a restore builds that new worker file before exposing the executor. Use one `capture-<generation_id>.sqlite3` spill file per admitted candidate. Validate the numeric identities through the source-owned filename parser and register every original before opening it. A file left by a dead worker is not reused as trusted scratch. Receive that worker/its readers and reclaim it through the existing owner; a new worker uses its own file. Do not infer permission to delete a foreign file from a matching filename. Creation synchronization uses the original construction's directory primitive before the candidate can be selected.

The lazy generation connection has a separate original passive construction child retained by the existing owner slot after grant and before effects. Do not reenter the completed single-attempt `JournalConstruction` opening frame. Its own admitted connection/file records and capture/restore children participate in the original workspace lease's composite receiving, as specified in IMPLEMENTATION.md. Scratch closure alone cannot release a workspace still used by an unreceived generation child.

Current Native operational SQLite already stores admitted provenance frame and assertion/map bytes. Capture only those owned Native tables and bounded control metadata. Do not capture arbitrary process memory, artifact payloads, decrypted archive/cache buffers, keys or cached authorization decisions. Do not upload/export these images as plaintext or treat this as permission for a new plaintext storage tier. Anything moved beyond the existing operational boundary must use the existing encrypted storage contracts. See [the current store](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/riverhog-provenance/src/riverhog_provenance/journal_store.py) and [reclamation owner](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/riverhog-provenance/src/riverhog_provenance/journal_reclamation.py).

The retained choice deliberately uses EXTRA: SQLite documents an additional directory synchronization for DELETE-mode commit; FULL alone has filesystem-dependent last-transaction durability. This is a rationale for the proposed primitive, not a power-loss certification. Initial file/directory creation, fullfsync availability, controller/filesystem behavior and the shipped VFS require actual platform qualification. [SQLite synchronous documentation](https://www.sqlite.org/pragma.html#pragma_synchronous).

The pinned Python 3.12.3 runtime observed in this workspace uses SQLite **3.45.1**. Current SQLite documentation describes a WAL-reset race fixed in 3.51.3 and designated backports, triggered by concurrent connections writing/checkpointing the same file. Enforce the private single-connection topology, with no simultaneous checkpoint/writer/readers, and audit the installed runtime when implementing. This reference neither upgrades the repository nor claims the pinned runtime is qualified for arbitrary concurrent WAL use. Frozen terminal readers have their own read-only lifetime after mutation stops; resumed readers receive a separate restored file. [SQLite WAL documentation, section 11](https://www.sqlite.org/wal.html).

## 2. Authenticated fixed-depth page map

Use SQLite's documented 32-bit page-number domain and an eight-level radix map, four bits per level. Each immutable node has 16 child addresses plus its depth and covered-page count. The final level addresses immutable complete SQLite page images. Addresses are SHA-256 digests with distinct node/page domain tags. A generation envelope commits to the root, page count/size, exact binding and serialized quiescent control; the selected descriptor commits to that envelope digest and store/generation identity.

The object table contains `(address, kind, body, incoming_references)`. The generation table holds its root, exact binding, state and envelope. Use the repository's canonical encoding primitives in production; the reference's explicitly named format is internal test input and not a proposed public archive format.

To change page `p`, write its new immutable page object, then rebuild only the eight nodes on its path. Each rebuilt node reuses all untouched child addresses. A page deletion replaces the leaf with absence and removes any now-empty ancestors. The initial capture inserts every page once into an empty map; later captures update only changed pages and genuine truncation tombstones. Hashing the new paths is bounded independently of the length of the validation history. Physical SQLite B-tree/rollback-journal work remains separately attributable.

The choice of an authenticated map closes a real corruption case. A table queried as “latest page version at or before this generation,” even with per-row checksums, can silently return an older page if the newest row is deleted. The map instead references the exact required page address: deletion, corruption or wrong kind fails validation. This also avoids an ever-longer parent/delta chain during restore. It does not prevent a shared object from being damaged; if both retained generations reference a damaged object, both may require authoritative replay.

`generation_store.py` implements the map, complete-envelope construction, staged writes, direct restoration, explicit candidate outcome receiving, pins and bounded reference-count reclamation. It creates at most eight new nodes per changed page. It uses bounded batches rather than a transaction over the whole source. Intermediate path nodes may become unreachable; their cleanup is counted and queued, not hidden.

The reference uses 64 pages per staging transaction. In production use the smaller of 64 and the current admitted byte allowance divided by page width; if even one page cannot be admitted, defer/refuse the optional operation. This bounds an execution unit and does not limit total database or archive size. Bound the collector similarly by its actual node/edge/byte envelope. Continue both operations through ordinary owner scheduling, without holding a Core writer while they wait.

## 3. Obtaining changed pages without persistent per-step tracking

Use a documented WAL reader for the owner-private scratch DB. Do not depend on the transient shared-memory WAL-index format, a new VFS, an OS copy-on-write filesystem, or SQLite's optional snapshot API.

### Before the first candidate

Run ordinary scratch transactions with bounded WAL resource management. There is no optional dirty-map database, generation store, hashing pass or checkpoint representation. A restart-only executor never opens them. A nonseekable input does not gain replay authority merely because scratch exists.

After the first grant, quiesce the single connection at a complete semantic epoch and disable its automatic checkpoints before entering the extractor. Run the scratch WAL checkpoint/TRUNCATE, verify completion and exact database page extent, and stream the complete database once into the candidate. This is a consistency operation on disposable scratch; it is not application recovery publication. Bind the live control from the same quiescent point. Keep the executor stopped while capture is in progress, yielding bounded snapshot work/cancellation rather than accumulating an unbounded mutation log.

If the first candidate fails, a bounded retry may continue the same frozen, charged candidate. If that frozen candidate cannot be continued, disable further optional capture for that live executor; do not repeatedly rebaseline the growing database. A new recovery attempt remains subject to the same surviving policy/accounting root and pays its actual replay/capture work.

After positive selection receiving, establish an empty-WAL tracking interval, disable automatic checkpoints on that private connection, and reserve the finite scratch WAL envelope. The original selected descriptor remains immutable.

### Subsequent candidates

There is no periodic uncharged coalescing task. At a policy decision, use fixed metadata such as WAL length and admitted extents to estimate work. Only after a grant:

1. Quiesce and require the original sole connection, no active transaction and unchanged input/control binding.
2. Check the WAL extent against the granted/resource envelope. Stream the documented header/frame/checksum structure; verify the magic/version, page size, salts, rolling checksums and committed database extent. Reject unsupported, malformed, reset, partial or uncertain input rather than guessing the live `mxFrame` from an undocumented structure.
3. Coalesce each committed page's last image in a bounded disposable spill database. Memory holds a page and fixed metadata/batch state; the total coalesced set may spill within admitted workspace quota. Preserve transaction shrink semantics and discard pages outside the final database extent. Add explicit tombstones for pages removed since the previous complete generation.
4. Stage those changed images/tombstones into the immutable page map. Bind the exact new page count and full quiescent control. No source frame can be considered covered merely because the WAL contains some of its writes.
5. Exhaust the page iterator and successfully exit the capture context, including its final original-file/owner/quiescence checks, **before** sealing the complete envelope or offering it to Core. A partially consumed incremental capture is not safe merely because the old tree still has the right page count. Persist the envelope, then offer it for Core selection. Positively received selection establishes the new tracking base; only then reset its interval for continued incremental promotion.

A known rejected candidate has no new base: after receiving it, resume the original base/unchanged WAL interval if its continuity and resource envelope remain valid. Otherwise disable further promotion, safely truncate only disposable scratch and continue semantics. An unknown outcome keeps the complete candidate pinned and blocks another candidate; if current worker/source authority is positively established, the owner may abandon tracking and continue disposable semantic execution with #995's conservative accounting seal. Optional receiving alone must not indefinitely block required work. Unknown current authority still refuses execution.

The reference extractor is intentionally conservative about a tail that SQLite itself might ignore: it refuses uncertain tails and never treats a previous commit marker as proof that later committed state was absent. A rejected optional capture preserves prior roots; charge its attempted work. Production must prove any relaxation against the actual pinned SQLite engine and its transaction/rollback cases.

The documented reserved lock-byte page is a concrete exception to “every new page has a WAL frame.” For the supported built-in Unix/Windows VFS behavior, when growth crosses or regrows page `(0x40000000 // page_size) + 1`, materialize an explicit zero image for that reserved page if no frame exists. No other missing new page may be synthesized. Include the reserved page in the map/extent and test boundary behavior without imposing a database-size cap. The reference uses a synthetic format-boundary witness for this case; installed VFS qualification remains necessary.

SQLite documents the on-disk WAL and checksum rules, while the WAL-index is transient. Its online backup API produces a complete database copy even when copied in steps; repeated full backups would not solve checkpoint-to-checkpoint growth. Its snapshot API requires a special build option and WAL-history lifetime and is not a self-contained durable generation. These are reasons for this design choice, not permission to bypass backend qualification. [File format](https://www.sqlite.org/fileformat2.html#wal_file_format), [backup API](https://www.sqlite.org/backup.html), [snapshot get](https://www.sqlite.org/c3ref/snapshot_get.html), [snapshot open](https://www.sqlite.org/c3ref/snapshot_open.html).

### Finite WAL envelope

Retaining every WAL frame until some future checkpoint would be unbounded. At each existing execution admission boundary, reserve enough private WAL space for the next bounded quantum under the actual storage admission rules. If tracking cannot continue within the finite envelope and no checkpoint can be admitted, receive/discard the optional tracking state, checkpoint/TRUNCATE only the disposable scratch, and **disable further optional promotion for that live attempt** with reason `tracking_capacity` or `tracking_continuity_lost`. Preserve selected generations and continue semantic/mandatory work.

Make the resource input concrete: add a strict Native-owned `NativeSnapshotResources` value with `wal_octets=67_108_864` (64 MiB), `dirty_index_pages=131_072` and `stage_pages=64` as initial engineering defaults. All are positive integers; the staging value is further reduced by the current byte allowance. Thread this owner-local resource value through Core configuration/installed validation and Native construction, separate from the shared #995 replay-policy defaults. The WAL cap bounds an optional tracking interval, and the dirty-index cap bounds distinct coalescing keys; neither limits accepted journal/database size. Operators can supply other finite values through that owner configuration. Qualification must record the actual values and demonstrate the supported long path with defaults; measured calibration may change these implementation defaults through normal issue review, not a new user clarification or a weakened acceptance case.

The existing `sqlite-admission` component meters connections, cursors, blobs and operations; it does **not** already provide a physical byte quota. Implement the next-quantum byte-envelope check and attributable file/sidecar accounting in the Native storage owner, while retaining the original handle scopes. Do not claim that connection admission proves disk admission. The maintained operation/growth profile must give a conservative write envelope before a quantum; absent that proof, drop optional tracking before execution. Actual disk-full/uncertain write outcomes use the existing capacity/receiving protocol. Index-cache size starts at the existing 512 KiB Native convention; spilled index files and rollback overhead still count separately.

Do not silently make another full baseline later in the same attempt, run an uncharged background harvester, force a checkpoint below the shared threshold, or reject an otherwise valid archive as incomplete. This is a resource-specific loss of optional recovery coverage. It must be observable and accounted, and normal qualified long/unknown-length workloads must still demonstrate at least two real default-policy captures with the admitted envelope. It cannot become a blanket excuse to suppress the supported retained family.

On recovery, the same composed accounting root survives. A valid latest selected generation can start a fresh tracked interval after restoration and any ambiguous candidate outcome is settled. Falling back below the accepted tip disables new promotion for that resumed attempt; it must not inherit state from a later, rejected or corrupt branch. The reference `begin(base=...)` enforces one linear accepted tip and one unresolved candidate.

## 4. Dependency persistence, selection and failure windows

Candidate object and root references are updated in bounded transactions in the private retained store. A building root is pinned before any staged dependency can be reclaimed. Completed envelope persistence makes a candidate **ready**, not selected. Only the owner Core transaction can select its exact descriptor and settle the grant/count. All shared-catalog transactions remain short.

| Interruption/failure | Required outcome |
| --- | --- |
| Before grant | No optional store/encoding/scan work. Continue semantics. |
| After grant, before store open or after a partial constructor | Keep the original construction/connection handle; settle known cost or conservatively receive unknown cost; do not forge closure. |
| During page/node staging | Candidate remains building and unusable. Prior selected root is intact. Receive and reclaim the abandoned candidate after worker/selection status is known. |
| Before/inside envelope commit | Core has not selected it. A torn/rolled-back envelope cannot produce a usable descriptor. Preserve the predecessor. |
| After ready persistence, before Core select | Candidate stays pinned. Resume uses the previous selected frontier unless the exact owner outcome proves selection. |
| Source/worker/lease changed at select | Refuse selection under the current fence; receive the rejected candidate and grant. No older authorization is reused. |
| Core commit succeeded but response was lost | Query by original grant/generation identity. Do not retry as a new grant or delete the possibly selected candidate. |
| After selection, before Native receives it | A ready pin protects it until reconciliation. Select/count/accounting are already authoritative in Core; never substitute local candidate status. |
| During later scratch mutations | Selected page objects and map root are unaffected. A mutable scratch file is never a retained generation. |
| Newest optional dependency missing/corrupt | Preserve the diagnostic; try an older selected complete descriptor. Shared corruption may force replay. |
| Exact source/history/algorithm/authorization differs | Do not resume the candidate. A currently authorized exact source may be fully revalidated; required source/custody failures remain errors. |
| Cancellation with open reader/worker | Retain reservations/pins and join/receive originals. A cancel request, timeout or cleared Python reference is not positive receiving. |
| Disk full or store synchronization failure | Never select an incomplete candidate. Preserve prior root and original error/cost; use valid replayable execution where possible. |

The reference tests include an actual child process exiting after durable partial staging. That establishes the tested process-exit case on this Linux runtime; it is not OS-crash/power-loss evidence and does not test Core's production transaction.

## 5. Restore, terminal results and fallback

Read the expected descriptor from the owner's selected frontier, not by discovering the newest local manifest. Verify store/generation identity and envelope digest against it; compare exact input/history/configuration/algorithm binding. Open the selected root under an admitted live reader pin. Walk it in page-number order, validate every node/page digest, node depth/count and complete contiguous extent, and stream into a newly admitted private scratch DB. Never mutate the object store's selected images.

Validate the restored SQLite/Native schema and state, prefix/continuation, graph runs, active child/quarantine references and terminal invariants before allowing advancement. Recheck current source/worker authority before returning a live executor/result. The manifest describes captured state, not continuing permission. Include this full validation and reconstruction cost in the replay-saving estimate; an expensive restore can make a candidate uneconomical.

Keep at most the policy's selected usable roots (normally two), plus expressly admitted temporary candidate/reader/worker pins. A selected root is an independent logical snapshot, but shared storage is not a second physical archive copy. If all optional roots fail, replay from the verified mandatory anchor. Report which failure and fallback occurred; do not swallow archive/intent/receipt/custody corruption in a broad `except Exception: restart`.

At semantic completion, freeze the live disposable DB for independently owned result readers and persist the existing required exact semantic outcome. Do not force a full final snapshot simply to preserve an obsolete `open_retained_result` implementation. Missing derived result state later causes bounded rebuild/revalidation of its exact source under the shared owner/executor; status reads do not perform that work. Reuse a successfully rebuilt view within its admitted lifetime so each member query does not independently rebuild the entire journal. This contract cut must accompany the callers/tests, while mandatory captured evidence and outcome/custody records remain durable.

## 6. Reclamation and full lifecycle growth

Each new immutable node acquires one reference to each nonempty child. Generation/candidate roots acquire root references; actual readers hold original generation pins. When a root/object loses its last incoming reference, enqueue it for bounded reclamation. A collector processes at most its admitted number of queued objects, checks that a queued object is still unreferenced, deletes it, and decrements its at most 16 child references. A revived object is skipped. Every created edge is acquired and ultimately received once per physical object lifetime. No whole-live-tree scan is required on each retirement.

Delete retired generation metadata after known outcome and pin receiving, retaining only the owner's bounded accounting/receipt state. Use the existing original grant identity for the production generation identity; encode any associated lifetime sequence with Riverhog's canonical arbitrary-precision nonnegative scalar convention, not a fixed-width SQLite counter. Never recycle a grant or selected descriptor. The prototype uses a SQLite INTEGER sequence for its finite fixtures and preserves those IDs after retired rows are removed; this is not the production unlimited-lifetime representation. Free pages may be reused within the retained database; bound peak allocated space and charge the final owner deletion. If platform-specific incremental vacuum is needed for physical space return, admit bounded steps and measure their actual movement/sync cost; do not run periodic full VACUUM or whole-store rehash as an unmeasured default.

Let `P0` be pages in the first admitted capture, `Di` the distinct changed/deleted page keys in later capture `i`, `Fi` the WAL frames inspected for that capture, `Ri` all per-page tombstone updates across intermediate shrink commits, `B` page width, `G` completed/failed candidate attempts, `Pr` pages restored, and `E` the actually created immutable child edges. The application-level bounds are:

| Work | Required bound/interpretation |
| --- | --- |
| Initial state bytes captured | `P0 * B`, once per newly admitted baseline; not once per generation. |
| WAL input inspected | Proportional to `sum(Fi * (B + frame_header))`, including checksum/coalescing passes and rejected tails. |
| Dirty-page index | `O(sum((Fi + Ri) * log(1 + Di)))` indexed work and `O(Di)` disk metadata. Count repeated intermediate truncation updates even when final extent regrows; a tiny WAL can truncate a large pre-existing freelist. |
| Page images written/hashed | At most `(P0 + sum(Di)) * B` before deduplication; tombstones have fixed map work. Count attempted/rolled-back work as well. |
| New radix nodes | At most `8 * (P0 + sum(Di))`; each has 16 children and fixed header width. |
| Map path work | Constant-depth node/child work per change. Count all existing-node verification, SQL lookups and intermediate nodes. |
| Control encoding | Once per admitted complete candidate attempt, `sum(control_size_i)`; not per semantic row/yield. |
| Direct restore | One traversal of the selected closure, at most eight nonempty map nodes per page plus page bytes and Native/SQLite restore verification. No factor for historical generation count. |
| Reclamation | Proportional to created/retired objects and `E`, in bounded quanta; include failed candidates, intermediate nodes, pins and final deletion. |
| Memory | Fixed-depth traversal/page buffers plus admitted bounded batch/spill cache; no all-history/page map in Python memory. |
| Live retained space | Selected roots plus admitted candidate/pins and bounded queued garbage; throttle/refuse optional work if receiving cannot keep up. |

These are logical algorithm bounds. SQLite indexed lookup/update work has a B-tree factor in live object count; physical pages, VM instructions, synchronization, journaling amplification, storage stalls and locking must be declared/observed separately. Do not call the entire backend flat linear I/O merely because map depth is fixed. This extra indexed cost is owned and justified by checkpoint lookup, not imposed as a changed archive traversal bound. Existing declaration/delegate evidence must cover the actual maintained primitives.

The policy cost `C` includes failure-free candidate setup, creation-time consistency validation, source/WAL inspection, spill, object/node writes and hashes, all creation commits/syncs, selection/coordination, failed optional attempts and retained retirement tail. Future restore/load/revalidation is excluded from `C`: subtract its conservative estimate once from saved replay `S`, and report its actual cost separately on interrupted runs. Useful-time accounting cannot silently absorb optional housekeeping. Measure result latency and final resource quiescence separately. A grant enforces deterministic admission against estimates; it cannot guarantee storage wall time under arbitrary stalls.

## 7. Implementation limits and integration requirements

The reference store directly opens SQLite and paths; its pin tokens are a protocol demonstration, not retained-resource receiving. It does not implement Core grants, authorization, an installed filesystem backend, production cancellation, quota admission, offline recovery or a published format. Its small synthetic page tests and actual SQLite reconstruction prove the stated mechanism only. The WAL extractor has a separate same-runtime test scope; the Native conductor experiment is also separate.

The integration agent should implement the same selected design in Native-owned modules, use admitted original connections and resource scopes, and carry the envelope through the typed #995/#997 owner interfaces. Add tests to the existing package and qualification discovery rather than inventing a new maintained developer command. [QUALIFICATION.md](QUALIFICATION.md) fixes the required concrete counterexamples and first real workload; [VALIDATION.md](VALIDATION.md) states what this reference actually ran.
