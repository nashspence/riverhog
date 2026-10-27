# Selected SQLite resource-admission contract

This is a proposed focused revision for #982, not implemented qualification.
The [handoff](HANDOFF.md) pins source, scope, provenance and revision identity.
Primary sources and inspected functions are mapped in [RESEARCH.md](RESEARCH.md).

## 1. Decision: enforce the envelope, do not guess the engine's worst case

Select the request's **explicit enforcement mechanism** alternative. Preserve the
locked SQLite library, current single-container schema ownership, PERSIST/EXTRA,
normal Windows locking/flush implementation, private NTFS owner and shared producer.
Add one private native VFS resource gate, bound to that same loaded engine, and
an application-owned aggregate SQLite heap policy. No second database, engine,
storage process, WAL, custom page cache or allocator replacement is selected.

The first reference asked for a small checked dirty-page bound without supplying
its proof. This revision replaces that ambiguous scalar with three independent
limits. They are operator resource grants, not measured performance maxima:

* `Qio`: maximum distinct database pages submitted to the native writer in one
  forward transaction. Include attempted delegations that return failure.
* `J`: maximum active rollback-journal write extent, with separate total-byte and
  callback-count grants. Retained EOF, allocation and active write extent differ.
* `H`: application-process ceiling on **SQLite-accounted heap** in the exact
  loaded library. It is aggregate, not a per-construction cache reservation.

For the default heap-backed page cache, no memory mapping or external page-cache
pool, resident dirty frames satisfy `Qresident <= floor(H/P)`, because each such
frame contains at least one `P`-byte buffer charged to SQLite. This is not a bound
on the cumulative distinct pages dirtied during a transaction. `Qio` alone does
not bound memory; `cache_size` remains tuning, not an enforcement mechanism.

### Success and rejection are deliberately distinct

A successful operation must fit all grants. A structural operation exceeding a
grant fails/defers with original state retained, not with truncated output. A
larger B-tree, fragmented freelist, overflow chain or pointer-map update cannot
escape the gate. This proves a resource envelope for delegated work, **not that
all such operations succeed under a particular small quota**.

Fixed already-allocated control writes have the derivation below. Structural
allocation, selection, deletion and vacuum use the checked envelope and may
require an explicit operator grant increase. Never retry forever with the same
known-insufficient profile, silently enlarge it, or claim cleanup/physical release
while a cursor is pending. Existing confirmed custody remains confirmed even if
local structural cleanup is deferred. This is the explicit liveness trade-off
relative to the old blanket statement that reclaim always progresses at saturation.

## 2. Fixed-control bound and readiness minimum

Let `P=4096`, `U=P-reserved_bytes`, `S` be the actual native sector size, and
`A=max(1,ceil(S/P))`. Admit only qualified power-of-two geometry; do not invent
sector size or powersafe-write capabilities. For fixed BLOB extents whose written
prefixes have lengths `L1...Lm`, use the conservative bound

```
Qcontrol = A * (1 + sum(2 + ceil(Lj / (U-4))))
Jcontrol = 2*S + Qcontrol*(P+8) + 16
```

Derivation: incremental BLOB writing preserves record length and topology. Each
written contiguous prefix touches at most one leaf plus an overflow chain; each
overflow page has `U-4` payload bytes. The extra page in `2+ceil(...)` covers an
arbitrary starting offset in that chain. Add page one for the change counter;
multiply by sector peers conservatively for journal before-images. With one
main database, no savepoints/ATTACH, no dirty-cache spill, and normal rollback
journaling, each old page needs at most one main-journal record of `P+8` bytes.
Two sectors and 16 bytes conservatively cover header/alignment/tail handling.
Enforce the journal extent independently even when using this formula.

The inspected implementation anchor is SQLite `sqlite3BtreePutData` ->
`accessPayload` plus pager `pager_write`, `writeJournalHdr`, `syncJournal` and
sector journaling. No split/merge, index-key, freelist or pointer-map change occurs
in this profile. Tree depth and fragmented overflow placement still add read/seek
cost; they are not constant-time lookups. This profile excludes structural SQL,
triggers, resizing the BLOB and changing indexed columns.

Examples at 4096-byte sectors/reserved=0: warming an 8 KiB control plus its 9-byte
header and the existing 16-byte reservation field yields `Qcontrol=9`,
`Jcontrol=45144`; 256 KiB yields 71 and 299592. These are derived conservative
examples, **not recommended production defaults or maxima from the four fixtures**.
`envelope.py:control_bound` is the executable arithmetic anchor.

For each existing control mutation, sum exactly the extents touched in its one
transaction. `put` changes one fixed control; `_warm` also rewrites the 16-byte
reservation; readiness changes its fixed record and the ready byte. Do not add
all controls in the construction merely because they exist.

**The current integer `primary_format.barrier=barrier+1` is not fixed extent:**
SQLite integer encoding can grow. Either classify it as structural, or make the
current-only schema change to a preallocated 8-byte BLOB counter and update it
incrementally. Select the latter for the progress/open barrier. Keep overflow
rejection, StateSchema initialization/upgrades and exact allocation identity.
Add a fixed-size versioned backend resource-profile/phase control in the same
owner, not a separate workflow registry. Its own writes enter the formula.

## 3. Native gate and proof obligation

The new production boundary is a private native extension under the shared client
storage owner. It wraps the **already loaded** runtime SQLite Windows VFS, using
its public API. Compare source ID and loaded module identity with stdlib SQLite;
never load another SQLite DLL by search path or ship another amalgamation. Keep
all file names/URI metadata, buffers, real native handles, lock calls, xSync flags
and native results intact. Explicitly select this VFS for every container open,
including StateSchema factory connections before the first pragma/schema read.
The existing test-only ctypes interposer is an experiment aid, not production code.

Before entering SQLite allocate fixed-capacity page-ID tables and counter banks
for forward and recovery work. Native callbacks perform no growing allocation,
Python callback, logging I/O, network operation or collection scan. Capacity
exhaustion rejects before delegation; no silent loss of tracked pages. Use
checked signed-offset arithmetic and count all pages intersected by a write.

For every `xWrite`, check the candidate union of database page IDs, maximum end
extent, total attempted byte count and operation count **before** invoking the
native method. Charge the whole attempt before calling it; an IOERR may have
partially written, so do not refund it. Journal offsets get their own absolute
cap; rewrites consume total-byte/call quotas even when EOF does not grow. Zero-
length writes do not manufacture a huge end extent. Unadmitted other-file writes
fail, rather than spilling into unbudgeted temporary storage.

By induction over calls: the empty prefix fits; the next native call is permitted
only if the entire new prefix fits; failure cannot remove its charge. Therefore
every delegated prefix respects Qio/J/byte/call bounds regardless of which internal
B-tree, overflow, freelist or pointer-map code produced the request. This is the
structural-work proof. Counting only successful calls, as in the existing diagnostic,
is insufficient for this enforcement purpose.

Gate **all** ways to change storage, not just xWrite: xTruncate growth and shrink,
xOpen file classes/creation, xDelete, and allocating xFileControl requests such as
SIZE_HINT/CHUNK_SIZE. Forbid mmap, WAL, ATTACH, external temp files and alternate
atomic-write bypass paths in this backend profile; return explicit errors rather
than successful no-ops. Forward admitted operations unchanged. Allocation rounding
must fit the separately granted NTFS allocation-cluster allowance. Existing
Win32 retries remain visible and bounded by the native policy, not hidden as
one cheap logical call. Instrument read bytes/calls too; quota-limited point work
must not turn into an unbounded scan or sort. Preserve native durability errors.

A profile is armed before BEGIN, not just COMMIT. It remains armed through commit,
implicit rollback, close and error handling. It names the operation class and
original allocation, permitted files, Qio, J, database backed ceiling, read/write
and call grants, and a reserved recovery bank. A class cannot switch while the
transaction is uncertain. No engine call with mutable storage may run unarmed.

## 4. Rollback and cold-open accounting

**Never spend the forward budget twice or disable the gate to roll back.** SQLite
can start rollback before returning the failing call to Python. The first quota
refusal/native failure atomically latches the connection as poisoned and arms
its already-allocated restoration bank before returning the error to SQLite.
No further forward work is permitted. Capacity refusals map to a capacity result;
other native errors retain their error meaning. The exact original outcome is
reconciled on reopen under existing ownership, not inferred from the error code.

Recovery has independent database-page, extent, byte, read and call limits.
A conservative page allowance can use the retained journal capacity:
`Qrecovery = 1 + floor(Jretained/(P+8))`; every complete saved page record consumes
at least `P+8` bytes. Keep an independent total-work cap for headers, repeated
reads/writes and defensive failure on malformed input. Do not equate journal EOF
with the valid record count or parse it as new custody authority. SQLite remains
the journal interpreter. Reject work beyond the recovery envelope, retain the
journal/database and report recovery-blocked; never erase them to regain space.

On cold open use native file metadata and the application's admitted maximum
profile *before* SQLite can read or replay a hot journal. Allow restoration up to
the originally granted database ceiling, not only the possibly truncated current
EOF. If actual retained journal/DB extent exceeds the offered recovery grant,
stop before opening and require restoration of a sufficient grant. Do not first
open unguarded just to read a resource row that itself requires hot rollback.
After recovery validate the retained resource profile and source identity, then
perform the fixed barrier and resume the exact operation/cursor.

A reference real-SQLite experiment denied the third forward database page write;
rollback wrote 66 original pages. This demonstrates why sharing a two-page forward
budget with rollback would be wrong. It is a Linux experiment, not native Windows
qualification. Preparation verification, initialization/open, explicit audits,
rollback and normal receipt work retain separate evidence/cost labels.

## 5. Physical allocation, readiness and replenishment

Keep four independent quantities: logical retained commitments; SQLite live/free
pages; actual database/journal EOF and allocated clusters; and process heap.
A PRAGMA, free-space query or configuration number is not an allocated reserve.

The application resource owner grants database bytes `Dmax`, journal retention
`Jretained`, active operation envelopes, a free-page floor `Fprogress`, and a bounded
reserve-preparation workspace. New payload work cannot borrow these progress
allocations. Fixed control slots for the member/root/inventory, including their
maximum permitted encodings, must exist and be journal-ready before publication.
Keep the already implemented pending-control tag and measured warming evidence.

Use a versioned same-container state `preparing -> ready -> depleted/maintenance`
with exact allocation, engine/schema/profile identity, original grants, backed
DB page ceiling, free-page floor and journal floor. Publish ready only after the
following *actual* barriers, under native ownership:

1. Allocate fixed control extents through the existing bounded preparation path.
   Allocation can cause splits/overflow/freelist/pointer-map work; it is structural
   and may fail before ready. It is not covered by the incremental-update formula.
2. Prepare reusable DB headroom through valid SQL scratch/reserve rows: allocate
   bounded rows, commit, then delete bounded rows without vacuum. Verify SQLite
   free-page count and the guard's allocated-file metadata. Those pages and the
   control extents stay in the owned container, not in an imaginary external disk
   quota. Reserve work has its own charged cursor and scratch capacity.
3. Warm an already allocated dedicated reserve BLOB, or fixed control extents,
   through valid SQLite writes and commits. Advance only to a journal floor whose
   EOF **and actual allocated bytes** were observed after success. Keep a pad large
   enough for the configured maximum progress envelope; never raw-write a journal.
   Warming is subject to a bootstrap envelope too. If a target cannot be backed
   within it, return not-ready; do not record the requested size as measured.
4. Effective runtime journal cap is at most that verified floor. Its minimum must
   cover the derived fixed-control profile. Raise readiness through a separate
   fixed-record transaction whose own journal need already fits. Recheck backing
   after it. Retention cannot be configured below the retained floor.

Readiness is a durable dependency barrier, not a snapshot of disk free space.
The native guard rejects sparse/compressed/reparse substitutions under the existing
NTFS contract. After a failed allocation/flush, leave preparing/depleted state;
resume from the exact resource cursor and verify backing again. Only known reserve
rows may be cleaned. No drive-wide scan, guessed orphan deletion or automatic
container recreation is allowed.

### Protect the free-page floor

For ordinary candidate/chunk/name/order admission, under BEGIN IMMEDIATE require
ready; set the SQLite page ceiling no larger than the already backed database
ceiling for that quantum. Before COMMIT read page_count/freelist_count and require
`end_pages <= backed_pages` and `free_pages >= Fprogress`. A violated condition
rolls back. With cache_spill disabled no dependent publication follows a rejected
transaction. The VFS cap remains active during that rollback.

For fixed-control progress use only existing slots: it needs no new SQL pages.
Structural reclaim/order cleanup may use the explicitly reserved progress grant,
but still faces the hard envelope and cannot imply successful cleanup on refusal.
Replenishment runs in execution, with priority over new payload admission, and
may run independently of an unrelated pending byte candidate using only reserve
objects. Never require successful cleanup to authorize the *resource increase*
that cleanup itself needs. Raising a grant is an explicit owner operation; lowered
grants cannot silently revoke retained reservations or make hot recovery destructive.

Vacuum remains one explicit step, not a receipt/status side effect. Retain the
free-page floor and journal reserve; verify actual EOF/allocation after success.
If shrink/partial failure invalidates a backed ceiling, mark depleted and reconcile
before new payload. Rollback may need to restore the old length after a truncate;
unexpected ENOSPC/failed media is still a safe stop, not a promise of forward
progress on unwritable hardware. Never conflate reusable pages with bytes returned
to the filesystem. Shared custody/decision/cursor and once-only original-length
accounting remain unchanged.

## 6. Heap/cache ownership

The application, not each `PrimaryStore`, owns one `SqliteProcessBudget` for the
loaded SQLite library. Establish its H ceiling once, before container work; reject a ceiling
below current SQLite-accounted use rather than claiming existing memory vanished; account
for all other connections to the same library and reject inconsistent owners.
Use `sqlite3_hard_heap_limit64`, with memory accounting enabled, the default
heap-backed cache, no supplied pagecache pool and mmap disabled. Validate the
exact runtime configuration. Never temporarily change a global limit on behalf
of one construction or label cache_size a hard/private ceiling.

The upper bound is SQLite-accounted heap <= H (plus separately bounded native
shim bookkeeping and application buffers). SQLite allocator/CRT overhead, Python,
Native authorship, other libraries, mapped OS cache and process RSS are not thereby
bounded. Application buffers/active calls still need their existing explicit
admission. This is not whole-member Native preparation memory qualification.

A finite active-connection/operation grant and priority scheduling avoid opening
unbounded idle caches. NOMEM fails the current operation, poisons/reconciles state
as appropriate, and defers new work; it is never permission to disable the ceiling.
The aggregate limit gives safety, **not per-construction isolation or guaranteed
fairness**. If another SQLite user occupies the budget, progress waits for resource
release/a deliberately increased application grant. It does not acquire a new
storage-process boundary silently. Existing consumers in each application must
be reconciled before enabling this application policy.

## 7. Operation mapping and cost declarations

| Owner operation | Admission class and explicit bound |
| --- | --- |
| StateSchema initialize/upgrade | Bootstrap envelope from first open; DDL/index/root/overflow work counts; no ready marker or remote work on failure. Supported migrations remain explicit. |
| Cold open/status and barrier | Reserved recovery bank first; constant-schema validation separately; fixed 8-byte barrier profile after recovery. No whole-store scan disguised as a point call. |
| Control slot allocate/prepare | Structural allocation followed by bounded fixed-extent warming; actual journal backing required. |
| Exact receipt/decision/control update | Fixed-extent formula plus live Qio/J/H gates; zero pending-preimage content reads. Large complete receipts remain separate admitted C+R byte work. |
| Chunk append/immutable selection | Existing single-row transaction shapes, structural envelope including every affected index, overflow, freelist and pointer map. Refusal preserves singleton candidate/original winner. |
| Ordering build/read/cleanup | Admitted key/value sizes; indexed one-row steps; scratch grant retained; read/VM work measured separately from writes; no unbudgeted sorter/temp file. |
| Reclaim | One original chunk/control step; hard progress envelope, independent recovery bank, exact cursor atomicity; capacity refusal remains pending. |
| Incremental vacuum | Maintenance envelope for page move/parent/pointer-map/freelist/truncate; not a one-page-write assumption merely because N=1. |

Controls cost O(C+R) content validation plus index seeks, admitted VFS work and
separate error recovery. Data work remains proportional to bytes actually processed
and attempts, plus explicit B-tree operations. Native callback enforcement uses
fixed-capacity hash/set bookkeeping with a bounded collision policy, not an N-sized
registry or full-store hash. Declaration changes must distinguish successful,
rejected, rollback and cold recovery work. Timing observations are not gates.

No new archive/member support ceiling, weakened identity/custody check, disposal
permission or release authorization follows from this resource policy.
