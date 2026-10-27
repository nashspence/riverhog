# Native Windows prepared-state backend

Proposed mechanism for #982, against `8c94eb3e5e349147d46b8f10fc25e161cb7106be`.
The [handoff](HANDOFF.md) is the reading entrypoint; [research](RESEARCH.md) owns
external source attribution. This document specifies proposed integration behavior.

## 1. Decision and supported boundary

Use the locked Python runtime's SQLite engine and its normal locking Windows VFS
for a **per-construction, application-owned database on local NTFS**. Use
`journal_mode=PERSIST`, `synchronous=EXTRA`, no attached databases, and bounded
transactions. Retain the present POSIX file backend. Factor the logical byte-store
interface from `_prepared_primary_posix.PrimaryFiles`; do not duplicate producer,
receipt, completion, inventory-authority or retry semantics by platform.

This deliberately changes Windows's private physical representation. A member
namespace is a database key, not an NTFS directory. Complete prepared bytes,
controls and their logical names live in the same database. There is no SQL
manifest pointing at separately published durable payload files. That hybrid
would retain the unresolved cross-file/directory commit problem.

Proposed initial qualification: native CPython on the existing `windows-2025`
runner and a non-elevated Windows 11 x64 NTFS environment. Exact Windows build,
SQLite source ID, VFS, disk/driver and flush conditions belong in evidence.
This is not qualification of every Windows release or filesystem. Reject UNC,
SMB/mapped network drives, reparse/cloud-placeholder paths, removable/FAT/exFAT,
ReFS and nonqualified filter/filesystem configurations for this initial backend.
Reject unsupported placement explicitly before remote mutation; do not fall back
to memory, disabled sync, another directory, or POSIX-only producer behavior.

Storage must honor successful flushes. Neither this design nor SQLite can make
lying hardware, media loss, malicious kernel/admin writes or a deleted whole
allocation recoverable. Missing/corrupt state remains unavailable, never fresh.

## 2. Why this mechanism

SQLite's persistent rollback mode commits by invalidating and syncing a journal
header after database writes, rather than deleting a journal name. Its Windows
VFS uses `FlushFileBuffers`; its `xDelete(syncDir)` does not implement a POSIX
parent-directory fsync. `EXTRA` alone is therefore not a directory-flush solution.
The chosen combination removes normal publication's reliance on directory-entry
removal and puts atomic selection/counters in an established transaction engine.
See research S1-S4.

WAL with FULL synchronization is a viable transaction design, but is not selected:
checkpoint scheduling, retained readers and WAL recovery introduce additional
lifetime/budget work here without needed multiwriter concurrency. DELETE journaling,
OFF/NORMAL synchronization, TxF, privileged volume flush, a guessed directory
`FlushFileBuffers`, and a mechanical `os.replace` port are not the implementation.
No custom SQLite pager or private NT syscall durability contract is proposed.

## 3. Native placement, privacy and ownership

### Physical layout and bootstrap

Expose an explicit platform storage location through the owning application:
POSIX directory placement versus Windows database-file placement. A factory may
select the platform implementation, but must not guess a retained format from
missing files. Reconcile all public exports/callers in one pre-v1 cut.

On Windows use a deterministic ASCII basename derived from a length-framed
application/allocation/scope descriptor, in an **already-existing durable local
parent**. The interactive default may use the current user's existing LocalAppData
known folder directly; do not first manufacture an unflushed nested application
folder. Services can use an explicitly provisioned existing private local parent.
Refuse a missing parent instead of recursively creating it. This is a visible
placement prerequisite, not a claim that file flush durably creates an arbitrary
parent chain. Ordinary allocation and member creation then creates database rows,
not new physical directories. Validate any supplied parent, including the default.

Use fixed derived names for the container, its `-journal`, and permanent owner
file. Keep original allocation/scope identity in the database and compare it
against caller authority; a filename hash alone is not identity. Do not interpolate
artifact/source names, credentials, network locations or arbitrary SQL identifiers.
A location must be stable across retry; no automatic relocation on failure.

For explicit fresh initialization, before any remote call:

1. Pin and validate the existing parent path and security as below. Create the
   owner file with `CreateFileW(CREATE_NEW)`, noninheritable handle, read/write
   access, share mode zero, and an explicit private security descriptor. Flush
   it and retain the handle; never use delete-on-close.
2. Exclusively create the database file (`CREATE_NEW`), with the same privacy and
   a compatible read/write, no-delete sharing guard. Flush the new file; retain
   its identity guard. Open SQLite in read/write **existing-file** mode, without
   an implicit create fallback. Reject any unexpected pre-existing sidecar.
3. Execute the explicit StateSchema initialization described below with the
   required SQLite settings. Commit the allocation identity, schema/format,
   storage settings and initial controls. Do not publish a directory/rename
   candidate to install the database.
4. Commit a real small bootstrap/barrier update and re-read its exact identity.
   Confirm journal privacy/identity and database/parent handles. Only successful
   transaction synchronization permits initialization to return. An empty
   transaction or close/reopen alone is not a durability barrier.

Initial file-name persistence relies on SQLite's supported new-database contract,
Windows file/metadata flushing and the admitted NTFS storage stack (S1-S5); it is
not inferred from a nonexistent directory-fsync primitive. Qualify first-container
creation through abrupt machine reset as well as process exit. Unlike a per-member
file protocol, this namespace bootstrap is not repeated for each object.

A partially initialized file, a missing expected owner/container, unknown format,
unversioned nonempty database or allocation mismatch is an error. Never delete it
or initialize a replacement on ordinary resume. Explicit initialization cleanup
may be considered only by the application after proving no publication was begun;
retain evidence if that cannot be established. First-session-create ambiguity
remains the existing fail-closed contract.

### Native checks, not chmod emulation

Use wide Win32 APIs. Check each path component with no-follow/open-reparse-point
semantics and directory handles (`FILE_FLAG_BACKUP_SEMANTICS` where required).
`FILE_FLAG_OPEN_REPARSE_POINT` on the final file alone does not protect ancestors.
Pin traversed directories against deletion/rename; use trusted ancestry and
handle-based final-path/volume/file-ID validation. Reject reparse attributes,
non-NTFS/remote volumes, unsupported namespace forms, and multi-link database,
owner or journal files. Check parent permissions against untrusted create/delete-
child/rename access; canonicalizing a path once is not a security boundary.

Create protected DACLs granting the owning user/service SID and SYSTEM the
required rights, without permissive inherited grants. Verify owner/DACL on resume
and every newly created durable sidecar before it can contain preimages. Require
private sidecar inheritance from the admitted parent (or an equivalent
qualified native creation policy) before SQLite can create a journal; do not write
first and repair its ACL later. Engine recovery may remove/recreate a journal
before connection settings take effect: preserve that safe creation policy on
every open, and never prevent legitimate engine recovery with a no-delete guard
on the journal. Revalidate sidecar identity at that known boundary, not by blindly
accepting arbitrary replacement during an active transaction. Same-principal
malicious processes and administrators are outside this
private-owner threat model; ordinary competing producers still must be excluded.

Hold the permanent owner file with share mode zero for the **entire construction
execution**, not merely during SQL writes. A sharing violation returns Busy, not
an age-based takeover. Sharing is OS-handle authority, not timestamp authority.
Pin database identity with a separate handle compatible with SQLite read/write
opens but not delete/rename. Do not block the SQLite engine's legitimate writes
with a share-zero handle on the database itself.

Use one serialized connection/connection owner per live store and one local mutex;
no concurrent calls on that connection. Children/readers share an owner token
with a generation and closed/poisoned state. Closing the root invalidates all
children. Check handle IDs, file kinds/security, database generation and logical
object generation at operation boundaries. Handles are noninheritable, including
spawned test/worker processes. SQLite's own locks supplement this ownership; they
do not substitute for ownership between transactions or during network requests.

Close readers/cursors/connections and actually join all payload/heartbeat workers
before releasing native ownership. Cancellation, timeout, connection rollback,
thread exit or an abandoned request does not prove those workers stopped. Keep
independent allocations on separate locks and databases.

## 4. Current-only schema and transaction discipline

The database is operational state owned by its calling application. Shared schema
code belongs with the prepared-store owner, not Riverhog's archive/database or a
new coordinator. Add the declared `state-schema` dependency, packaged migration
resources and built-install tests to the owning package. No runtime downloaded DLL,
unlocked Python replacement, generic server database, or global allocation index.

Initialize using an explicit StateSchema upgrade on a caller-owned connection.
Supply a backend-specific engine factory: the existing `state_schema.sqlite_engine`
forces WAL and a 30-second busy timeout, so it cannot be used unchanged here;
its other consumers must keep their existing behavior. On this backend,
ordinary startup checks revision and exact expected schema without upgrading or
erasing it. Reuse the owner's bounded schema-shape verifier with StateSchema's
status/revision machinery. Retain full `StateSchema.validate()`/upgrade integrity
checks in explicit initialization, maintenance and qualification. The current
`_verify_database` runs `quick_check` and `foreign_key_check`: invoking it on every
point operation or cold receipt would scan unrelated data. Do not weaken that
existing method globally or claim a structural startup check is a full audit.
Preserve interrupted and future-supported upgrade tests and all nonrebuildable data.

Settings are set/read back on every connection **before writes**:

- `journal_mode=PERSIST`; `synchronous=EXTRA`; `busy_timeout=0`; foreign keys on.
- Fixed page/chunk geometry in the private format; page cache explicitly budgeted,
  memory mapping off, no auto WAL checkpoint, extension loading disabled.
- `auto_vacuum=INCREMENTAL` established while empty, not FULL auto-vacuum.
  No ordinary `VACUUM`, whole-database copy, or rebuild-on-open.
  Keep SQLite scratch/temp storage inside the admitted privacy/resource boundary;
  do not spill sensitive values to an unqualified system temp directory.
- Journal size retention includes its high-water allocation; no routine switch to
  DELETE and no manual removal of a possibly hot journal. All connections,
  including migration helpers, use the same durability policy.

Use existing locked runtime SQLite only after recording its actual source ID,
compile options and VFS. CPython 3.12.3's upstream Windows recipe names SQLite
3.45.1, but this does not prove the mise-delivered binary uses exactly that build.
Validate the installed runtime; reconcile any difference under dependency policy.

A minimal logical schema has these indexed roles (names illustrative):

| Role | Key/content | Purpose |
| --- | --- | --- |
| format/allocation/barrier | Singleton | Exact owner identity, layout parameters, durable open barrier. |
| namespaces | Parent key + validated component, unique | Logical equivalents of `child`; no physical member directories. |
| controls | Namespace + role, unique | Original canonical control bytes, separately from large preimages. |
| objects | Immutable generation ID | Role, expected byte count/hash, committed extent, selected/retiring state. |
| chunks | Generation + ordinal, unique ordered key | Bounded byte segments; no single member-sized BLOB. |
| names | Namespace + role, unique -> generation | Atomically selected immutable byte object. |
| backend operation | Singleton with generation/cursor/phase | One resumable candidate publication or cleanup, not a workflow queue. |
| ordering scratch | Generation + opaque artifact ID | Recomputable sorted owner input; never authoritative membership. |

Keep the existing canonical `PreparedPrimary`, receipt proof, decision ack,
usage/transition and closing controls. The original manifest seal remains checked
on reads and is never minted to bless changed data. No Native/archive/protocol
serialization changes follow from the storage choice.

Bound transactions by declared chunk rows **and actual dirty pages/journal bytes**,
including index splits/overflow/free-list work. An admission limit on `executemany`
row count alone is insufficient. Start with small fixed chunk batches, qualify a
conservative page bound for the selected schema, and include that physical grant
in admission. Do not hold a transaction across an observer, remote request,
worker wait or arbitrary caller iterator. Materialize only an admitted batch,
then begin/commit it. No unbounded busy retries in coordination code.

## 5. Publication and recovery algorithm

All operations are under live construction ownership. Normal immutable controls
are one bounded `INSERT` transaction. A duplicate compares the original exact
bytes; different content conflicts. Never use SQL `REPLACE`, an upsert, or
`INSERT OR IGNORE` that silently accepts a different winner. Only already-mutable
usage/transition/cursor controls use an explicit compare-and-update generation.

For byte publication:

1. Reserve logical/physical grants and durably identify the one candidate generation
   in `backend operation`. Bind its destination, role, admitted maximum bytes,
   supplied expected length/hash when available, and original replace/no-replace
   mode. Final measured length/hash are mandatory before selection even when the
   source did not supply them. A losing candidate is never remote authority.
2. Stream bounded chunks; each transaction commits those rows plus the exact
   progress offset. Hash complete bytes as produced. Empty objects have explicit
   zero-length commitments. Verify supplied expected length/hash before selection.
3. In one small transaction mark the object complete and insert its `names` entry,
   or compare-and-switch an explicitly mutable target. The selection points only
   at prior successfully committed chunks. For a mutable byte-object replacement,
   atomically retain the displaced generation in the backend cleanup cursor; do
   not lose its reclamation identity when switching the name. Defer its deletion
   while readers pin it, then delete in bounded steps. Clear or advance the
   backend-operation record atomically. Commit before returning the byte handle.
4. Publish the existing sealed prepared manifest only after **all** referenced
   objects have passed these barriers. Finish the existing store usage transition.
   Only then may the shared publisher register/stage/bind remotely.

A crash before step 3 leaves no selected name. The singleton operation names its
candidate and cleanup cursor; remove only its committed chunk ranges in bounded
transactions. A crash after step 3 retains the selected generation even if the
response was lost. Read that exact winner; do not remove it as an orphan. Cleanup
must not find candidates by a whole-table scan, glob, timestamp or naming guess.
Keep at most one unfinished backend mutation per allocation to bound restart work;
new mutation pauses while that specific work is resolved.

A successful SQL commit is a local durability acknowledgement. Any write, flush,
commit, ACL/identity or journal error poisons the active mutation: report failure,
perform no dependent remote mutation, and retain the original state. Close/reopen
under ownership, let SQLite recover its own journal, then reconcile the exact
operation/name/control. A failed commit has an uncertain outcome, not permission
to author a replacement. Before promoting a visible uncertain winner, commit a
real small barrier update successfully. Never infer durability from a cached row,
a read-only transaction, or a successful later `close()`.

Backend `sync()` means all preceding mutations are successfully committed and the
owner is current, not a no-op after an error. After an uncertain outcome/open it
uses the explicit barrier above; after known successful commits it need not
perform another fsync on every read. No physical directory fsync is simulated.

Strict replay checks the complete original bytes before exposing a replay reader.
Use indexed chunk range iteration with bounded buffers, verify length/hash, and
pin that immutable generation while readers exist. A seekable chunk reader must
finish each SQL cursor/transaction before handing bytes to network code. Prevent
reclaim or replacement of a pinned generation; the shared owner and local reader
pins provide stability without a long SQL snapshot across network waits.

## 6. Receipt, reclaim and terminal lifecycle

Preserve the implemented #981 ruling: `load_controls` and receipt acceptance
validate original sealed controls plus the complete authenticated receipt, not
pending preimage contents. Store the compact original receipt proof in its own
transaction, compare the full primary/member/session/completion identity and
repeat-receipt identity, then acknowledge safe custody. A database checksum is
not remote authentication. Keep separate materialization-decision acceptance.

No decision acknowledgement means no prepared-byte reclaim, even with valid
custody. Once both predicates are durable, the existing reclaim transition names
that member and its original total reservation. Backend cleanup removes that
member's selected byte generations in bounded ordinal ranges; each transaction
atomically advances its deletion cursor. Do not cascade-delete an arbitrarily
large BLOB/chunk subtree in a single SQL statement. Until deletion completes,
charge the unreclaimed allocation and return pending rather than early success.

Only after all prescribed role bytes are removed does the shared owner install
`reclaimed` and complete its before/after usage transition. Use the original
committed lengths, not current damaged/truncated contents. This preserves once-
only subtraction. Keep `remove` idempotent and reconcile its backend operation
before advancing a higher-level cursor. Evolve the adapter/store cleanup seam to
expose a bounded continuation where needed; a synchronous compatibility wrapper
may only drive it in execution work, never a status/coordinator call.

SQLite free pages are reusable capacity, **not automatically returned disk space**.
Track logical pending bytes, reusable pages, database/journal physical high water,
control grants and scratch separately. Schedule `incremental_vacuum(k)` in bounded
maintenance steps, charge actual page moves/writes and preserve the control-page
reserve. Do not run full `VACUUM`, truncate the database yourself, or perform a
whole-store shrink on each receipt. Report physical capacity release only after
it actually occurs; leftover journal/unused pages stay private operational data.
Logical deletion is not a promise of forensic secure erasure.

Keep compact original manifest/receipt/decision/usage controls until the application
settles its allocation. Terminal disposal is a backend-aware, owner-authorized
operation, not `rmtree` on a guessed directory. A retired record in a retained
small database prevents accidental reopening. Physical deletion of the container
is permitted only when the application independently retains terminal disposition
so absence cannot mean fresh state; otherwise retain the compact container. Close
and quiesce SQLite before deleting the container and its paired journal. Never
remove a hot journal while retaining a live database. A failed/deferred delete is
pending cleanup, not recovered disk capacity or permission to recreate the job.

## 7. Resource and growth declarations

Let D be bytes prepared/replayed/reclaimed, M indexed stored records/chunks, B the
admitted chunk size, K the transaction chunk allowance, Q its verified dirty-page
bound, C selected control bytes, R complete receipt bytes and N member count.

- Useful-byte hashing/copying stays O(D). Indexed writes have an explicit B-tree
  bookkeeping term (worst-case O((D/B) log M)); ordered traversal has initial seek
  and range/page costs. Do not mislabel every SQL operation strictly O(1).
- Selected receipt processing is O(C + R) byte work, with indexed lookups and
  bounded durability work, **zero pending-preimage content reads/hashes**, and no
  member scan. Keep controls in separate tables/pages from large byte objects.
- Heap adds bounded connection/page cache and O(KB) batch/reader buffers, not an
  all-member dictionary. The existing whole-member Native authoring working set
  is unchanged and remains separately measured/admitted under #981.
- Hot rollback recovery is proportional to the bounded last transaction's journal
  work, not accumulated archive history. Cleanup of committed unselected chunks
  is proportional to those actual chunks, with a durable cursor. Interrupted
  shared preparation can still require its existing complete byte verification;
  label that phase separately from steady receipt reads.
- The necessary N*C compact metadata, SQL/index/page overhead, reusable slack and
  bounded journal reserve are real storage, not a hidden member limit. SQLite's
  BLOB limit must not become a total-journal limit: use chunks. Integer/database/
  filesystem capacity exhaustion must explicitly reject/defer before accepting
  additional work, not omit data or claim the archive has a new support ceiling.

Extend operator policy to cover physical page/journal/cache/transaction grants;
the present `PrimaryStoreLimits` counts logical bytes, not SQLite overhead. Set
and validate `max_page_count` from the physical grant without pretending it covers
the journal. Reserve already allocated database free pages for receipt, decision
and progress controls; account for worst-case index splits. Prepare/replenish a
persistent journal high-water reserve through valid bounded SQLite work before
admitting publication, not raw writes to a journal. New payload batches must not
consume those reserves. Keep the maximum dirty-page grant small and checked.

At logical saturation, reserved controls and reclaim must progress. At actual
media failure, unexpected ENOSPC, denied flush or exhausted physical reserve,
fail safely and retain state; never promise progress on unwritable hardware.
A reservation/configuration number is not evidence the filesystem allocated it.
Measure allocated bytes, journal reuse/growth and scratch, with cold caches and
failure paths. Do not hide db-wide health scans or VFS retry waits in point costs.

## 8. Concrete source integration seams

| Source owner at audited base | Required change |
| --- | --- |
| `_prepared_primary_store.py` direct `PrimaryFiles` import and `_member` | Logical adapter dispatch, explicit placement, preserve shared controls; bounded cleanup continuation. |
| `_prepared_primary_posix.py` | Keep its native behavior; extract protocol without pretending it works on Windows. |
| `_prepared_primary_inventory.py:ordered_primary_inventory` | It directly opens `files.root / selection.sqlite3` with journaling OFF. Replace this physical-path assumption with an adapter-owned ordering facility. On Windows use a separately admitted indexed scratch generation in the same database; durability settings never switch OFF. Scratch is reconstructible, not membership authority. Batch construction and deletion; original inventory framing remains shared. |
| `_prepared_primary_closing.py` | Preserve exact inventory/cursor/chain rules; implement bounded extent reads on chunks and no completed-prefix rescans. |
| `_prepared_primary_documents.py`, `_publisher.py`, `_session.py`, `producer.py` | Keep sealed original controls, exact replay, current authorization and receipt/decision separation. Both producer forms must actually select the Windows backend. |
| CLI `directory_upload.py` | Stop treating directory existence as a universal backend test; explicit fresh/resume authority and stable Windows file placement. Windows bootstrap must not depend on freshly made XDG directories. Original source/member allocation remains owned by CLI. |
| FTP `landing.py`; target `execution.py`, `persistent.py` | Pass application-owned placement and use backend-aware retention/disposal instead of `.primary` directory scans/removal. Do not infer that every surrounding FTP/target deployment is Windows-qualified merely because the shared store is. |
| `state-schema`, client `pyproject.toml`, lock/build/install selection | Explicit schema/migration dependency, exact-current opening and packaged migrations; no import of app/server implementations. |
| `scripts/ci_qualification.py`, native platform workflow, store tests | Parameterize behavior tests over native backends; distinct POSIX syscall tests may still skip on Windows, shared recovery behavior may not. |

These are routine integration cuts for the selected mechanism, not another design
study. Preserve the current POSIX source and durable state; do not create readers
for abandoned Windows formats or migrate user data silently. Reference publication
adds no protected-contract change, release/provider authority or test waiver.
