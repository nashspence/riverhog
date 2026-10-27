# Integration and native qualification

This is a focused proof plan, not performed acceptance. Use the existing
qualification definitions and issue decisions; no separate reference gate or
release permission is created. Read [HANDOFF.md](HANDOFF.md) first.

## First slice

Implement only the Windows location/bootstrap/owner, current schema initialization
and immutable byte-selection transaction first. Use a real non-elevated native
Windows process on local NTFS, not WSL, a Linux SQLite run, or mocks of Win32.

With a fresh database under an admitted existing parent, retain one multi-chunk
object. Terminate the child before and after the selection commit. A new process
must see either no selected name (and the exact cleanup candidate) or the same
complete immutable bytes. Conflicting publication cannot overwrite the winner.
A live second process is busy; after the owner dies, acquisition and SQLite
recovery succeed without a timeout/age-based takeover. Instrument a remote-call
sentinel and prove it cannot fire before durable original-manifest publication.

Then integrate the adapter into existing `PrimaryStore` and run the actual
HTTP/Native stage-before-bind interruption witness, cold producer restart,
complete history/finalization, exact receipt and independent decision acceptance,
cleanup, and source-free finalized receipt readback. Preserve unrelated-journal
rejection and all original Native coordinates. Re-run the ingress-custody lifecycle
on the exact changed source; a backend-only SQL experiment cannot establish it.

## Platform and failure matrix

| Family | Required witness |
| --- | --- |
| Fresh bootstrap | Existing valid parent, absent/malformed parent, owner/file creation race, each failed file flush, crash before/during/after schema/allocation/barrier commit, unknown/unversioned/incompatible state; no implicit replacement or remote mutation. |
| New-file persistence | Abrupt guest-machine reset immediately after acknowledged fresh initialization/first selection. Reopen from the resulting disk and compare external acknowledgement oracle. A warmed DB is insufficient. |
| Security | Non-elevated token, ACL inspection before preimages are written, another ordinary user denied access, permissive/untrusted parent rejected, standard SID handling, unexpected sidecar rejected. No admin/backup/volume-flush privilege. |
| Namespace | Junction/symlink/mount/reparse points in ancestors and final names; cloud placeholders, UNC/mapped remote volume, FAT/exFAT/ReFS rejection; hard-linked database/journal/owner, ADS/device names, alternate aliases, long/Unicode paths, case variants. No follow-before-check write. |
| Live ownership | Same process and competing processes, blocked network worker, timeout/cancel before actual exit, root/owner/path replacement attempts, owner close with live children, noninherited handles on spawn; independent construction progresses. |
| Byte construction | Empty, small, multi-batch and large journals; exact expected digest/length, short writes, quota/full disk, conflicting contenders; partial candidate is never selected or staged. |
| SQL fault boundaries | Inject xWrite/xSync/commit failures, journal-header invalidation failures and lock/share errors. After uncertain outcomes, poison/reopen/reconcile with no replacement primary. Distinguish API-returned failure from before/after-process termination. |
| Bounded candidate recovery | Crash after early/middle/last committed chunk batch, before selection, after selection but before response, and mid-orphan cleanup. Cursor resumes without scanning unrelated generations or discarding the selected winner. |
| Original control integrity | Change manifest/request/graph commitment before any later ack exists; original seal rejects it. Wrong allocation/session, absent controls, stale record/generation, changed complete primary all fail. |
| Receipt exception | Resolved preparation; receipt path performs zero preimage-content reads/hashes even for missing/truncated/changed pending bytes when original controls and authenticated exact receipt match. Strict replay of needed corrupt bytes still fails before mutation. |
| Receipt versus decision | Valid receipt without decision ack persists, does not reclaim or advance terminal settlement; valid ack without receipt also cannot reclaim. Forged internally self-consistent receipt, different repeated receipt, wrong completion/primary/member reject. |
| Reclamation | Interrupt every role and chunk-delete boundary; cursor/rows atomic, original lengths charged until complete, once-only usage subtraction, compact authority retained; zero content rehash in receipt-only path. |
| Physical space | Database/free/journal/cache/scratch measured separately. Reuse free pages; bounded incremental vacuum; journal high water counted. No full VACUUM/copy on receipt, no credit for physical bytes still allocated. |
| Saturation | Fill logical and physical configured admissions independently. Demonstrate preallocated control/journal headroom supports exact receipt/ack/progress and cleanup; true write/flush failure retains state. Include receipts larger than local compact-control encoding. |
| Closing/inventory | Indexed scratch batch construction and restart, duplicate member rejection, complete framed inventory, exact finish-ready/custody cursors, crash during scratch cleanup; no direct private filesystem bypass or second membership authority. |
| Schema/install | Explicit StateSchema initialize and unknown-format failure; startup does not upgrade; preserve interrupted future upgrades. Installed client/CLI contains schema/migrations and uses locked SQLite. Full audits remain separate from point operations. |
| Disposal | Terminal authority precedes any container teardown, quiesced engine, paired journal retained until safe, sharing-denied deletion is pending, absent path cannot restart a retired allocation. Retained compact container is valid where no external disposition exists. |

The matrix applies at the owning behavior level. Parameterize common prepared-store
and producer tests across POSIX and Windows adapters rather than globally removing
skips from POSIX syscall tests. Audit both test collection **and execution**: a green
Windows job with shared recovery behavior skipped remains incomplete evidence.

## Distinguish process death, OS crash and power failure

Use `subprocess` native spawn and abrupt process termination for ownership and
transaction-boundary tests. A controller stores expected bytes/control identities
outside the killed process. For commit-response loss, the controller distinguishes
acknowledged commits from uncertain ones: acknowledged state must survive;
uncertain work may be absent or exactly committed, never partially selected.

A killed process leaves the Windows kernel/disk cache running. It does not prove
power-loss durability. Add VM hard-reset/storage-fault tests at actual I/O barriers,
including first container creation and journal reuse. Capture guest build, NTFS,
virtual disk/controller/cache semantics and host flush behavior. A VM reset proves
only that recorded stack; it is not a universal physical power-cut guarantee.
Use the repository's authorized qualification mechanism, with production-facing
code running unprivileged; do not silently require elevation in the backend.

For file/flush injection use a test-only VFS wrapper or native shim that delegates
to the **same** normal Windows VFS and can reject/interrupt selected calls. Record
its exact source/build and positive controls. A pure Python exception before
COMMIT is not coverage of a failed FlushFileBuffers within the engine. Never ship
the fault VFS or reduced-sync configuration as the production backend.

## Growth and resource evidence

Independently vary:

- Unrelated member backlog and compact/pending ratio (use the owning issue's
  approved workload cadence, not a new universal count requirement).
- Journal and request/observation sizes independently, chunk count, interrupted
  attempt count and actual bytes replayed, cleanup cursor position and cache state.
- Receipt encoding versus compact-control size, physical/free-page reserves,
  page/index depth, journal high water, active readers and native share denial.

Measure actual Windows file/VFS reads, writes, flushes, file opens and allocated
bytes, plus SQLite page-cache behavior/query work and Python hashing/Native calls.
SQL-statement counts alone do not establish byte I/O. A SQL query that selects no
payload can still fetch a database page: distinguish bounded control/index page
I/O from bulk preimage content. Cold caches and same-sized control fixtures reveal
dependence on unrelated data. Save execution conditions and elapsed time.

Required claims: immutable complete publication, no second author for a selected
member, point receipt cost with zero pending-content hashing, bounded transaction
journal/recovery, no full-store validation per point call, resumable bounded cleanup,
explicit B-tree bookkeeping, actual physical-space behavior, responsive independent
constructions. Report preparation/replay/receipt/cleanup/maintenance separately.
No constant-memory Native authoring claim follows from chunk storage.

For incremental vacuum, include alternating growth/deletion and completed-heavy
stores; count moved pages cumulatively, not only calls. Keep control reserves after
maintenance. A full health audit or upgrade can have a separately declared full-
database cost, but never hide it in each cold receipt or poll.

## Delivery and finite consumption

Use `make lint` and owner tests for checkpoints; run the existing required exact-
source/tested-merge suites and built native-client install path before delivery.
The current platform matrix already has `windows-2025`. Add actual native backend
and non-elevated witnesses to its maintained selection; do not create a decorative
new job that bypasses the producer. Preserve Linux/macOS and actual ingress tests.
Required exhaustive local work remains governed by the integration rail, including
`make linux-qualification` where applicable.

Record exact source, adopted/independently reproduced/partially used/superseded/
rejected disposition, executed evidence and remaining qualification in #982.
Windows support follows successful implementation/qualification, not publication
of this reference. Keep #981's other resource/Native acceptance distinct. No
numerical timing promise, waived proof, provider qualification or release action
is authorized here.
