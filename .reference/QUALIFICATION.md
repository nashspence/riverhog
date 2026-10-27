# Finite integration and qualification plan

Start at [HANDOFF.md](HANDOFF.md). This is a proposed proof plan, not evidence that
these native/product cases ran. Retain Native141 and the four measured transaction
families as prior issue evidence; neither is a numeric worst-case proof.

## First slice and source placement

First extend `tests/support/sqlite_vfs_faults.py` and
`packages/riverhog-client/tests/test_prepared_primary_vfs.py` with pre-delegation
page/extent refusal plus a separate automatic-rollback bank. Exercise the real
loaded Windows engine, not a mocked SQL exception. The reference Linux probe
locates this seam but must not become a production dependency.

The production resource gate and process SQLite budget belong to a private native
client storage helper, called by `_prepared_primary_sqlite.py` before *every*
connect/factory/transaction. Keep `_prepared_primary_windows_native.py` as the
native identity/ownership/ACL authority, and continue delegating to its selected
normal Windows VFS. Do not import `tests.support` or dynamically compile production
code at application startup. Add native packaging through the existing locked
build/installed-wheel gates; the extension must not carry a second SQLite engine.

Update `_prepared_primary_sqlite_controls.py` for exact fixed-extent class metadata;
`_prepared_primary_sqlite_journal.py` for target-versus-observed readiness;
`_prepared_primary_sqlite_space.py` for backed-ceiling/free-floor maintenance;
`_prepared_primary_sqlite_ordering.py` and candidate/reclaim methods for structural
classes, resource cursors and pending-capacity results. Change the current-only
schema/migration and StateSchema shape tests for a fixed barrier/resource profile.
At the inspected newer source the schema helpers have leading underscores; that
rename is the only drift from the experiment base.

One application process resource owner accounts for all users of the same SQLite
library. Thread that policy explicitly through maintained application startup;
do not let independently opened constructions race to overwrite a global limit.
No general backend/process extraction is required by this reference.

## Proof obligations and finite vectors

| Family | Required witnesses and exact assertions |
| --- | --- |
| Gate completeness | Arm before initial connect/PRAGMA/StateSchema; cover xWrite, truncate grow/shrink, allocating file-control hints and allowed file opens/deletes. No default-VFS or SQLAlchemy factory bypass. Unknown temp/WAL/ATTACH paths fail explicitly. |
| Prefix induction | Cross-page/unaligned/multi-page writes, repeated page, exact Q and Q+1, total-byte and callback limits, signed-offset overflow, zero writes, tracking collisions/capacity. No rejected range reaches the delegate; charge failed and partial attempted calls without refund. |
| Automatic rollback | Refuse a journal write before DB writes, then a DB write after at least one successful DB delegation; native failure both before and after delegation; restoration begins inside the same failing call. Recovery bank remains live until settle/close, independent of spent forward budget. |
| Cold hot journal | Kill during journal record/header/sync and DB flush/commit. Fresh process arms recovery before first SQLite read. Restore under original or deliberately larger grant; reduced grant blocks nondestructively. Corrupt/oversized journal never leads to raw deletion or unbounded scanning. |
| Fixed controls | Sizes around inline/overflow and varint boundaries: 0, 1, 9, 4060/4061/4062, 4091/4092/4093, 8 KiB, 64 KiB, 256 KiB; admitted large values are not archive ceilings. Vary record prefix/name lengths, tree depth and deliberately fragmented overflow chains. Verify the derived Q/J with actual VFS attempts, not only successes. |
| Geometry | Actual native sector/page settings; model 512/4096/8192-byte sectors and reject unsupported geometry. Verify all engine device characteristics are genuine; do not assert powersafe overwrite merely to reduce the reserve. |
| Structural indexes | Ascending/descending/interleaved keys; root split, interior split, underfull redistribution, root collapse; rowid and WITHOUT ROWID paths. Include long common-prefix admitted keys, overflow payload thresholds, freelist trunk rollover and pointer-map boundaries. Observe actual tree height, not just N. Successful steps stay within quota; too-small quotas reject with original authority intact. |
| Allocation/selection | Control zeroblob allocation, namespace/object/chunk/name creation, final immutable selection and duplicate/conflicting retry. Failure before ready prevents staging; failure after selection reconciles the exact winner. Initialization/upgrade/barrier are separately included. |
| Cleanup/vacuum | One selected/unselected chunk, final object/index deletion, scratch generation/cursor deletion, incremental_vacuum(1) moving an overflow or B-tree page. Quota refusal never advances the cursor, releases original admission, or reports device space freed. Explicit larger grant permits exact continuation. |
| Physical reserves | Actual allocated extents, not file length only. Reserve preparation killed before/after each commit/readiness update; short allocation, failed flush, quota reduction and external low-space. Payload tries to consume Fprogress or extend beyond backed pages: refuse/rollback before publication. |
| Progress at saturation | All applicable fixed controls already allocated, journal floor backed, sufficient process memory available. Persist exact custody/decision and control cursors with payload denied. Separate structural cleanup deferral from custody success. Never require payload/candidate-idle to replenish independent reserve resources. |
| Shrink/replenish | Keep free-floor and journal allocation through maintenance; failed truncate and hot replay of pre-shrink size; explicit increase while work remains pending; no unguarded recovery under a lowered profile. Report logical/free/EOF/allocated quantities separately. |
| Heap/cache | Cache suggestion deliberately tiny; actual aggregate memory reaches H; NOMEM with exact rollback/reopen. Two constructions plus another SQLite connection demonstrate aggregate scope, noninterference of policy ownership and explicit contention. No per-construction/RSS/Native-memory claim. Qualify source ID, compile options and default heap-cache assumptions. |
| Source/custody | Existing full HTTP/Native stage-before-bind, original-manifest seal, complete authenticated receipt, independent decision acknowledgement, strict unrelated-journal rejection, live authorization and source-free receipt path unchanged. |
| Packaging/platform | Locked toolchain, installed extension/client, correct loaded DLL identity, shared-store and Windows owner integration execute natively without POSIX skips. Preserve Linux/macOS existing gates and explicit forward-upgrade tests. |

Each deterministic boundary case needs a positive exact-budget and a negative
one-below-budget version where meaningful. Finite tests validate the implementation
of the universal pre-delegation invariant; they do not prove a universal small
numerical structural bound by sampling. Failure evidence is retained. A successful
small synthetic gate does not qualify the maintained backend.

Instrument per phase: attempted/delegated/successful reads and writes, maximum
active journal extent and retained allocation, distinct DB pages, calls, native
retry observations, peak SQLite-accounted heap, shim/application buffers, page/free
counts, resource transitions and exact result/custody hashes. Include initialization,
prepared-but-unselected recovery, implicit rollback and cold open; do not report
only runtime commits. No payload/credentials in diagnostics.

Larger index populations and journal/body dimensions should grow independently;
use 1/17/1,025/8,193 as reusable fixtures, plus constructed deeper/fragmented states
and the issue's agreed larger-workload cadence. Those sizes are not support limits
or new per-commit gates. Timing remains an observation; growth-contract violations
and invalid evidence remain validation failures.

## Acceptance and disposition

Native process exits are not machine-reset/power-loss qualification. Keep the
existing actual NTFS full-volume/flush/VM-reset work, exact source/tested merge CI,
producer activation and #981 Native working-set admission distinct. Do not weaken
those requirements because the resource gate makes a model pass.

The owning issue decides adoption of this explicit enforcement/liveness trade-off.
After consumption, record exact integrated source, adopted/partially used/reproduced/
superseded/rejected disposition and remaining qualification in #982. Do not retain
this branch as a parallel implementation or close #981/#964 by reference publication.
