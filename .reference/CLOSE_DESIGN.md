# Retained POSIX receiving: selected contract

## 1. The boundary that changes

A descriptor integer identifies a slot in a process table, not a permanently
owned open resource. Do not wrap `os.close(fd)` in a retry callback, probe `fstat`
to decide whether a past close succeeded, or reopen a matching inode to replace
original custody. Both #987 reproductions are consequences of losing ownership
at that boundary. Source and platform references are in [RESEARCH.md](RESEARCH.md).

Select **one neutral CPython native resource owner for files and directories**.
The native boundary owns both the syscall and outcome bookkeeping; the application
owns admission, retention and authority. It is not another storage layer, process,
lease service or scheduler. Native code is necessary here to remove the Python
return-value/assignment gap without interpreting a reused descriptor as evidence.

`Resource(kind)` allocates state only. The caller reserves resources and retains
the exact object before calling `acquire` or `duplicate`. All argument conversion
precedes acquisition. There is no custom Python opener. The native result is
recorded on this object before any Python result, error, or signal-handler check
can be delivered. Object construction, garbage collection and destruction never
perform a resource syscall or claim receiving.

The supplied package is `retained-posix-reference`, not a second production rail.
Integration should put this neutral boundary in a focused shared package and use
one current API across Core and Native. Do not keep raw-integer cleanup as fallback.

## 2. Scope, support and assumptions

The selected source profile is GIL-enabled CPython on native Linux and Darwin,
with `openat`, `O_CLOEXEC`, `O_NOFOLLOW`, `O_DIRECTORY`, `O_NONBLOCK`, `fstat`,
`F_DUPFD_CLOEXEC`, and `close`. The reference rejects other build platforms and
free-threaded CPython. Linux was executed; Darwin and the repository-pinned
CPython 3.12.3 remain installation/native qualification, not inferred passes.

Only existing regular files and directories in the caller's admitted original
namespace are selected. No create, truncate-at-open, device/socket, Windows,
network-filesystem liveness, or arbitrary user-provided native function profile
is supplied. O_NONBLOCK avoids blocking on an accidental FIFO before validation;
post-open kind validation retains a rejected acquired handle until explicit close.
Private ownership, exact paths, retirement rights and symlink-safe ancestor
traversal remain the caller's existing responsibilities. Root paths must be
absolute; relative opens require one exact component beneath a held directory.
The flag protects the final component, not every ancestor of an absolute path.

Cancellation is cooperative. Do not use pthread_cancel, forced native thread
termination, custom signal handlers that mutate descriptors, fork with live
owners, or external close/dup2/detach on a borrowed descriptor. The PID guard
rejects use of an inherited owner; O_CLOEXEC is not protection against fork before
exec. This is the same ownership discipline that makes any descriptor-owning
stream meaningful. Native corruption or a syscall interposer that lies about
success is outside the supported backend contract.

Acquisition/close may block in the OS. The GIL is released around those calls;
work stays in its existing execution worker, not a coordination/status call.
Other resource instances can progress. The state `opening`/`closing` is not
receiving, even if cancellation was requested or the Future appears finished.

## 3. Native outcome state machine

| State | What is known | Permitted behavior |
| --- | --- | --- |
| new | No acquisition entered | Acquire once, or explicitly receive unused state. |
| opening | Native acquisition/validation has not returned its recorded outcome | Retain reservation and dependencies; refuse close/reacquisition. |
| absent | open/dup returned -1; no descriptor was returned | Explicit close records no-resource receiving. The path's absence grants no retirement authority. |
| live | Exact acquired handle is retained and kind-validated | Borrow or close after all dependent uses receive. |
| rejected | A handle was acquired but kind/metadata validation failed | No lending; explicit close is still required. |
| closing | The one close dispatch is in flight | Retain charge/dependencies; no retry or new use. |
| received | The original close returned zero, or no acquisition ever occurred | Idempotent close returns without a syscall. |
| unknown | Native acquisition/close outcome cannot establish receiving | Retain charge and parent custody; deny lending, redispatch, reopening and forced success. |

A before-dispatch refusal that is known to have made **no syscall** leaves the
original state retryable. An arbitrary exception is not automatically such a
refusal. The test build has an explicit before-effect seam; production calls do
not guess which side of a syscall an exception happened on.

A successful acquisition followed by a lost Python reply is `live` (or `rejected`
after validation failure) on the same retained object. Never acquire it again.
A successful close followed by a lost Python reply is `received`. Retrying close
returns the stored result and does not access the old descriptor slot, even if
it now contains an unrelated open or a new open of the identical inode.

**Only close return zero is positive acquired-resource receiving in this profile.**
Every nonzero native close result, including EINTR, EIO and EBADF, is retained as
`unknown` with its errno. No retry is attempted. Linux documents stronger release
behavior for several errors, but using that fact is not necessary for this
conservative cross-platform contract. It also must not erase failed flush or
write evidence. Do not infer receiving from `FileIO.closed`: the audited CPython
implementation invalidates its stored integer before calling close.

The implementation makes an uncertain integer non-executable. If a genuine
failure left it open, the original charge and parent dependencies remain; it is
not a bounded-leak-by-retry scheme. No `force_receive` API is provided. Another
native completion can settle an in-flight operation before returning, but a
completed unknown result has no automatic in-process positive receiver here.
This is an explicit safety/availability policy: refuse new work when retained
uncertainty exhausts admitted capacity, rather than leak unbounded owners or
close guessed descriptors. Existing operator/execution reconciliation owns any
further disposition; this handoff grants no new authority to kill workers or
release durable state.

The state is process-local, not a restart file containing integers. A restarted
process must not replay raw descriptor values. Process exit in tests cleans their
kernel resources but is not counted as a product receiving receipt or namespace
retirement. Durable application retirement/lease reconciliation keeps its own
original-owner and actual-execution requirements.

## 4. Borrowing and dependencies

`Borrow(owner)` also allocates state without pinning. Retain the borrow before
`acquire()` pins the original resource. `fileno()` lends a non-owning integer only
while that exact borrow remains held. It cannot transfer ownership, extend its
lifetime by copying the integer, or authorize external close. Pin release is
recorded natively before return and is idempotent after reply loss.

A borrower releases its pin only after its actual descriptor-using execution has
returned. The original execution owner still establishes actual worker join.
Join alone does not release the pin or close the resource. Conversely, closing a
resource does not establish that a worker using other resources has joined.

A child acquired relative to a directory increments that original directory's
native dependency count before openat and retains it through uncertain outcomes.
Positive child receiving releases that dependency once. Definitive acquisition
failure releases it because no child was acquired. Duplication uses the held
source, not a pathname, and inherits its parent dependency; each duplicate is
separately retained/charged. Closing one reference is not proof that every alias,
SQLite connection or open file description in the application has been received.

`Group` is a single-controller retained reverse-dependency list. Reserve its
capacity before creating it. It inserts each Resource/Borrow **before** any
acquisition/pin. `close_step(record_budget)` visits at most that many records,
removing one only after positive receiving. A failure preserves the exact record
and every predecessor. It refuses to release an active Borrow on behalf of its
executor. There are no arbitrary close callbacks and no GC-driven disposal.
One controlling executor serializes Group mutations; Resource state itself
rejects competing effects while its native operation is active.

A directory dependency is not sufficient application parent custody by itself.
The actual original workspace lease must remain in the outer owner until **all**
child turns, Core/Native handles and duplicated references receive. The shared
package grants no catalog phase, lease replacement or namespace deletion rights.

## 5. Neutral extent composition

[extent.py](retained-posix/src/retained_posix/extent.py) supplies an executable
reference for the existing `workspace_reclamation.reclaim_retired_files` boundary.
The caller retains ExtentTurn before advance; both receiving owners and both
borrow records already exist before the first open. The turn validates exact
private files, never enumerates a directory, truncates/unlinks at most one
admitted extent, and preserves unselected entries. Original stat checks remain
mutation-identity checks, never close-result proof.

The turn's synchronous descriptor uses end before their Borrow objects receive.
Only then are the file and directory resource closes attempted. A data close
refusal keeps the acquired data owner and prevents directory receiving. Advance
raises and the original turn remains available for close-only retry; close does
not truncate the file a second time. Reply loss after directory close similarly
retains a recorded receiver, not an executable old integer.

Extent mutation/durability and descriptor receiving are separate results. A
failed fsync still fails advance even if subsequent closes all succeed. A
successful close cannot certify previous truncation/unlink durability or lower
the byte charge. The existing irreversible retirement owner reconciles actual
remaining extents under its original lease; this package does not recompute
semantic metadata from a retired database.

Integration must hard-cut the current helper and all Core/Native callers to
mandatory caller retention, as detailed in [INTEGRATION.md](INTEGRATION.md).
The reference does not import or secretly patch those implementations.

## 6. Work and physical-charge rules

Per resource lifetime: at most one openat OR F_DUPFD_CLOEXEC dispatch, at most
one acquisition fstat, and at most one native close dispatch. Before-effect
refusals and idempotent/unknown close retries add no syscall. Relative-child
validation costs O(encoded name bytes); the native owner has fixed-size state.
A status observation does no resource syscall, payload read or enumeration.

Across R retained resources and T observation/refusal/retry calls, native
resource effects are O(R), user-space control work O(R + T + admitted path bytes),
and retained owner state O(R). This is not constant wall time, a kernel pathname
cost bound, or a bound on the Python allocator/process heap. Group receiving is
O(record_budget) per step and O(R + actual retries) overall; it never scans all
unrelated resources. Each group's record capacity is operator-admitted work,
not a limit on the archive or total number of retirement turns.

The neutral extent borrower retains the existing name-validation, exact metadata
and octet work bounds. It adds no payload read, SQLite connection, directory
listing or new authority. Its complete name validation still has O(N log N)
comparison work and O(total name bytes) storage; do not reclassify that as O(1).
The reference native counters and source-owned declarations in
[growth.py](retained-posix/src/retained_posix/growth.py) are exercised through the
maintained growth validator, with actual unrelated-name and extent dimensions.

Admit file/dir handles, borrow records and native owner memory before effects.
Charge an uncertain acquisition conservatively as one possible handle. Preserve
ordinary execution and reserved terminal capacity separately; do not use a
terminal grant to admit fresh semantic work or release it on a Future state.
Charge release requires original owner receiving **and** the existing worker join
predicate. Release once using the original charge token, not mutable path sizes.
No new acquisition is allowed merely to get a cleaner receiver after a failure.

Native code is an explicit build/runtime boundary, not a claim of full product
qualification. [QUALIFICATION.md](QUALIFICATION.md) retains macOS, pinned-runtime,
Core/Native integration, installed-distribution and exact-source CI obligations.
