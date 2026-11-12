# Retained Native construction and cross-platform resource receiving

## 1. Selected cut

Make construction a retained operation, not a Python return-value convention.
The application admits and stores an exact **passive construction frame** before
invoking an effectful Native factory. The frame owns the originating file resources
and retains the independent SQLite construction scope. Native attaches returned
storage/session receivers to that same frame before callbacks or validation.

The frame is not another database, scheduler, archive authority or resource-budget
policy. It is a process-local receiving record in the existing owner slot. Missing
`slot.session` is never proof that construction acquired nothing. No raw descriptor
adoption, optional legacy mode, garbage-collection receipt or FileIO.closed test
is selected. Extend the authorized neutral package rather than importing the
client's Windows implementation into Native or maintaining two owner models.

The reference is an executable candidate, not a complete product patch. The neutral
installed package has no Native/Core imports. Its `Construction` illustrates the
journal profile; `native_bridge.py` uses explicit test routing to exercise the
unchanged Native format/session code and Core owner. Production passes the original
frame explicitly through maintained interfaces, with the hard cut in INTEGRATION.

## 2. Three receipts, not one

| Owner | What it can establish | What it cannot establish |
| --- | --- | --- |
| Native file/directory Resource | Confirmed original acquisition or one positively recorded close | SQLite internals, archive custody, namespace deletion or worker completion |
| Existing SQLite factory/connection/cursor scope | Its exact original connections, unreturned constructors, cursors and blobs have received | File creator/directory resources or another connection scope |
| Application execution/namespace owner | Actual original worker quiescence/join, lease and budget release under its contract | Success inferred merely from no returned Native object |

The Native storage receiver additionally preserves final-reader custody. A closed
JournalStore facade may still have a borrowed frozen reader keeping its connection.
Never call the factory scope's emergency receiving on behalf of an active reader.
First request Native close; if its final-reader receipt is absent, retain the frame.
An unreturned connection with no storage wrapper remains the factory scope's work.

The reference supplies a dedicated scope using the existing SqliteConnectionAdmission
with explicit fixture grants. It does NOT add SQLite open/close semantics. A shared
production admission must expose a per-construction ticket or equivalent original
receiver; its aggregate connection count reaching zero is not a per-attempt proof.

## 3. Before the first effect

The caller performs the following order inside its already admitted owner slot:

```
slot.construction = NativeConstruction(exact_scope, explicit_handle_grants)
# No open, mkdir, connect, validation callback or finalizer close in that allocation.
Native.open_retained(..., construction=slot.construction)
```

Store the frame before acquiring the application namespace lease where that lease
can leave an obligation, or retain the original namespace owner first and attach
it before invoking Native. The reference Core bridge does the former. Use #987's
retained namespace owner at actual raw-lease origins; an ExitStack callback around
an integer is not promoted into a positive receipt by this design.

Reserve file records, connection/cursor/operation grants and execution capacity
before acquisition. The journal profile needs one creator record on Windows and
creator plus directory-sync record on POSIX; a read-only frame may leave those
records unused. Allocate all required records before the first file operation.
Memory-allocation failure while making a passive frame leaves no acquired file.

Exclusive creation stays an atomic native create: POSIX O_CREAT|O_EXCL or Windows
_O_CREAT|_O_EXCL. Do not replace it with exists()+SQLite creation. Reopen retains
the existing validated format, readonly `mode=ro` and other URI/thread/cache options.
A failed attempt never silently changes create into reopen based on path existence,
truncates a partial file, upgrades old state, or recreates missing original bytes.
The owning initialization/recovery contract decides further use of retained files.

The exact frame is single-attempt. Retrying its receiving is not rerunning its
constructor. A known pre-dispatch refusal can retry the original primitive when
its phase allows it; a lost result cannot select a new resource. After constructor
failure, successful receiving records resource release, NOT successful creation.

## 4. Native outcome boundary

`Resource(kind)` allocates only bookkeeping. `acquire` executes into that already
retained C object. The outcome and original descriptor are recorded before any
Python notification or allocating result. The object remains OPENING during
post-open validation, preventing another thread from closing a descriptor while
fstat is still using it. Rejected validation retains the acquired resource.

| Outcome | Retained state / action |
| --- | --- |
| Known refusal before dispatch | No native effect; original phase remains retryable |
| Native open returns failure/no descriptor | ABSENT for descriptor custody; no claim that namespace bytes are unchanged |
| Open succeeds, Python reply lost | Same LIVE owner, never reopen it |
| Post-open type/private validation fails | REJECTED but still held; explicit receiving required |
| Acquisition result genuinely unknown | UNKNOWN; charge retained, no lending/reopen/close-by-guess |
| Close blocked by active borrow/child/native call | Busy; original resource stays held |
| Native close returns zero | RECEIVED recorded before Python reply/signal delivery |
| Python close reply lost after recorded success | Retry reports the same receipt; no second native close |
| Native close fails/indeterminate result | UNKNOWN; executable integer invalidated before dispatch and never reused |

The supported profiles use one backend close attempt per resource lifetime. No
retry loop handles EINTR. A descriptor slot reused for an unrelated file, or even
for the identical inode, cannot become a new close target. The native receipt is
not derived from fstat, EBADF, path identity, empty lookup or wall time. Nonzero
close results remain conservative UNKNOWN even on a system that often releases
the slot on error. There is no in-process force-receive path for an unknown result.

The safety/availability consequence is intentional: unknown resources can occupy
the original grant indefinitely. Other admitted constructions progress independently;
new work can be refused by its existing resource owner. Process exit in a test
cleans up fixture resources, but is not a fabricated in-process resource receipt.
No descriptor integer or CRT/HANDLE identity is persisted for use after restart.

## 5. Supported backend profiles

### Linux and Darwin, GIL-enabled CPython

Use the #987 native openat/fstat/close outcome mechanism, with no-follow and
close-on-exec flags, POSIX private/current-user validation and a retained directory
resource for each directory sync. Directory acquisition and fsync do not create
transient unowned descriptors. Keep original namespace permissions, inode/nlink
checks and read-only behavior with their owners. Such identity checks validate
which file is used, never whether an earlier resource received.

No external close/detach/dup2 of a lent descriptor, fork with active owners, forced
native-thread cancellation, or free-threaded CPython is in this profile. A borrower
must finish its actual I/O and receive its borrow before the resource can close.
The application still needs actual worker join before releasing execution charges.

### Windows maintained Native journal API, GIL-enabled CPython + dynamic UCRT

Use the same dynamic UCRT family as supported MSVC CPython, not a second static CRT
or the client's NTFS/HANDLE owner. The native reference uses `_wsopen_s` with a
Unicode absolute path, binary/noninheritable flags, explicit sharing and exclusive
creation when requested. `_fstat64` rejects a non-regular result. `_close` closes
both the CRT descriptor and its underlying OS handle; do NOT also CloseHandle it.
A thread-local invalid-parameter handler is restored after the close boundary;
no process-global handler is changed. A nonzero outcome stays UNKNOWN.

CRT mode bits are NOT POSIX ownership or a private ACL. The current Native caller
retains/authenticates its private directory and user/lease policy. Do not claim
`_S_IREAD|_S_IWRITE` creates private ACLs, import the client policy, or add an NTFS-only
support restriction. Preserve the current Native path/symlink guards, admitted
namespace, original verified content and read-only open. The reference tests use
ordinary isolated temporary directories, not a new security deployment policy.

This profile supplies the Windows regular-file creator needed by JournalStore.
The existing Native Windows `_sync_workspace_directory` has no directory fsync;
that remains explicit. The POSIX-only retained-corpus/directory API is not silently
advertised as Windows capable. Conversely, Windows JournalStore tests must execute;
the POSIX corpus condition cannot skip the required shared journal behavior.
No Windows CRT directory emulation, privileged volume flush, new durability claim
or HANDLE-to-CRT adoption is needed for this focused receiving cut.

## 6. Callback, borrower and constructor ordering

The internal retention hook first attaches the exact Native storage/session to
the original frame, then notifies the caller. Notification can fail without hiding
that original receiver. If a storage wrapper constructor itself fails after a
connection opens, the pre-retained factory scope still owns that connection.

On successful journal initialization, receive its transient creator/directory
resources before transferring the final-reader release callback to storage.
A callback cannot release parent custody while one of those transients remains
unreceived. Preserve the existing final-reader callback ordering: database close
is confirmed before the application callback, while the final `_closed` marker
is assigned after the callback. Avoid a circular condition that requires that
last marker before permitting the callback which precedes it.

Callback failure retains the same callback and original release owner for retry;
never null the callback before it positively returns. A callback's return only
settles its own stated obligation. A worker future being done or joined cannot
close an unreturned Native file. Conversely, receiving all files does not prove
that the worker is joined or that schema/sync/validation succeeded.

The bridge's ContextVar and monkeypatches are TEST instrumentation only. No hidden
thread-global ownership discovery, optional raw fallback, callback-only mode or
new executor is selected for production. Constructors receive an explicit frame.

## 7. Retained corpus and namespace-sync cut

The POSIX corpus origin uses the same primitive but a different lifetime:
pre-retain its **guard** and original borrow before opening `corpus.sqlite3` and
before connecting. Keep that guard/borrow live across corpus validation; it is
not the transient journal creator and must not be closed by journal `ready()`.
Store every directory-sync owner before acquisition. Attach the corpus's existing
SQLite receiver before `_from_workspace` or schema/current-source validation.

At corpus receiving: stop new work; obtain the original database/cursor receipt;
finish/receive the guard borrower; receive the original guard and sync owners;
only then invoke the retained parent-release callback. Preserve `_verify_file`'s
regular-file, UID, exact private mode, nlink and namespace identity requirements
using the original borrow. Unknown closes retain the original frame/parent budget.

INTEGRATION provides the exact owner mapping. The executable bridge currently
covers JournalStore/JournalValidationSession/Core, not a complete corpus adaptation.
The primitive directory tests prove that backend's outcome handling; they do not
substitute for the required maintained corpus lifecycle acceptance.

## 8. Bounds and growth evidence

Let R be admitted resource records, C the original scope's admitted SQLite resources,
and L the admitted path-encoding length. A primitive call has at most one open
entry, one post-open stat, one sync entry when requested, or one lifetime close.
The native owner has fixed bookkeeping per resource. Receiving never scans a
namespace or rereads payload. Windows CRT entries may implement multiple internal
OS operations; the declaration is not a one-kernel-syscall or latency guarantee.

The frame holds O(R+C) owner metadata plus existing bounded source/validation state.
One receiving turn visits only its admitted records and the separately bounded
SQLite receiver. No per-retry growing callback list, garbage-collector sweep,
workspace enumeration or new thread is introduced. Path encoding/validation costs
O(L); filesystem sync latency and the existing SQLite engine's work remain separate.
Native authoring, journal validation and aggregate process heap limits are not
qualified by these primitive bounds. An unknown record stays in the original
ordinary/terminal grant; receiving uses the original charge exactly once.

The source-owned declarations and executable growth probe measure actual native
entry counters through the maintained growth validator. Difficult fault vectors
are in QUALIFICATION. Scope and platform results must stay separate; no POSIX-only
success is relabeled Windows qualification.
