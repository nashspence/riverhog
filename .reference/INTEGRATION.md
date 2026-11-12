# Owner-local hard cut and first witness

All anchors below are at `c5157d69712185aa8d4ad185e2e6d465dfbd7179`. The unchanged
source is the experiment oracle. This packet does not edit those product paths.

## First slice: the reproduced creator

Add a passive receiving field to the actual Core slot, before entering Native or
any owner lease which could leave an obligation. Thread the same required frame
through JournalValidationSession to JournalStore. Replace only the exclusive
creator and POSIX directory-sync origins with the neutral native owner, retaining
existing SQLite URI/schema/checkpoint/final-reader behavior.

Run the issue's actual public-source witness first: `reproduce_public.py` must say
`reproduced-not-qualified`. On integrated source, use the same fixture ordering
but inject at the new native before-close boundary. After advance raises, either
the slot remains with its original live creator, or an explicit same-frame retry
has positively received it. Parent lease release without that receipt is forbidden.

Then inject a genuinely unknown native close. The original slot must remain,
competing workspace admission must fail, repeated receiving must dispatch no
second close, and a different construction must still progress. This is tested
in the supplied bridge against the actual Core advance and workspace lease, in
an isolated subprocess. Its final process exit is fixture cleanup, not receiving.

## Required interfaces

Names are illustrative; the required pre-effect order is not:

```
frame = NativeConstruction(original_connection_scope, explicit_file_record_grant)
slot.construction = frame                 # before any Native effect
session = JournalValidationSession.open_retained(..., construction=frame)
slot.session = session                    # useful result, not sole ownership record
```

The frame constructor has no resource-acquiring default, directory maker, side
effectful validation or finalizer cleanup. Reject a missing/incompatible scope
before effectful construction. Default/optional retained ownership is not selected.
A receiving exception retains the same field; do not reset it when no session was
returned. No successful `None` return is used as a receipt.

| Owning source | Concrete integration |
| --- | --- |
| `riverhog_core/services/upload_journal_validation_owner.py` `_Slot`, `_open`, `_drop`, `advance`, `close` | Pre-retain frame and namespace owner; stop deleting slots on `session is None`. Gate resource/capacity release on original receiving and separately actual worker join. |
| `riverhog_provenance/journal_session.py` `JournalValidationSession.open_retained` | Require and pass the original frame, retain partial session state without replacing its original store/scope. |
| `riverhog_provenance/journal_store.py` `JournalStore.open_retained`, `_open`, `_sync_workspace_directory` | Exclusive create into pre-retained owner; keep SQLite connect/schema/read-only behavior. Retain sync directory; no raw open/finally-close. |
| Same file `_SnapshotStorage`, `_SnapshotLease`, `JournalStore.close` | Preserve final-reader release and independent SQLite receiver. Gate callback transfer on transient receiving; do not use facade close as final storage receipt. |
| `packages/sqlite-admission/.../__init__.py` `SqliteConnectionAdmission`, `_ConnectionLease` and admitted cursor/connection receivers | Reuse exact original scope/ticket and resource grants. Never close guessed SQLite internals or count a shared aggregate as the attempt's receipt. |
| `riverhog_provenance/corpus_workspace.py` `RetainedCorpusValidator.open_retained`, `_sync_directory`, `_verify_file`, `close` | Pre-retain long-lived guard + borrow and sync owner before opening. Preserve actual guard validation and original SQLite/cursor ownership. Clear parent callback only after positive return. |
| Core retained-workspace/lease receiving introduced under #987 | Keep actual original namespace owner, not a fresh callback over an integer; receive dependent resources before parent custody. |

Native owns storage/session-specific initialization state and callback semantics.
The neutral package owns only explicit resource outcomes and bounded composition.
Core owns the application slot, lease, process/resource admission and worker join.
Neither imports another component's private implementation to infer its receipt.
The test bridge uses private attributes to observe those exact owners, not as a
proposal to expose them as cross-component production API.

## Corpus profile: do not reuse transient-ready literally

The reference `Construction` implements the journal profile. A corpus profile
uses the same Resource/Group mechanism but retains its data guard after successful
opening. A concrete sequence is:

1. Allocate and retain the corpus frame, connection ticket, guard Resource, guard
   Borrow, and sync-directory owner under explicit grants before source validation.
2. Acquire the guard exclusively when creating, otherwise open no-follow; pin the
   original borrow, and use its non-owning integer only for `_verify_file`.
3. Record/retain any returned database/corpus owner immediately. A factory or
   wrapper failure still leaves the original connection ticket and guard reachable.
4. Sync through the same retained directory owner; receive only this transient
   sync owner before successful return. The guard and its borrow remain live.
5. On close, stop/receive database operations and its actual connection; receive
   the original guard borrow after I/O exits; receive guard and outstanding sync
   owners; invoke the retained parent callback; finally mark Native receiving.

No inode/path check certifies a previous close. A callback failure does not clear
its reference. Constructor failure may require phase flags because `_db` or
`_corpus` was never returned; absence never certifies the earlier origin.
Full maintained corpus integration is not executed by the bridge and remains a
named acceptance obligation, not a hidden assumption.

## Resource and release ordering

Keep original ordinary and reserved-terminal budgets. A constructor resource is
charged from admission through positive receiving, including pre-return errors.
Borrowed readers and original SQLite cursors retain their own grants. Do not
release one because another owner received or because a Future is done.

A synchronous receiving return can establish file/scope release. An execution
owner must still observe actual original worker termination/join before releasing
its worker/process capacity. A test callback/fixture `verify_process=lambda: None`
is not a product permission policy; production uses the existing admission owner.

A constructor's frame receiving does not mean the workspace's bytes can be removed,
that an unsuccessful schema initialization succeeded, or that a published receipt
exists. Retain files and durable workflow identity under their current owners.
On cold reopen, follow the original current-only workspace policy. Never replay
an old process's descriptor numbers or discard a partially initialized allocation
merely to make a new constructor pass.

## Maintained interfaces and gates

Update all retained Native caller paths and their tests together: direct retained
JournalStore users, JournalValidationSession, Core upload and discovery/corpus
owners, built/installed Native interface and platform selection. No optional old
raw-creator branch, compatibility alias or alternate callback-only mode survives.
Audit any non-retained constructor separately if sharing an edited helper; do not
claim its directory/factory obligations solved incidentally by this packet.

Add the selected shared package to the existing locked workspace/dependency/build
and native-test gates. Its name/build packaging is an integration choice; the
reference wheel deliberately identifies itself as external reference material.
The branch-only workflow is NOT a substitute for maintained gates, public-interface
parity, source-owned composed growth evidence or exact final-source/tested-merge CI.
