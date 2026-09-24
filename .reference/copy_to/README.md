# Durable upload `copy_to`: server reference

**Non-authoritative prototype for #869 under #903.** See `../HANDOFF.md` first.
This directory is intentionally not imported by the running server. It supplies
executable reference code and a Riverhog-specific bridge, not a silently enabled
feature or an assertion that all #869 acceptance criteria are complete.

## What runs here

`handoff.py` owns two proposed tables in the **same catalog transaction** as the
upload, published collection, ordinary copy job, and lifecycle event. Acceptance
records normalized destinations and resolved operation choices before payload
work. Publication changes accepted intents into pending durable continuations in
the final catalog transaction. A restartable sweeper inserts the ordinary copy
job, its requested event, and a handoff receipt in one transaction.

`riverhog_bridge.py` adapts the inspected application's key/grant records,
collection access checks, published archive-copy checks, source selection, job
record, and lifecycle-event service. It is not loaded or end-to-end tested here.
The harness uses real relational transactions but deliberately reduced stand-in
catalog tables and authority/job adapters. It does not copy or impersonate the
complete Riverhog schema or copy executor.

## State and crash argument

| Boundary | Durable state before commit | Durable state after commit |
| --- | --- | --- |
| Upload creation / intent acceptance | Neither upload acceptance nor intents committed | Resolved choices and every destination are recorded; no job |
| Successful publication | Unpublished upload; accepted intents | Published collection and pending continuations together |
| One destination handoff | Pending intent; no newly generated job/event | Ordinary job + requested event + immutable receipt together |

A process death before a transaction commits rolls back that transaction. A death
after commit but before response leaves the complete durable result for a retry.
There is no independent queue acknowledgement, volatile work list, or caller-side
copy-intent database. A savepoint rolls back partial adapter writes before a
classified terminal failure is recorded. Unknown failures roll back the whole
handoff transaction; a fresh transaction schedules bounded exponential backoff.
Every destination is processed independently, so a failing destination does not
block a healthy sibling in the same sweep.

`handed_off` means lifecycle ownership was transferred, **not that bytes were
copied successfully**. Observation returns the current ordinary job state.
Terminal ordinary jobs stay terminal. A retained receipt prevents an upload retry
from recreating even a subsequently deleted job. Missing job observation is
explicitly returned as `None`, not disguised as success or resubmission.

The caller must use an explicit catalog transaction and commit before acknowledging
acceptance/publication. These methods deliberately do not commit internally.
The `Jobs.ensure` port must use that exact Session; an independently committing
implementation invalidates the crash argument. This is why the bridge extracts a
small creation primitive rather than calling `create_or_resume`.

## Prototype policy choices (not accepted repository decisions)

**Authorization.** Durable copy requests require a current attributable app key,
collection-create authority, and archive-management authority when accepted.
Pre-publication tag scope uses the existing creation helper conservatively: all
initial tags must be covered. Handoff reloads the **original** key and grants and
checks ordinary archive-management scope on the now-published collection.
Revocation, expiry, grant removal, app mismatch, or lost tag scope becomes a
visible terminal `authorization_denied` intent; an upload retry never changes the
initiator to the finalizing caller or the worker. Revocation after successful
handoff does not invent new ordinary-job cancellation behavior.

The bridge holds key/grant, collection, and tag-membership row locks while it
checks/materializes. Reconcile its lock ordering with all current mutation paths
and test revocation/expiry interleavings on PostgreSQL before integration. SQLite
serialization tests are not evidence that every PostgreSQL race is resolved.

**Attribution and observation.** New job/event app, key, and event context are
copied from accepted intent. An already-existing natural job identity is observed
without restarting it, rewriting its attribution, or emitting a duplicate event;
its receipt records `created=False`. A published intent can also be inspected by
another currently authorized archive manager, allowing operators to observe
failures after the originating key is revoked. Inspection never reattributes work.
A destination copy already complete **without** a job yields a terminal
`destination_already_present` classification rather than fabricating a job receipt.

**Creation identity.** Destinations are sorted and de-duplicated. Source,
resolved cache choice, destination set/bindings, original app/key, non-placement
upload identity, initial tags, and event context are bound in the reference
identity. Explicit changes conflict. Omitted options on resume reuse the durable
choices rather than current defaults; explicit `copy_to=[]` is not omission.
A new key cannot take over a pending operation merely because it has the same app.
The caller supplies the non-placement `base_identity`; integrating it with the
existing creation-identity document is required, not done by an extra HTTP call.

**Configuration.** Missing stores or same-name binding remaps fail visibly at
handoff; there is no silent substitution of a changed default. A stable binding
identity must be derived from the actual immutable server registry/configuration,
not a caller claim, secret, worker process identity, or ephemeral provider URL.
The initial archive binding is validated; ordinary Riverhog source selection is
still delegated to its existing helper. Binding identity/reconfiguration policy
is an explicit integration question, not a new provider-protocol requirement.

**Cache scope.** `use_cache` is carried and fenced to avoid weakening future
creation identity. This prototype does not implement upload caching or an ordinary
copy cache override. Existing job execution keeps its current server policy.
Do not claim that this completes the explicit-cache portion of #869. Preserve the
restore-required default and reconcile #492/#442 in the authoritative integration.

**Cancellation and failure.** Pre-publication discard cancels all unmaterialized
intents in its transaction. After publication, cancellation uses the ordinary
copy-job lifecycle; this prototype rejects using upload cancellation to reach
through that boundary. Terminal failed handoffs do not silently revive after
config or permissions return. Recovery/resubmission is an explicit operator or
integration policy decision. Arbitrary exception strings are not persisted.

## Server integration seams

1. **Schema and identity:** move/reconcile the two proposed records into Riverhog's
   single catalog owner and sanctioned pre-v1 state baseline/migration history.
   Preserve receipts beyond upload-session deletion; do not add a cascading FK to
   the transient upload row. Decide collection-deletion retention and durable
   attribution references. Integrate resolved choices into the existing
   `CollectionUploadCreationIdentityDocument` before the normal identity checks.
   Do not alter startup to call `metadata.create_all`.
2. **Creation:** after the reserved `CollectionUploadRecord.collection_id` exists,
   accept the complete destination set in the same creation transaction, before
   returning a session that permits payload. On replay, resolve omitted choices
   from durable state before recomputing identity. Avoid a second post-creation
   intent request, which would reopen the caller-owned failure window.
3. **Finalization and discard:** call `publish(session, collection_id)` in the final
   publication transaction, with the completed archive copy present and before
   deleting transient upload state. Call `cancel` in the discard transaction.
   Reconcile orphan/resume, custody expiry, cleanup, and all alternate publication
   paths; a background scan alone is not a substitute for the atomic publish hook.
4. **Continuation and ordinary jobs:** construct the bridge on the same catalog
   session factory and immutable archive registry; run `process_due` on startup
   and periodically. Extract/share the new-job primitive with ordinary creation
   in authoritative code instead of maintaining two implementations. Preserve the
   natural job uniqueness constraint and lifecycle-event transaction. Do not
   call the current independently committing `create_or_resume` from the outbox.
5. **Observation and qualification:** expose resolved choices, per-destination
   handoff/terminal evidence, and ordinary job references through authorized server
   responses. Validate real PostgreSQL locks, crashes, state baseline, HTTP,
   restart worker composition, provider/cache contracts, and repository gates.
   Client, CLI, and provider integration remain with the integration agent.

The existing service files are large and the environment could not clone/install
the full repository; only the source areas in `../AUDIT.md` were inspected. This
branch therefore keeps these seams explicit rather than claiming working request
paths it did not test.

## Running the reference tests

From a full checkout containing this branch, or from the archive's `reference-files`
directory:

```sh
python -m pip install -r .reference/copy_to/requirements.txt
python -m pytest -q --confcutdir=.reference/copy_to .reference/copy_to/test_handoff.py
python -m py_compile .reference/copy_to/*.py
```

The requirements pin the versions actually tested, not Riverhog's release
requirements. Use a disposable environment. `--confcutdir` prevents the unrelated
root test fixtures from being loaded by this standalone reference suite.

The process-kill tests require a POSIX host. The optional PostgreSQL test also
requires an installed SQLAlchemy driver and `COPY_TO_TEST_POSTGRES_URL` pointing
at an **empty, disposable** database. It refuses a nonempty database. The test
creates only reduced reference-harness tables, not the production baseline.
It was skipped in this handoff; PostgreSQL DDL compilation was performed instead.
See `../VALIDATION.md` and the archive's `evidence/` for results and qualifications.
